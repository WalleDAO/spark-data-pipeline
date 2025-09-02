with
    -- Generate daily sequence from launch date to current date
    seq as (
        select dt from unnest(sequence(date '2023-11-14', current_date, interval '1' day)) as t(dt)
    ),
    
    -- Calculate daily sUSDe balance changes (mints and burns)
    susde_balance as (
        select
            evt_block_date as dt,
            sum(
                if("from" = 0x0000000000000000000000000000000000000000, "value", -"value")
            ) / 1e18 as amount
        from ethena_labs_ethereum.stakedusdev2_evt_transfer
        where 0x0000000000000000000000000000000000000000 in ("from", "to")
        group by 1
    ),
    
    -- Calculate cumulative sUSDe balance over time
    susde_balance_cum as (
        select
            dt,
            sum(coalesce(amount, 0)) over (order by dt) as amount
        from seq
        left join susde_balance b using (dt)
    ),
    
    -- Extract daily USDe rewards sent to StakingRewardsDistributor
    usde_returned as (
        select
            evt_block_date as dt,
            sum("value") / 1e18 as interest_amount
        from ethena_labs_ethereum.usde_evt_transfer
        where "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439 -- StakingRewardsDistributor
        group by 1
    ),
    
    -- Fill missing dates with zero interest
    usde_returned_cum as (
        select
            dt,
            coalesce(interest_amount, 0) as interest_amount
        from seq
        left join usde_returned b using (dt)
    ),
    
    -- Calculate APY using 7-day rolling average for smoothing
    susde_apy as (
        select
            dt,
            daily_rate,
            -- Daily compounding APY: (1 + daily_rate)^365 - 1
            POWER(1 + daily_rate, 365) - 1 AS susde_apy,
            daily_rate * 365 AS susde_apr
        from (
            select
                dt,
                -- 7-day rolling average of daily yield rate
                avg(r.interest_amount / nullif(b.amount, 0)) over (
                    order by dt rows between 6 preceding and current row
                ) as daily_rate
            from susde_balance_cum b
            join usde_returned_cum r using (dt)
        )
    ),
    
    -- Combine with Spark protocol data (50% allocation)
    ethena_payout as (
        select
            dt,
            'ethereum' as blockchain,
            'ethena' as protocol_name,
            'USDe' as token_symbol,
            'NA' as reward_code,
            0 as reward_per,
            'APR-BR' as interest_code,
            a.susde_apy,
            a.susde_apr, 
            a.susde_apr - i.reward_per as interest_per,
            -- 50% Grove allocation, 50% Spark allocation
            (usde_pay_value + payout_value) / 2 as actual_amount,
            -- 50% Grove allocation, 50% Spark allocation
            (usde_value + susde_value + u_usde_value) / 2 as amount,
            i.reward_per as BR_cost_per
        from seq s
        cross join query_5353955 i -- Spark - Accessibility Rewards - Rates: interest (already APR)
        left join query_5163486 b using (dt) -- Spark - sUSDe yield for USDe
        left join susde_apy a using (dt)
        where dt >= date '2024-10-23'
          and i.reward_code = 'BR'
          and dt between i.start_dt and i.end_dt
    ),
    
    -- Calculate daily metrics
    final_output as (
        select
            *,
            -- Daily actual revenue (difference from previous day)
            coalesce(
                actual_amount - lag(actual_amount) over (order by dt), 
                0
            ) as daily_actual_revenue,
            
            amount * BR_cost_per / 365 as daily_BR_cost
            
        from ethena_payout
    )

-- Final output with all calculated metrics
select 
    dt,
    blockchain,
    protocol_name,
    token_symbol,
    reward_code,
    reward_per,        -- This is already APR (from Step 1)
    susde_apy,         -- Keep for reference
    susde_apr,         -- This is APR (converted)
    interest_code,
    interest_per,      -- This is APR difference (susde_apr - reward_per)
    actual_amount,
    amount,
    daily_actual_revenue,
    daily_BR_cost      -- Uses APR/365 (not APY/365)
from final_output 
order by dt desc;
