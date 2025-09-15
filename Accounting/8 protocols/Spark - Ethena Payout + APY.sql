with seq as (
    select
        dt
    from unnest(
            sequence(
                date '2023-11-14',
                current_date,
                interval '1' day
            )
        ) as t(dt)
),
daily_staking_data as (
    select
        date(evt_block_time) as dt,
        coalesce(sum(assets/1e18), 0) as daily_deposits
    from ethena_labs_ethereum.StakedUSDeV2_evt_Deposit
    where date(evt_block_time) >= date('2023-11-14')
    group by 1
    
    union all
    
    select
        date(evt_block_time) as dt,
        -coalesce(sum(assets/1e18), 0) as daily_deposits
    from ethena_labs_ethereum.StakedUSDeV2_evt_Withdraw
    where date(evt_block_time) >= date('2023-11-14')
    group by 1
),
-- Aggregate daily net staking changes
daily_net_staking as (
    select
        dt,
        sum(daily_deposits) as net_daily_staking
    from daily_staking_data
    group by 1
),
-- Calculate daily protocol returns from USDe transfers
daily_data as (
    select
        date(evt_block_time) as dt,
        sum(
            case
                when "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439
                    then value / 1e18
                else 0
            end
        ) as daily_protocol_return
    from ethena_labs_ethereum.usde_evt_transfer
    where
        date(evt_block_time) >= date('2023-11-14')
        and "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439 -- StakingRewardsDistributor
    group by 1
),
-- Calculate cumulative staked USDe balance over time
balances_cum as (
    select
        s.dt,  
        sum(coalesce(d.net_daily_staking, 0)) over (order by s.dt) as total_staked_usde
    from seq s
    left join daily_net_staking d
        on s.dt = d.dt
),
-- Calculate sUSDe APY using 7-day rolling average with weekly compounding
susde_apy as (
    select
        dt,
        POWER(1 + weekly_avg_daily_rate * 7, 52) - 1 AS susde_apy,
        weekly_avg_daily_rate * 365 AS susde_apr
    from (
        select
            dt, 
            avg(coalesce(daily_protocol_return, 0)) over (
                order by dt rows between 6 preceding and current row
            ) / nullif(
                avg(total_staked_usde) over (
                    order by dt rows between 6 preceding and current row
                ),
                0
            ) as weekly_avg_daily_rate
        from balances_cum b
        left join daily_data d using (dt)
    )
),
-- Combine all data for Ethena protocol analysis
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
        (usde_pay_value + payout_value) / 2 as actual_amount,
        (usde_value + susde_value + u_usde_value) / 2 as amount,
        i.reward_per as BR_cost_per
    from seq s
    cross join query_5353955 i
    left join query_5163486 b
        using (dt)
    left join susde_apy a
        using (dt)
    where
        dt >= date '2024-10-23'
        and i.reward_code = 'BR'
        and dt between i.start_dt
        and i.end_dt
),
-- Calculate daily revenue and BR costs
final_output as (
    select
        *,
        coalesce(
            actual_amount - lag(actual_amount) over (
                order by dt
            ),
            0
        ) as daily_actual_revenue,
        amount * BR_cost_per / 365 as daily_BR_cost
    from ethena_payout
)
-- Final output with all metrics
select
    dt,
    blockchain,
    protocol_name,
    token_symbol,
    reward_code,
    reward_per,
    susde_apy,
    susde_apr,
    interest_code,
    interest_per,
    actual_amount,
    amount,
    daily_actual_revenue,
    daily_BR_cost
from final_output
order by dt desc;
