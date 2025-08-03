-- header
-- @dev: allocation is shared with Grove. So only 50% of the allocation is Spark’s (and thus, 50% of the yield)

with
    seq as (
        select dt from unnest(sequence(date '2023-11-14', current_date, interval '1' day)) as t(dt)
    ),
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
    susde_balance_cum as (
        select
            dt,
            sum(coalesce(amount, 0)) over (order by dt) as amount
        from seq
        left join susde_balance b using (dt)
    ),
    usde_returned as (
        select
            evt_block_date as dt,
            sum("value") / 1e18 as interest_amount
        from ethena_labs_ethereum.usde_evt_transfer
        --where "from" = 0xf2fa332bd83149c66b09b45670bce64746c6b439 -- StakingRewardsDistributor
        --  and "to" = 0x9d39a5de30e57443bff2a8307a4256c8797a3497   -- sUSDe
        where "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439 -- StakingRewardsDistributor
        group by 1
    ),
    usde_returned_cum as (
        select
            dt,
            coalesce(interest_amount, 0) as interest_amount
        from seq
        left join usde_returned b using (dt)
        --group by 1
    ),
    susde_apy as (
        select
            dt,
            POWER(1 + 7 * rate_7d, 52) - 1 AS susde_apy -- weekly compounding
        from (
            select
                dt,
                avg(r.interest_amount / nullif(b.amount, 0)) over (order by dt rows between 6 preceding and current row) rate_7d
            from susde_balance_cum b
            join usde_returned_cum r using (dt)
        )
    ),
    ethena_payout as (
        select
            dt,
            'ethereum' as blockchain,
            'ethena' as protocol_name,
            'USDe' as token_symbol,
            'NA' as reward_code,
            0 as reward_per,
            'APY-BR' as interest_code,
            --coalesce(a.susde_apy, 0) as interest_per,
            greatest(a.susde_apy - i.reward_per, 0) as interest_per,
            usde_pay_value + payout_value as actual_amount,
            usde_pay_value + payout_value as actual_amount_usd,
            -- 50% Grove , 50% Spark
            (usde_value + susde_value) / 2 as amount,
            (usde_value + susde_value) / 2 as amount_usd -- @todo: should be converted in USD?
        from seq s
        cross join query_5353955 i -- Spark - Accessibility Rewards - Rates: interest
        left join query_5163486 b using (dt) -- Spark - sUSDe yield for USDe
        left join susde_apy a using (dt)
        where dt >= date '2024-10-23'
          and i.reward_code = 'BR'
          and dt between i.start_dt and i.end_dt
    )
    
select * from ethena_payout order by 1 desc

