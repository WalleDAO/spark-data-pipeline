with addr (syrup_addr, alm_addr) as (
    values
        (
            0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b,
            0x1601843c5E9bC251A3272907010AFa41Fa18347E
        )
),
seq as (
    select
        d.dt
    from (
            select
                min(evt_block_date) as start_dt
            from maplefinance_v2_ethereum.pool_v2_evt_deposit
            where
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b -- syrupUSDC
        ) d
    cross join unnest(
            sequence(d.start_dt, current_date, interval '1' day)
        ) as d(dt)
),
-- Track ALM's share changes in Maple SyrupUSDC pool
syrup_shares as (
    select 
        evt_block_date as dt,
        sum(
            case 
                when owner_ = (select alm_addr from addr) then cast(shares_ as double) / 1e6
                else 0 
            end
        ) as deposit_shares,
        0 as withdraw_shares
    from maplefinance_v2_ethereum.pool_v2_evt_deposit
    where contract_address = (select syrup_addr from addr)
    group by 1
    
    union all
    
    select 
        evt_block_date as dt,
        0 as deposit_shares,
        sum(
            case 
                when owner_ = (select alm_addr from addr) then cast(shares_ as double) / 1e6
                else 0 
            end
        ) as withdraw_shares
    from maplefinance_v2_ethereum.pool_v2_evt_withdraw
    where contract_address = (select syrup_addr from addr)
    group by 1
),

syrup_shares_daily as (
    select 
        dt,
        sum(deposit_shares) - sum(withdraw_shares) as net_shares_change
    from syrup_shares
    group by 1
),
-- Cumulative shares held by ALM
syrup_shares_cum as (
    select 
        dt,
        sum(coalesce(net_shares_change, 0)) over (order by dt) as total_shares
    from seq
    left join syrup_shares_daily using(dt)
),
syrup_indices as (
    select
        evt_block_date as dt,
        max_by(assets_ / 1e6, evt_block_time) as assets,
        max_by(shares_ / 1e6, evt_block_time) as shares,
        max_by(
            (assets_ / 1e6) / (shares_ / 1e6), 
            evt_block_time
        ) as supply_index
    from (
            select
                evt_block_time,
                evt_block_date,
                assets_,
                shares_
            from maplefinance_v2_ethereum.pool_v2_evt_deposit
            where
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b -- syrupUSDC
                and assets_>1 and shares_>1
            union all
            select
                evt_block_time,
                evt_block_date,
                assets_,
                shares_
            from maplefinance_v2_ethereum.pool_v2_evt_withdraw
            where
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b -- syrupUSDC
                and assets_>1 and shares_>1
        )
    group by 1
),
syrup_gaps as (
    select
        dt,
        -- Amount: ALM shares multiplied by current supply index (current position value)
        coalesce(
            s.total_shares * last_value(i.supply_index) ignore nulls over (order by dt),
            0
        ) as amount,
        last_value(i.assets) ignore nulls over (
            order by dt
        ) as assets,
        last_value(i.shares) ignore nulls over (
            order by dt
        ) as shares,
        last_value(i.supply_index) ignore nulls over (
            order by dt
        ) as supply_index
    from seq
    left join syrup_indices i
        using (dt)
    left join syrup_shares_cum s
        using (dt)
),
syrup_apy as (
    select
        dt,
        amount,
        (
            supply_index / lag(supply_index, 7) over (
                order by dt asc
            ) - 1
        ) / 7 * 365 as apr_7d,
        (
            supply_index / lag(supply_index, 30) over (
                order by dt asc
            ) - 1
        ) / 30 * 365 as apr_30d,
        (
            supply_index / lag(supply_index, 90) over (
                order by dt asc
            ) - 1
        ) / 90 * 365 as apr_90d
    from syrup_gaps ev
),
syrup_rates as (
    select
        dt,
        'ethereum' as blockchain,
        'Maple' as protocol_name,
        'USDC' as token_symbol,
        amount,
        'NA' as reward_code,
        0 as reward_per,
        'APR-BR' as interest_code,
        apr_90d - i.reward_per as interest_per,
        apr_7d,
        apr_30d,
        apr_90d
    from syrup_apy
    cross join query_5353955 i
    where
        i.reward_code = 'BR'
        and dt between i.start_dt
        and i.end_dt
)
select
    *
from syrup_rates
order by 1 desc
