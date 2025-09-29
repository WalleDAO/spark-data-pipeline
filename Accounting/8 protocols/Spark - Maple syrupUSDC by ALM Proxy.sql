with seq as (
    select
        d.dt
    from (
            select
                min(evt_block_date) as start_dt
            from maplefinance_v2_ethereum.pool_v2_evt_deposit
            where
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b
        ) d
    cross join unnest(
            sequence(d.start_dt, current_date, interval '1' day)
        ) as d(dt)
),
syrup_shares as (
    select 
        evt_block_date as dt,
        sum(
            case 
                when "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E 
                then cast(value as double) / 1e6
                else 0 
            end
        ) as inflow_shares,
        sum(
            case 
                when "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E 
                then cast(value as double) / 1e6
                else 0 
            end
        ) as outflow_shares
    from erc20_ethereum.evt_transfer
    where contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b
    and (
        "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E 
        or "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
    )
    group by 1
),
syrup_shares_daily as (
    select 
        dt,
        sum(inflow_shares) - sum(outflow_shares) as net_shares_change
    from syrup_shares
    group by 1
),
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
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b
                and assets_>1e6 and shares_>1e6
            union all
            select
                evt_block_time,
                evt_block_date,
                assets_,
                shares_
            from maplefinance_v2_ethereum.pool_v2_evt_withdraw
            where
                contract_address = 0x80ac24aA929eaF5013f6436cdA2a7ba190f5Cc0b
                and assets_>1e6 and shares_>1e6
        )
    group by 1
),
syrup_gaps as (
    select
        dt,
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
            supply_index / lag(supply_index, 1) over (
                order by dt asc
            ) - 1
        )  * 365 as supply_rate_apr
    from syrup_gaps ev
),
syrup_rates as (
    select
        dt,
        'ethereum' as blockchain,
        'Maple' as protocol_name,
        'USDC' as token_symbol,
        amount,
        i.reward_code as borrow_cost_code,
        i.reward_per as borrow_cost_apr,
        supply_rate_apr
    from syrup_apy
    cross join query_5353955 i
    where
        i.reward_code = 'BR'
        and dt between i.start_dt
        and i.end_dt
)
select *
from syrup_rates
order by 1 desc