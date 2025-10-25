with
    spark_tokens (blockchain, category, symbol, contract_address, decimals, start_date, underlying_asset) as (
        values
        ('avalanche_c', 'aToken', 'USDC', 0x625e7708f30ca75bfd92586e17077590c60eb4cd, 6, date '2022-03-11', 0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E),  
        ('avalanche_c', 'debtToken', 'USDC', 0xfccf3cabbe80101232d343252614b6a3ee81c989, 6, date '2022-03-11', 0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E)
    ),
sp_transfers_totals as (
    select
        dt,
        symbol,
        sum(if(category = 'aToken', amount, 0)) as supply_amount,
        sum(if(category = 'debtToken', amount, 0)) as borrow_amount
    from (
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                (
                    (
                        cast(t."value" as int256) - cast(t.balanceIncrease as int256)
                    ) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.atoken_evt_mint t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -(
                    (t."value" + t.balanceIncrease) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.atoken_evt_burn t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                (
                    (
                        cast(t."value" as int256) - cast(t.balanceIncrease as int256)
                    ) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.variabledebttoken_evt_mint t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'debtToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -(
                    (t."value" + t.balanceIncrease) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.variabledebttoken_evt_burn t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'debtToken'
        )
    group by 1, 2
),
sp_transfers_alm as (
    select
        symbol,
        dt,
        sum(amount) as alm_supply_amount
    from (
            select
                t.evt_block_date as dt,
                st.symbol,
                (
                    (
                        cast(t."value" as int256) - cast(t.balanceIncrease as int256)
                    ) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.atoken_evt_mint t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and t.onBehalfOf = 0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC
                and t.evt_block_date >= st.start_date
            union all
            select
                t.evt_block_date as dt,
                st.symbol,
                -(
                    (t."value" + t.balanceIncrease) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_avalanche_c.atoken_evt_burn t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and t.target = 0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC
                and t.evt_block_date >= st.start_date
            union all
            select
                t.evt_block_date as dt,
                st.symbol,
                if(
                    "to" = 0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC,
                    1,
                    -1
                ) * (t."value" / power(10, st.decimals)) / (index / 1e27) as amount
            from aave_v3_avalanche_c.atoken_evt_balancetransfer t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and 0xecE6B0E8a54c2f44e066fBb9234e7157B15b7FeC in ("from", "to")
                and t.evt_block_date >= st.start_date
        )
    group by 1, 2
),
seq as (
    select
        t.symbol,
        s.dt
    from (
            select
                symbol,
                min(start_date) as start_dt
            from spark_tokens
            where
                category = 'aToken'
            group by 1
        ) t
    cross join unnest(
            sequence(t.start_dt, current_date, interval '1' day)
        ) as s(dt)
),
reserve_data as (
    with daily_reserve_data as (
        select 
            date_trunc('day', evt_block_time) as day, 
            reserve as asset_address,
            max_by(liquidityRate * 1e-27, evt_block_time) as supply_rate_apr,
            max_by(liquidityIndex * 1e-27, evt_block_time) as supply_index,
            max_by(variableBorrowIndex * 1e-27, evt_block_time) as borrow_index,
            max_by(variableBorrowRate * 1e-27, evt_block_time) as borrow_rate_apr
        from aave_v3_avalanche_c.pool_evt_reservedataupdated
        where reserve in (
            select distinct underlying_asset 
            from spark_tokens
        )
        group by 1, 2
    )
    select 
        s.dt as day,
        s.symbol,
        coalesce(
            rrd.supply_rate_apr, 
            last_value(rrd.supply_rate_apr) ignore nulls over (
                partition by s.symbol 
                order by s.dt 
                rows between unbounded preceding and current row
            )
        ) as supply_rate_apr,
        coalesce(
            rrd.supply_index, 
            last_value(rrd.supply_index) ignore nulls over (
                partition by s.symbol 
                order by s.dt 
                rows between unbounded preceding and current row
            )
        ) as supply_index,
        coalesce(
            rrd.borrow_index, 
            last_value(rrd.borrow_index) ignore nulls over (
                partition by s.symbol 
                order by s.dt 
                rows between unbounded preceding and current row
            )
        ) as borrow_index,
        coalesce(
            rrd.borrow_rate_apr, 
            last_value(rrd.borrow_rate_apr) ignore nulls over (
                partition by s.symbol 
                order by s.dt 
                rows between unbounded preceding and current row
            )
        ) as borrow_rate_apr
    from seq s
    join spark_tokens ft on s.symbol = ft.symbol and ft.category = 'aToken'
    left join daily_reserve_data rrd 
        on s.dt = rrd.day 
        and ft.underlying_asset = rrd.asset_address
),
sp_balances as (
    select
        dt,
        symbol,
        sum(coalesce(ta.alm_supply_amount, 0)) over (
            partition by symbol
            order by dt
        ) as alm_supply_amount,
        sum(coalesce(tt.supply_amount, 0)) over (
            partition by symbol
            order by dt
        ) as supply_amount,
        sum(coalesce(tt.borrow_amount, 0)) over (
            partition by symbol
            order by dt
        ) as borrow_amount
    from seq s
    left join sp_transfers_alm ta
        using (dt, symbol)
    left join sp_transfers_totals tt
        using (dt, symbol)
),
sp_balances_index as (
    select
        b.dt,
        'avalanche_c' as blockchain,
        'AAVE' as protocol_name,
        b.symbol as token_symbol,
        rd.supply_index,
        rd.borrow_index,
        rd.supply_rate_apr,
        if(
        365 * (exp(ln(1 + 0.08) / 365) - 1) - rd.supply_rate_apr > 0,
        365 * (exp(ln(1 + 0.08) / 365) - 1) - rd.supply_rate_apr,
        0) as avalanche_reward_per,
        rd.borrow_rate_apr,
        b.alm_supply_amount * rd.supply_index as alm_supply_amount,
        b.supply_amount * rd.supply_index as supply_amount,
        b.borrow_amount * rd.borrow_index as borrow_amount,
        (b.supply_amount * rd.supply_index) - (b.borrow_amount * rd.borrow_index) as idle_amount
    from sp_balances b
    left join reserve_data rd
        on b.symbol = rd.symbol
        and b.dt = rd.day
)
select
    b.*,
    borrow_amount / supply_amount as utilization,
    (alm_supply_amount / supply_amount) as alm_share,
    r.reward_code as borrow_cost_code,
    r.reward_per as borrow_cost_apr,
    (
        idle_amount * (alm_supply_amount / supply_amount)
    ) as alm_idle
from sp_balances_index b
cross join query_5353955 r
where
    r.reward_code = 'BR'
    and b.dt between r.start_dt
    and r.end_dt
order by dt desc