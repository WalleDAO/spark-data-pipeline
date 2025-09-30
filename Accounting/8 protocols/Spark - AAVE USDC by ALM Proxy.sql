with
    spark_tokens (blockchain, category, symbol, contract_address, decimals, start_date) as (
        values
        ('base', 'aToken', 'USDC', 0x4e65fe4dba92790696d040ac24aa414708f5c0ab, 6, date '2023-12-21'),  
        ('base', 'debtToken', 'USDC', 0x59dca05b6c26dbd64b5381374aaac5cd05644c28, 6, date '2023-12-21')
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
            from aave_v3_base.atoken_evt_mint t
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
            from aave_v3_base.atoken_evt_burn t
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
            from aave_v3_base.variabledebttoken_evt_mint t
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
            from aave_v3_base.variabledebttoken_evt_burn t
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
            from aave_v3_base.atoken_evt_mint t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and t.onBehalfOf = 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA
                and t.evt_block_date >= st.start_date
            union all
            select
                t.evt_block_date as dt,
                st.symbol,
                -(
                    (t."value" + t.balanceIncrease) / power(10, st.decimals)
                ) / (index / 1e27) as amount
            from aave_v3_base.atoken_evt_burn t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and t.target = 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA
                and t.evt_block_date >= st.start_date
            union all
            select
                t.evt_block_date as dt,
                st.symbol,
                if(
                    "to" = 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA,
                    1,
                    -1
                ) * (t."value" / power(10, st.decimals)) / (index / 1e27) as amount
            from aave_v3_base.atoken_evt_balancetransfer t
            join spark_tokens st
                on t.contract_address = st.contract_address
            where
                st.category = 'aToken'
                and 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA in ("from", "to")
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
    with daily_index_data as (
        select
            date_trunc('day', evt_block_time) as day,
            st.symbol,
            max_by(index / 1e27, evt_block_time) as supply_index
        from aave_v3_base.atoken_evt_mint t
        join spark_tokens st
            on t.contract_address = st.contract_address
        where
            st.category = 'aToken'
        group by 1, 2
        union all
        select
            date_trunc('day', evt_block_time) as day,
            st.symbol,
            max_by(index / 1e27, evt_block_time) as supply_index
        from aave_v3_base.atoken_evt_burn t
        join spark_tokens st
            on t.contract_address = st.contract_address
        where
            st.category = 'aToken'
        group by 1, 2
    ),
    daily_borrow_index_data as (
        select
            date_trunc('day', evt_block_time) as day,
            st.symbol,
            max_by(index / 1e27, evt_block_time) as borrow_index
        from aave_v3_base.variabledebttoken_evt_mint t
        join spark_tokens st
            on t.contract_address = st.contract_address
        where
            st.category = 'debtToken'
        group by 1, 2
        union all
        select
            date_trunc('day', evt_block_time) as day,
            st.symbol,
            max_by(index / 1e27, evt_block_time) as borrow_index
        from aave_v3_base.variabledebttoken_evt_burn t
        join spark_tokens st
            on t.contract_address = st.contract_address
        where
            st.category = 'debtToken'
        group by 1, 2
    ),
    combined_index_data as (
        select
            day,
            symbol,
            max(supply_index) as supply_index,
            cast(null as double) as borrow_index
        from daily_index_data
        group by 1, 2
        union all
        select
            day,
            symbol,
            cast(null as double) as supply_index,
            max(borrow_index) as borrow_index
        from daily_borrow_index_data
        group by 1, 2
    ),
    aggregated_data as (
        select
            day,
            symbol,
            max(supply_index) as supply_index,
            max(borrow_index) as borrow_index
        from combined_index_data
        group by 1, 2
    ),
    filled_data as (
        select
            s.dt as day,
            s.symbol,
            coalesce(
                ad.supply_index,
                last_value(ad.supply_index) ignore nulls over (
                    partition by s.symbol
                    order by s.dt rows between unbounded preceding
                        and current row
                )
            ) as supply_index,
            coalesce(
                ad.borrow_index,
                last_value(ad.borrow_index) ignore nulls over (
                    partition by s.symbol
                    order by s.dt rows between unbounded preceding
                        and current row
                )
            ) as borrow_index
        from seq s
        left join aggregated_data ad
            on s.dt = ad.day
            and s.symbol = ad.symbol
    )
    select
        day,
        symbol,
        supply_index,
        borrow_index,
        case
            when lag(supply_index) over (
                partition by symbol
                order by day
            ) is not null
                then (
                supply_index / lag(supply_index) over (
                    partition by symbol
                    order by day
                ) - 1
            ) * 365
            else 0
        end as supply_rate_apr,
        case
            when lag(borrow_index) over (
                partition by symbol
                order by day
            ) is not null
                then (
                borrow_index / lag(borrow_index) over (
                    partition by symbol
                    order by day
                ) - 1
            ) * 365
            else 0
        end as borrow_rate_apr
    from filled_data
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
        'base' as blockchain,
        'AAVE ALM Proxy' as protocol_name,
        b.symbol as token_symbol,
        rd.supply_index,
        rd.borrow_index,
        rd.supply_rate_apr,
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