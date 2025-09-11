with
    spark_tokens (blockchain, category, symbol, contract_address, decimals, start_date) as (
        values
        ('ethereum', 'aToken', 'USDS', 0x09AA30b182488f769a9824F15E6Ce58591Da4781, 18, date '2024-10-02'),   -- aEthLidoUSDS
        ('ethereum', 'debtToken', 'USDS', 0x2D9fe18b6c35FE439cC15D932cc5C943bf2d901E, 18, date '2024-10-02') -- variable Debt EthLidoUSDS
    ),
    
    -- Get underlying asset addresses for market data lookup
    underlying_assets as (
        select distinct
            st.symbol,
            case 
                when st.symbol = 'USDS' then 0xdC035D45d973E3EC169d2276DDab16f1e407384F -- USDS underlying asset
                when st.symbol = 'DAI' then 0x6B175474E89094C44Da98b954EedeAC495271d0F   -- DAI underlying asset
            end as underlying_asset
        from spark_tokens st
        where st.category = 'aToken'
    ),
    
    -- Get reserve data including rates and indices from protocol events
    reserve_data as (
        with daily_reserve_data as (
            select 
                date_trunc('day', evt_block_time) as day, 
                reserve as asset_address,
                -- Get the latest supply index for the day
                max_by(liquidityIndex * 1e-27, evt_block_time) as supply_index,
                -- Get the latest borrow index for the day
                max_by(variableBorrowIndex * 1e-27, evt_block_time) as borrow_index,
                -- Calculate annualized supply rate
                power(1 + max_by(liquidityRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as supply_rate,
                -- Calculate annualized borrow rate
                power(1 + max_by(variableBorrowRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as borrow_rate
            from spark_protocol_ethereum.Pool_evt_ReserveDataUpdated
            where reserve in (
                select underlying_asset 
                from underlying_assets
                where underlying_asset is not null
            )
            group by 1, 2
        ),
        -- Generate time series and forward fill missing data
        time_series as (
            select
                ua.symbol,
                s.dt
            from underlying_assets ua
            cross join unnest(sequence(date '2024-10-02', current_date, interval '1' day)) as s(dt)
            where ua.underlying_asset is not null
        )
        select 
            ts.dt as period,
            ts.symbol,
            -- Forward fill missing supply index data
            coalesce(
                rrd.supply_index, 
                last_value(rrd.supply_index) ignore nulls over (
                    partition by ts.symbol 
                    order by ts.dt 
                    rows between unbounded preceding and current row
                )
            ) as supply_index,
            -- Forward fill missing borrow index data
            coalesce(
                rrd.borrow_index, 
                last_value(rrd.borrow_index) ignore nulls over (
                    partition by ts.symbol 
                    order by ts.dt 
                    rows between unbounded preceding and current row
                )
            ) as borrow_index,
            -- Forward fill missing supply rate data
            coalesce(
                rrd.supply_rate, 
                last_value(rrd.supply_rate) ignore nulls over (
                    partition by ts.symbol 
                    order by ts.dt 
                    rows between unbounded preceding and current row
                )
            ) as supply_rate,
            -- Forward fill missing borrow rate data
            coalesce(
                rrd.borrow_rate, 
                last_value(rrd.borrow_rate) ignore nulls over (
                    partition by ts.symbol 
                    order by ts.dt 
                    rows between unbounded preceding and current row
                )
            ) as borrow_rate
        from time_series ts
        left join underlying_assets ua on ts.symbol = ua.symbol
        left join daily_reserve_data rrd 
            on ts.dt = rrd.day 
            and ua.underlying_asset = rrd.asset_address
    ),
    
    -- Calculate total supply and borrow amounts from token events
    sp_transfers_totals as (
        select
            dt,
            symbol,
            sum(if(category = 'aToken', amount, 0)) as supply_amount,
            sum(if(category = 'debtToken', amount, 0)) as borrow_amount
        from (
            -- aToken mint events (supply increases)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            
            union all
            
            -- aToken burn events (supply decreases)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_burn t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            
            union all
            
            -- Debt token mint events (borrow increases) - using atoken events for debt tokens
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'debtToken'
            
            union all
            
            -- Debt token burn events (borrow decreases) - using atoken events for debt tokens
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_burn t
            join spark_tokens st using (contract_address)
            where st.category = 'debtToken'
        )
        group by 1,2
    ),
    
    -- Calculate ALM Proxy specific supply amounts
    sp_transfers_alm as (
        select
            symbol,
            dt,
            sum(amount) as alm_supply_amount
        from (
            -- ALM Proxy aToken mints
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
              and t.onBehalfOf = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALM Proxy
              and t.evt_block_date >= st.start_date
            
            union all
            
            -- ALM Proxy aToken burns
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_burn t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
              and t.target = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALM Proxy
              and t.evt_block_date >= st.start_date
            
            union all
            
            -- ALM Proxy aToken transfers (fallback case)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                if("to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E, 1, -1) * (t."value" / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_balancetransfer t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
              and 0x1601843c5E9bC251A3272907010AFa41Fa18347E in ("from", "to") -- ALM Proxy
              and t.evt_block_date >= st.start_date
        )
        group by 1,2
    ),
    
    -- Generate daily time series starting from first transaction
    seq as (
        select
            t.symbol,
            s.dt
        from (select symbol, min(dt) as start_dt from sp_transfers_totals group by 1) t
        cross join unnest(sequence(t.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    
    -- Calculate cumulative balances using running sums
    sp_balances as (
        select
            dt,
            symbol,
            sum(coalesce(ta.alm_supply_amount, 0)) over (partition by symbol order by dt) as alm_supply_amount,
            sum(coalesce(tt.supply_amount, 0)) over (partition by symbol order by dt) as supply_amount,
            sum(coalesce(tt.borrow_amount, 0)) over (partition by symbol order by dt) as borrow_amount
        from seq s
        left join sp_transfers_alm ta using (dt, symbol)
        left join sp_transfers_totals tt using (dt, symbol) 
    ),
    
    -- Convert token balances to underlying amounts using indices
    sp_balances_index as (
        select
            b.dt,
            'ethereum' as blockchain,
            'AAVE ALM Proxy' as protocol_name,
            b.symbol as token_symbol,
            rd.supply_index,
            rd.borrow_index,
            b.alm_supply_amount * rd.supply_index as alm_supply_amount,
            b.supply_amount * rd.supply_index as supply_amount,
            b.borrow_amount * rd.borrow_index as borrow_amount,
            (b.supply_amount * rd.supply_index) - (b.borrow_amount * rd.borrow_index) as idle_amount
        from sp_balances b
        left join reserve_data rd
            on b.symbol = rd.symbol
            and b.dt = rd.period
        where b.symbol in ('DAI', 'USDS')
    ),
    
    -- Calculate reward rates and ALM metrics
    sp_balances_rates as (
        select
            b.*,
            case 
                when b.supply_amount > 0 then (b.alm_supply_amount / b.supply_amount) 
                else 0 
            end as alm_share,
            r.reward_code,
            r.reward_per,
            i.reward_code as interest_code,
            -i.reward_per as interest_per,
            case 
                when b.supply_amount > 0 then (b.idle_amount * (b.alm_supply_amount / b.supply_amount))
                else 0 
            end as alm_idle
        from sp_balances_index b
        cross join query_5353955 r -- Spark - Accessibility Rewards - Rates
        cross join query_5353955 i -- Spark - Accessibility Rewards - Rates => interest percentage
        where r.reward_code = 'AR'
          and i.reward_code = 'BR'
          and b.dt between r.start_dt and r.end_dt
          and b.dt between i.start_dt and i.end_dt
    )

select * from sp_balances_rates 
order by dt desc
