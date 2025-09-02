with
    -- Dynamically retrieve Spark tokens
    spark_tokens as (
        select 
            'ethereum' as blockchain,
            'aToken' as category,
            coalesce(t.symbol, 'UNKNOWN') as symbol,
            ri.aToken as contract_address,
            coalesce(t.decimals, 18) as decimals,
            ri.evt_block_date as start_date,
            ri.asset as underlying_asset
        from spark_protocol_ethereum.PoolConfigurator_evt_ReserveInitialized ri
        left join tokens.erc20 t 
            on ri.asset = t.contract_address 
            and t.blockchain = 'ethereum'
        
        union all
        
        select 
            'ethereum' as blockchain,
            'debtToken' as category,
            coalesce(t.symbol, 'UNKNOWN') as symbol,
            ri.variableDebtToken as contract_address,
            coalesce(t.decimals, 18) as decimals,
            ri.evt_block_date as start_date,
            ri.asset as underlying_asset
        from spark_protocol_ethereum.PoolConfigurator_evt_ReserveInitialized ri
        left join tokens.erc20 t 
            on ri.asset = t.contract_address 
            and t.blockchain = 'ethereum'
    ),
    
    filtered_tokens as (
        select * from spark_tokens
    ),
    
    -- Reserve factor configuration for different tokens
    sparklend_reserve_factor as (
        with latest_reserve_factors as (
            select 
                rf.asset,
                t.symbol as token_symbol,
                rf.newReserveFactor / 10000.0 as reserve_factor, 
                rf.evt_block_date,
                row_number() over (partition by rf.asset order by rf.evt_block_time desc) as rn
            from spark_protocol_ethereum.PoolConfigurator_evt_ReserveFactorChanged rf
            left join tokens.erc20 t 
                on rf.asset = t.contract_address 
                and t.blockchain = 'ethereum'
            where rf.asset in (
                select distinct underlying_asset 
                from filtered_tokens
            )
        )
        select 
            token_symbol,
            reserve_factor
        from latest_reserve_factors
        where rn = 1 -- Get the most recent reserve factor for each asset
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
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
            
            union all
            
            -- aToken burn events (supply decreases)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_burn t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
            
            union all
            
            -- Debt token mint events (borrow increases)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.variabledebttoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'debtToken'
            
            union all
            
            -- Debt token burn events (borrow decreases)
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.variabledebttoken_evt_burn t
            join filtered_tokens st on t.contract_address = st.contract_address
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
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and t.onBehalfOf = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALM Proxy address
              and t.evt_block_date >= st.start_date
            
            union all
            
            -- ALM Proxy aToken burns
            select
                t.evt_block_date as dt,
                st.symbol,
                -((t."value" + t.balanceIncrease) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_burn t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and t.target = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- ALM Proxy address
              and t.evt_block_date >= st.start_date
            
            union all
            
            -- ALM Proxy aToken transfers
            select
                t.evt_block_date as dt,
                st.symbol,
                if("to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E, 1, -1) * (t."value" / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_balancetransfer t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and 0x1601843c5E9bC251A3272907010AFa41Fa18347E in ("from", "to") -- ALM Proxy address
              and t.evt_block_date >= st.start_date
        )
        group by 1,2
    ),
    
    -- Generate daily time series for each token starting from their launch date
    seq as (
        select
            t.symbol,
            s.dt
        from (
            select symbol, min(start_date) as start_dt 
            from filtered_tokens 
            where category = 'aToken'
            group by 1
        ) t
        cross join unnest(sequence(t.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    
    -- Get reserve data including rates and indices from protocol events
    reserve_data as (
        with daily_reserve_data as (
            select 
                date_trunc('day', evt_block_time) as day, 
                reserve as asset_address,
                -- Calculate annualized supply rate (APY)
                power(1 + max_by(liquidityRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as supply_rate,
                max_by(liquidityIndex * 1e-27, evt_block_time) as supply_index,
                max_by(variableBorrowIndex * 1e-27, evt_block_time) as borrow_index,
                -- Calculate annualized borrow rate (APY)
                power(1 + max_by(variableBorrowRate * 1e-27, evt_block_time)/31536000, 31536000) - 1 as borrow_rate
            from spark_protocol_ethereum.Pool_evt_ReserveDataUpdated
            where reserve in (
                select distinct underlying_asset 
                from filtered_tokens
            )
            group by 1, 2
        )
        select 
            s.dt as day,
            s.symbol,
            -- Forward fill missing rate data (APY)
            coalesce(
                rrd.supply_rate, 
                last_value(rrd.supply_rate) ignore nulls over (
                    partition by s.symbol 
                    order by s.dt 
                    rows between unbounded preceding and current row
                )
            ) as supply_rate,
            -- Convert APY to APR for supply rate
            CASE 
                WHEN coalesce(
                    rrd.supply_rate, 
                    last_value(rrd.supply_rate) ignore nulls over (
                        partition by s.symbol 
                        order by s.dt 
                        rows between unbounded preceding and current row
                    )
                ) = 0 THEN 0.0
                ELSE 365.0 * (exp(ln(cast(1.0 + coalesce(
                    rrd.supply_rate, 
                    last_value(rrd.supply_rate) ignore nulls over (
                        partition by s.symbol 
                        order by s.dt 
                        rows between unbounded preceding and current row
                    )
                ) as double)) / 365.0) - 1.0)
            END as supply_rate_apr,
            -- Forward fill missing supply index data
            coalesce(
                rrd.supply_index, 
                last_value(rrd.supply_index) ignore nulls over (
                    partition by s.symbol 
                    order by s.dt 
                    rows between unbounded preceding and current row
                )
            ) as supply_index,
            -- Forward fill missing borrow index data
            coalesce(
                rrd.borrow_index, 
                last_value(rrd.borrow_index) ignore nulls over (
                    partition by s.symbol 
                    order by s.dt 
                    rows between unbounded preceding and current row
                )
            ) as borrow_index,
            -- Forward fill missing borrow rate data (APY)
            coalesce(
                rrd.borrow_rate, 
                last_value(rrd.borrow_rate) ignore nulls over (
                    partition by s.symbol 
                    order by s.dt 
                    rows between unbounded preceding and current row
                )
            ) as borrow_rate,
            -- Convert APY to APR for borrow rate
            CASE 
                WHEN coalesce(
                    rrd.borrow_rate, 
                    last_value(rrd.borrow_rate) ignore nulls over (
                        partition by s.symbol 
                        order by s.dt 
                        rows between unbounded preceding and current row
                    )
                ) = 0 THEN 0.0
                ELSE 365.0 * (exp(ln(cast(1.0 + coalesce(
                    rrd.borrow_rate, 
                    last_value(rrd.borrow_rate) ignore nulls over (
                        partition by s.symbol 
                        order by s.dt 
                        rows between unbounded preceding and current row
                    )
                ) as double)) / 365.0) - 1.0)
            END as borrow_rate_apr
        from seq s
        join filtered_tokens ft on s.symbol = ft.symbol and ft.category = 'aToken'
        left join daily_reserve_data rrd 
            on s.dt = rrd.day 
            and ft.underlying_asset = rrd.asset_address
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
            'Sparklend' as protocol_name,
            b.symbol as token_symbol,
            rd.supply_index,
            rd.borrow_index,
            rd.supply_rate,
            rd.borrow_rate,
            rd.supply_rate_apr,  -- Add APR fields
            rd.borrow_rate_apr,  -- Add APR fields
            b.alm_supply_amount * rd.supply_index as alm_supply_amount,
            b.supply_amount * rd.supply_index as supply_amount,
            b.borrow_amount * rd.borrow_index as borrow_amount,
            (b.supply_amount * rd.supply_index) - (b.borrow_amount * rd.borrow_index) as idle_amount
        from sp_balances b
        left join reserve_data rd on b.symbol = rd.symbol and b.dt = rd.day
    ),
    
    -- get the reward amounts
    sp_balances_rates as (
        select
            b.*,
            borrow_amount/supply_amount as utilization,
            (alm_supply_amount / supply_amount) as alm_share,
            r.reward_code,
            r.reward_per,
            (idle_amount * (alm_supply_amount / supply_amount)) as alm_idle,
            so.sofr
        from sp_balances_index b
        cross join query_5353955 r -- Spark - Accessibility Rewards - Rates : rebates
        left join query_5696213 so on b.dt=so.period
        where r.reward_code = 'BR'
          and b.dt between r.start_dt and r.end_dt
    ),
    sp_balances_interest as (
        select
            s.dt,
            s.blockchain,
            s.protocol_name,
            token_symbol,
            s.supply_index,
            s.borrow_index,
            s.supply_rate,
            s.borrow_rate,
            s.supply_rate_apr,  -- Include APR fields in output
            s.borrow_rate_apr,  -- Include APR fields in output
            s.alm_supply_amount,
            s.supply_amount,
            s.borrow_amount,
            s.utilization,
            s.idle_amount,
            s.alm_share,
            s.alm_idle,
            s.reward_code,
            s.reward_per,
            'APY-BR' as interest_code,
            s.borrow_rate_apr-s.reward_per as interest_per,  -- Use APR for interest calculation
            rf.reserve_factor,
            s.sofr,
            -- Use APR rates for interest calculations
            case when token_symbol='PYUSD' then s.alm_supply_amount * (s.sofr + s.borrow_rate_apr * s.utilization)
            else s.alm_supply_amount * s.borrow_rate_apr * s.utilization end as interest_amount,
            case when token_symbol in ('USDS','DAI') then s.alm_supply_amount * s.utilization *s.reward_per
            else s.alm_supply_amount *s.reward_per end as BR_cost,
            s.borrow_amount * s.borrow_rate_apr * rf.reserve_factor * (1 - s.alm_share) as sparklend_revenue  -- Use APR for revenue calculation
        from sp_balances_rates s
        join sparklend_reserve_factor rf using (token_symbol)
    )
    
select * from sp_balances_interest
order by dt desc,token_symbol desc
