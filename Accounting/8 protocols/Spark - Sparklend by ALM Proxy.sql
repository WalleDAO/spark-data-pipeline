with
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
        where rn = 1 
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
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
            
            union all
            
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_burn t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
            
            union all
            
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.variabledebttoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'debtToken'
            
            union all
            
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
    
    sp_transfers_alm as (
        select
            symbol,
            dt,
            sum(amount) as alm_supply_amount
        from (
            select
                t.evt_block_date as dt,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_mint t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and t.onBehalfOf = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
              and t.evt_block_date >= st.start_date
            
            union all
            
            select
                t.evt_block_date as dt,
                st.symbol,
                -((t."value" + t.balanceIncrease) / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_burn t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and t.target = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
              and t.evt_block_date >= st.start_date
            
            union all
            
            select
                t.evt_block_date as dt,
                st.symbol,
                if("to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E, 1, -1) * (t."value" / power(10, st.decimals)) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_balancetransfer t
            join filtered_tokens st on t.contract_address = st.contract_address
            where st.category = 'aToken'
              and 0x1601843c5E9bC251A3272907010AFa41Fa18347E in ("from", "to")
              and t.evt_block_date >= st.start_date
        )
        group by 1,2
    ),
    
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
    
    reserve_data as (
        with daily_reserve_data as (
            select 
                date_trunc('day', evt_block_time) as day, 
                reserve as asset_address,
                max_by(liquidityRate * 1e-27, evt_block_time) as supply_rate_apr,
                max_by(liquidityIndex * 1e-27, evt_block_time) as supply_index,
                max_by(variableBorrowIndex * 1e-27, evt_block_time) as borrow_index,
                max_by(variableBorrowRate * 1e-27, evt_block_time) as borrow_rate_apr
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
        join filtered_tokens ft on s.symbol = ft.symbol and ft.category = 'aToken'
        left join daily_reserve_data rrd 
            on s.dt = rrd.day 
            and ft.underlying_asset = rrd.asset_address
    ),
    
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
    
    sp_balances_index as (
        select
            b.dt,
            'ethereum' as blockchain,
            'Sparklend' as protocol_name,
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
        left join reserve_data rd on b.symbol = rd.symbol and b.dt = rd.day
    ),
    
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
        cross join query_5353955 r
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
            s.supply_rate_apr,
            s.borrow_rate_apr,
            s.alm_supply_amount,
            s.supply_amount,
            s.borrow_amount,
            s.utilization,
            s.idle_amount,
            s.alm_share,
            s.alm_idle,
            s.reward_code as borrow_cost_code,
            s.reward_per as borrow_cost_apr,
            rf.reserve_factor,
            s.sofr,
            case when token_symbol='PYUSD' then s.alm_supply_amount * (s.sofr + s.borrow_rate_apr * s.utilization)
            else s.alm_supply_amount * s.borrow_rate_apr * s.utilization end as interest_amount,
            case when token_symbol in ('USDS','DAI') then s.alm_supply_amount * s.utilization *s.reward_per
            else s.alm_supply_amount *s.reward_per end as BR_cost,
            s.borrow_amount * s.borrow_rate_apr * rf.reserve_factor * (1 - s.alm_share) as sparklend_revenue
        from sp_balances_rates s
        join sparklend_reserve_factor rf using (token_symbol)
    )
    
select * from sp_balances_interest
order by dt desc,token_symbol desc