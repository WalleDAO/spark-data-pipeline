with
    spark_tokens (blockchain, category, symbol, contract_address, decimals, start_date) as (
        values
        ('ethereum', 'aToken', 'USDS', 0x09AA30b182488f769a9824F15E6Ce58591Da4781, 18, date '2024-10-02'),   -- aEthLidoUSDS
        ('ethereum', 'debtToken', 'USDS', 0x2D9fe18b6c35FE439cC15D932cc5C943bf2d901E, 18, date '2024-10-02') -- variable Debt EthLidoUSDS
    ),
    sp_transfers_totals as (
        select
            dt,
            symbol,
            sum(if(category = 'aToken', amount, 0)) as supply_amount,
            sum(if(category = 'debtToken', amount, 0)) as borrow_amount
        from (
            -- ********************************** Mints/Burns aToken **********************************
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_burn t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            union all
            -- ********************************** Mints/Burns debtTokens **********************************
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from aave_v3_lido_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'debtToken'
            union all
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
    -- get total supply amount for ALM Proxy
    sp_transfers_alm as (
        select
            symbol,
            dt,
            sum(amount) as alm_supply_amount
        from (
            -- ********************************** Mints/Burns aToken **********************************
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
            -- ********************************** Transfers aToken **********************************
            -- @dev: the ALM Proxy contract does not have direct transfers, but just in case
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
    -- Extract indices directly from mint/burn events
    sp_indices_raw as (
        -- Supply indices from aToken events
        select
            t.evt_block_date as dt,
            st.symbol,
            'supply' as index_type,
            t.index / 1e27 as index_value
        from aave_v3_lido_ethereum.atoken_evt_mint t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
        union all
        select
            t.evt_block_date as dt,
            st.symbol,
            'supply' as index_type,
            t.index / 1e27 as index_value
        from aave_v3_lido_ethereum.atoken_evt_burn t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
        union all
        -- Borrow indices from debtToken events (using aToken events but with debtToken contract)
        select
            t.evt_block_date as dt,
            st.symbol,
            'borrow' as index_type,
            t.index / 1e27 as index_value
        from aave_v3_lido_ethereum.atoken_evt_mint t
        join spark_tokens st using (contract_address)
        where st.category = 'debtToken'
        union all
        select
            t.evt_block_date as dt,
            st.symbol,
            'borrow' as index_type,
            t.index / 1e27 as index_value
        from aave_v3_lido_ethereum.atoken_evt_burn t
        join spark_tokens st using (contract_address)
        where st.category = 'debtToken'
    ),
    -- Get daily max indices
    sp_indices_daily as (
        select
            dt,
            symbol,
            max(case when index_type = 'supply' then index_value end) as supply_index,
            max(case when index_type = 'borrow' then index_value end) as borrow_index
        from sp_indices_raw
        group by 1, 2
    ),
    seq as (
        select
            t.symbol,
            s.dt
        from (select symbol, min(dt) as start_dt from sp_transfers_totals group by 1) t
        cross join unnest(sequence(t.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    -- get cumulative balances
    sp_balances as (
        select
            dt,
            symbol,
            sum(coalesce(ta.alm_supply_amount, 0)) over (partition by symbol order by dt) as alm_supply_amount,
            sum(coalesce(tt.supply_amount, 0)) over (partition by symbol order by dt) as supply_amount,
            sum(coalesce(tt.borrow_amount, 0)) over (partition by symbol order by dt) as borrow_amount
        from seq s
        left join sp_transfers_alm ta
            using (dt, symbol)
        left join sp_transfers_totals tt
            using (dt, symbol) 
    ),
    -- get index data with forward fill for missing values
    sp_indices_filled as (
        select
            b.dt,
            b.symbol,
            -- Forward fill missing supply_index values
            coalesce(
                last_value(i.supply_index) ignore nulls over (
                    partition by b.symbol 
                    order by b.dt 
                    rows between unbounded preceding and current row
                ), 1.0
            ) as supply_index,
            -- Forward fill missing borrow_index values
            coalesce(
                last_value(i.borrow_index) ignore nulls over (
                    partition by b.symbol 
                    order by b.dt 
                    rows between unbounded preceding and current row
                ), 1.0
            ) as borrow_index
        from sp_balances b
        left join sp_indices_daily i
        on b.dt=i.dt and b.symbol=i.symbol
    ),
    -- get underlying amounts through indices
    sp_balances_index as (
        select
            b.dt,
            'ethereum' as blockchain,
            'AAVE ALM Proxy' as protocol_name,
            b.symbol as token_symbol,
            i.supply_index,
            i.borrow_index,
            b.alm_supply_amount * i.supply_index as alm_supply_amount,
            b.supply_amount * i.supply_index as supply_amount,
            b.borrow_amount * i.borrow_index as borrow_amount,
            (b.supply_amount * i.supply_index) - (b.borrow_amount * i.borrow_index) as idle_amount
        from sp_balances b
        join sp_indices_filled i
         on b.dt=i.dt and b.symbol=i.symbol
    ),
    -- get the reward amounts
    sp_balances_rates as (
        select
            b.*,
            case 
                when b.supply_amount > 0 then (b.alm_supply_amount / b.supply_amount)
                else 0 
            end as alm_share,
            r.reward_code as gross_yield_code,
            r.reward_per as gross_yield_apr,
            i.reward_code as borrow_cost_code,
            i.reward_per as borrow_cost_apr,
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
    
select * from sp_balances_rates order by dt desc