-- header
with
    spark_tokens (blockchain, category, symbol, contract_address, decimals, start_date) as (
        values
        ('ethereum', 'aToken', 'DAI', 0x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B, 18, date '2023-03-07'),    -- spDAI
        ('ethereum', 'aToken', 'USDS', 0xC02aB1A5eaA8d1B114EF786D9bde108cD4364359, 18, date '2025-01-28'),   -- spUSDS
        ('ethereum', 'debtToken', 'DAI', 0xf705d2B7e92B3F38e6ae7afaDAA2fEE110fE5914, 18, date '2023-03-07'), -- Variable Debt DAI
        ('ethereum', 'debtToken', 'USDS', 0x8c147debea24Fb98ade8dDa4bf142992928b449e, 18, date '2025-01-28') -- Variable Debt USDS
    ),
    sparklend_reserve_factor (token_symbol, reserve_factor) as (
        values
            ('sUSDS', 0.10),
            ('USDC', 0.10),
            ('DAI', 0.10),
            ('ezETH', 0.15),
            ('GNO', 0),
            ('rETH', 0.15),
            ('USDS', 0.10),
            ('weETH', 0.15),
            ('rsETH', 0.15),
            ('tBTC', 0.20),
            ('cbBTC', 0.20),
            ('wstETH', 0.30),
            ('LBTC', 0.15),
            ('sDAI', 0.10),
            ('USDT', 0.10),
            ('WBTC', 0.20),
            ('WETH', 0.05)
    ),
    -- get total supply and borrow amounts
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
            from spark_protocol_ethereum.atoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from spark_protocol_ethereum.atoken_evt_burn t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
            union all
            -- ********************************** Mints/Burns debtTokens **********************************
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
            from spark_protocol_ethereum.variabledebttoken_evt_mint t
            join spark_tokens st using (contract_address)
            where st.category = 'debtToken'
            union all
            select
                t.evt_block_date as dt,
                st.category,
                st.symbol,
                -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
            from spark_protocol_ethereum.variabledebttoken_evt_burn t
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
            from spark_protocol_ethereum.atoken_evt_mint t
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
            from spark_protocol_ethereum.atoken_evt_burn t
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
            from spark_protocol_ethereum.atoken_evt_balancetransfer t
            join spark_tokens st using (contract_address)
            where st.category = 'aToken'
              and 0x1601843c5E9bC251A3272907010AFa41Fa18347E in ("from", "to") -- ALM Proxy
              and t.evt_block_date >= st.start_date
        )
        group by 1,2
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
    -- get underlying amounts through indices
    sp_balances_index as (
        select
            b.dt,
            'ethereum' as blockchain,
            'Sparklend' as protocol_name,
            b.symbol as token_symbol,
            i.supply_index,
            i.borrow_index,
            (i.borrow_index  / lag(i.borrow_index, 7) over (partition by b.symbol order by b.dt asc) - 1) / 7 * 365 as apy_7d,
            i.borrow_rate,
            i.borrow_usd,
            b.alm_supply_amount * i.supply_index as alm_supply_amount,
            b.supply_amount * i.supply_index as supply_amount,
            b.borrow_amount * i.borrow_index as borrow_amount,
            (b.supply_amount * i.supply_index) - (b.borrow_amount * i.borrow_index) as idle_amount
        from sp_balances b
        left join dune.steakhouse.result_spark_protocol_markets_data i
            on b.symbol = i.symbol
            and b.dt = i."period"
        where i.symbol in ('DAI', 'USDS')
    ),
    -- get the reward amounts
    sp_balances_rates as (
        select
            b.*,
            (alm_supply_amount / supply_amount) as alm_share,
            r.reward_code,
            r.reward_per,
            (idle_amount * (alm_supply_amount / supply_amount)) as alm_idle,
            'BR*' as interest_code,
            i.reward_per as interest_per
            --(b.borrow_amount * b.apy_7d * rf.reserve_factor) - (b.borrow_amount * r.reward_per) as interest_per
        from sp_balances_index b
        cross join query_5353955 r -- Spark - Accessibility Rewards - Rates : rebates
        cross join query_5353955 i -- Spark - Accessibility Rewards - Rates : interest
        left join sparklend_reserve_factor rf -- Spark - Sparklend Reserve Factor
            on b.token_symbol = rf.token_symbol
        where r.reward_code = 'BR'
          and i.reward_code = 'BR'
          and b.dt between r.start_dt and r.end_dt
          and b.dt between i.start_dt and i.end_dt
    ),
    sp_balances_interest as (
        select
            s.dt,
            s.blockchain,
            s.protocol_name,
            token_symbol,
            s.supply_index,
            s.borrow_index,
            s.apy_7d,
            s.borrow_rate,
            s.borrow_usd,
            s.alm_supply_amount,
            s.supply_amount,
            s.borrow_amount,
            s.idle_amount,
            s.alm_share,
            s.reward_code,
            s.reward_per,
            s.alm_idle,
            s.interest_code,
            s.interest_per,
            rf.reserve_factor,
            s.borrow_usd * s.borrow_rate * rf.reserve_factor as asset_rev_amount,
            s.borrow_usd * s.borrow_rate * rf.reserve_factor - s.interest_per * s.borrow_usd as interest_amount
        from sp_balances_rates s
        join sparklend_reserve_factor rf using (token_symbol)
    )
    
select * from sp_balances_interest order by dt desc
