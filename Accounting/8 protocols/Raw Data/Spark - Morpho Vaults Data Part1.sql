/*
@title: Morpho V2 - Specific Vaults Daily Data (Original Logic)
@description: Daily data for specific 3 vaults using original algorithm
--dune.steakhouse.result_morpho_vaults_data
--https://dune.com/queries/4746996
--dune.steakhouse.result_morpho_vaults
--https://dune.com/queries/4835775
*/

with 
    -- Define specific vaults to analyze
    sp_vault_addr (blockchain, category, symbol, vault_addr, start_date) as (
        values
            ('ethereum', 'vault', 'spDAI', 0x73e65dbd630f90604062f6e02fab9138e713edd9, date '2024-03-15'),
            ('base', 'vault', 'spUSDC', 0x7BfA7C4f149E7415b73bdeDfe609237e29CBF34A, date '2024-12-30'),
            ('ethereum', 'vault', 'spUSDS', 0xe41a0583334f0dc4e023acd0bfef3667f6fe0597, date '2025-07-16'),
            ('ethereum', 'vault', 'spUSDC', 0x56A76b428244a50513ec81e225a293d128fd581D, date '2025-09-08')
    ),
    
    -- Get latest curator changes
    vault_curator_change as (
        SELECT 
            chain as blockchain, 
            contract_address, 
            max_by(newCurator, evt_block_time) as curator
        FROM metamorpho_vaults_multichain.metamorphov1_1_evt_setcurator
        WHERE contract_address in (select vault_addr from sp_vault_addr)
        GROUP BY 1, 2
    ),
    
    -- Get vault creation events
    vault_creation as (
        select
            chain as blockchain,
            evt_block_time as creation_date,
            metamorpho as vault_address,
            asset as token_address,
            COALESCE(vcc.curator, initialOwner) as owner_address,
            name as vault_name,
            symbol as vault_symbol
        from (
            select *, 1.0 as version from metamorpho_factory_multichain.metamorphofactory_evt_createmetamorpho
            union all
            select *, 1.1 as version from metamorpho_factory_multichain.metamorphov1_1factory_evt_createmetamorpho
        ) as c
        LEFT JOIN vault_curator_change as vcc 
            on c.chain = vcc.blockchain 
            and c.metamorpho = vcc.contract_address
        WHERE c.metamorpho in (select vault_addr from sp_vault_addr)
    ),
    
    -- Get latest vault symbol changes  
    vault_symbol_change as (
        select 
            chain, 
            contract_address, 
            max_by(symbol, evt_block_number) as vault_symbol
        from metamorpho_vaults_multichain.metamorphov1_1_evt_setsymbol
        where symbol != ''
        and contract_address in (select vault_addr from sp_vault_addr)
        group by 1, 2
    ),

    -- Get vault metadata (filtered by our specific vaults)
    vault_data as (
        select
            v.creation_date as creation_ts,
            DATE(v.creation_date) as creation_dt,
            v.blockchain,
            v.token_address,
            v.vault_address,
            t.symbol as token_symbol,
            t.decimals as token_decimals,
            18 as market_decimals,
            coalesce(s.vault_symbol, v.vault_symbol) as vault_symbol
            
        from vault_creation v
        left join dune.steakhouse.result_token_info t
            on v.token_address = t.token_address
            and v.blockchain = t.blockchain
        left join vault_symbol_change s
            on v.blockchain = s.chain
            and v.vault_address = s.contract_address
    ),
    
    -- Generate time series for each vault
    vault_series as (
        select
            t.dt,
            v.blockchain,
            v.vault_address,
            v.token_address
        from vault_data v
        cross join unnest(sequence(v.creation_dt, current_date, interval '1' day)) as t(dt)
    ),
    
    -- All metamorpho supply events (filtered)
    supply_events as (
        select evt_block_time, chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_multichain.metamorpho_evt_deposit
        where contract_address in (select vault_addr from sp_vault_addr)
        union all
        select evt_block_time, chain, contract_address, shares, assets, owner, 'deposit' as side
        from metamorpho_vaults_multichain.metamorphov1_1_evt_deposit
        where contract_address in (select vault_addr from sp_vault_addr)
        union all
        select evt_block_time, chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_multichain.metamorpho_evt_withdraw
        where contract_address in (select vault_addr from sp_vault_addr)
        union all
        select evt_block_time, chain, contract_address, shares, assets, owner, 'withdraw' as side
        from metamorpho_vaults_multichain.metamorphov1_1_evt_withdraw
        where contract_address in (select vault_addr from sp_vault_addr)
    ),
    
    -- Vault supply calculations
    vault_supply as (
        select
            evt_block_time as ts,
            date(d.evt_block_time) as dt,
            d.chain as blockchain,
            v.vault_address,
            if(side = 'deposit', 1e0, -1e0) * d.shares * power(10, -v.market_decimals) as supply_shares,
            if(side = 'deposit', 1e0, -1e0) * d.assets * power(10, -v.token_decimals) as supply_amount
        from supply_events d
        join vault_data v
            on d.contract_address = v.vault_address
            and d.chain = v.blockchain
        where d.shares > 1 and d.assets > 1
    ),
    
    daily_share_prices as (
        select
            dt,
            blockchain,
            vault_address,
            max_by(supply_amount/supply_shares, ts) as daily_share_price,
            sum(supply_shares) as daily_shares_change,
            sum(supply_amount) as daily_amount_change
        from vault_supply
        group by 1, 2, 3
    ),
    
    share_price_daily as (
        select
            vs.dt,
            vs.blockchain,
            vs.vault_address,
            coalesce(
                dsp.daily_share_price,
                last_value(dsp.daily_share_price) ignore nulls over (
                    partition by vs.blockchain, vs.vault_address
                    order by vs.dt
                    rows between unbounded preceding and current row
                ),
                1.0  
            ) as share_price
        from vault_series vs
        left join daily_share_prices dsp
            on vs.dt = dsp.dt
            and vs.blockchain = dsp.blockchain
            and vs.vault_address = dsp.vault_address
    ),
    
    -- Daily vault supply amount calculation
    vault_supply_daily as (
        select
            dt,
            blockchain,
            vault_address,
            sum(supply_shares) as daily_shares_change,
            sum(supply_amount) as daily_amount_change
        from vault_supply
        group by 1, 2, 3
    ),
    
    -- Get performance fees (filtered)
    vault_performance_fees as (
        select
            date(evt_block_time) as dt,
            chain as blockchain,
            contract_address as vault_address,
            max_by(newFee * 1e-18, evt_block_time) as performance_fee
        from (
            select evt_block_time, chain, contract_address, newFee 
            from metamorpho_vaults_multichain.metamorpho_evt_setfee
            where contract_address in (select vault_addr from sp_vault_addr)
            union all
            select evt_block_time, chain, contract_address, newFee 
            from metamorpho_vaults_multichain.metamorphov1_1_evt_setfee
            where contract_address in (select vault_addr from sp_vault_addr)
        )
        group by 1, 2, 3
    ),
    
    -- Calculate cumulative vault supply amount
    vault_supply_cumulative as (
        select
            vs.dt,
            vs.blockchain,
            vs.vault_address,
            sum(coalesce(vsd.daily_shares_change, 0)) over (
                partition by vs.blockchain, vs.vault_address
                order by vs.dt
                rows between unbounded preceding and current row
            ) as cumulative_shares,
            sum(coalesce(vsd.daily_amount_change, 0)) over (
                partition by vs.blockchain, vs.vault_address
                order by vs.dt
                rows between unbounded preceding and current row
            ) as cumulative_amount
        from vault_series vs
        left join vault_supply_daily vsd
            on vs.dt = vsd.dt
            and vs.blockchain = vsd.blockchain
            and vs.vault_address = vsd.vault_address
    )

-- Final output
SELECT 
    spd.dt,
    spd.blockchain,
    spd.vault_address,
    case when spd.vault_address=0x56a76b428244a50513ec81e225a293d128fd581d then 
        coalesce(
            last_value(vpf.performance_fee) ignore nulls over (
                partition by spd.blockchain, spd.vault_address 
                order by spd.dt 
                rows between unbounded preceding and current row
            ), 0.01
        ) else 
        coalesce(
            last_value(vpf.performance_fee) ignore nulls over (
                partition by spd.blockchain, spd.vault_address 
                order by spd.dt 
                rows between unbounded preceding and current row
            ), 0
        ) 
    end as performance_fee,
    vd.token_symbol,
    spd.share_price as supply_index,
    vd.vault_symbol,
    vsc.cumulative_shares * spd.share_price as vault_supply_amount

FROM share_price_daily spd
JOIN vault_data vd 
    ON spd.blockchain = vd.blockchain 
    AND spd.vault_address = vd.vault_address
LEFT JOIN vault_performance_fees vpf
    ON spd.dt = vpf.dt 
    AND spd.blockchain = vpf.blockchain 
    AND spd.vault_address = vpf.vault_address
LEFT JOIN vault_supply_cumulative vsc
    ON spd.dt = vsc.dt
    AND spd.blockchain = vsc.blockchain
    AND spd.vault_address = vsc.vault_address

ORDER BY spd.dt DESC, spd.vault_address
