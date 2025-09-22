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
    
    -- Get vault creation events
    vault_creation as (
        select
            chain as blockchain,
            evt_block_time as creation_date,
            metamorpho as vault_address,
            asset as token_address,
            name as vault_name,
            symbol as vault_symbol
        from (
            select *, 1.0 as version from metamorpho_factory_multichain.metamorphofactory_evt_createmetamorpho
            union all
            select *, 1.1 as version from metamorpho_factory_multichain.metamorphov1_1factory_evt_createmetamorpho
        ) as c
        WHERE c.metamorpho in (select vault_addr from sp_vault_addr)
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
            18 as market_decimals    
        from vault_creation v
        left join tokens.erc20 t
            on v.token_address = t.contract_address
            and v.blockchain = t.blockchain
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
            max_by(supply_amount/supply_shares, ts) as daily_share_price
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
    )
    

-- Final output
SELECT 
    spd.dt,
    spd.blockchain,
    spd.vault_address,
    coalesce(
            last_value(vpf.performance_fee) ignore nulls over (
                partition by spd.blockchain, spd.vault_address 
                order by spd.dt 
                rows between unbounded preceding and current row
            ), 0
        )  as performance_fee,
    vd.token_symbol,
    spd.share_price as supply_index
FROM share_price_daily spd
JOIN vault_data vd 
    ON spd.blockchain = vd.blockchain 
    AND spd.vault_address = vd.vault_address
LEFT JOIN vault_performance_fees vpf
    ON spd.dt = vpf.dt 
    AND spd.blockchain = vpf.blockchain 
    AND spd.vault_address = vpf.vault_address
ORDER BY spd.dt DESC, spd.vault_address