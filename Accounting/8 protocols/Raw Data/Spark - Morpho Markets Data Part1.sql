/*
---dune.steakhouse.result_morpho_markets_data
---https://dune.com/queries/4703784
---dune.steakhouse.result_morpho_markets
---https://dune.com/queries/4847727
*/

with
    -- Define specific Spark vaults to analyze
    sp_vault_addr (blockchain, category, symbol, vault_addr, start_date) as (
        values
            ('ethereum', 'vault', 'spDAI', 0x73e65dbd630f90604062f6e02fab9138e713edd9, date '2024-03-15'),
            ('base', 'vault', 'spUSDC', 0x7BfA7C4f149E7415b73bdeDfe609237e29CBF34A, date '2024-12-30'),
            ('ethereum', 'vault', 'spUSDS', 0xe41a0583334f0dc4e023acd0bfef3667f6fe0597, date '2025-07-16'),
            ('ethereum', 'vault', 'spUSDC', 0x56A76b428244a50513ec81e225a293d128fd581D, date '2025-09-08')
    ),

    -- Get vault addresses for filtering
    spark_vaults as (
        select
            blockchain,
            vault_addr as vault_address,
            symbol
        from sp_vault_addr
    ),

    -- Get markets that Spark vaults interact with
    spark_markets as (
        SELECT DISTINCT
            supply.id as market_id,
            supply.chain as blockchain
        FROM morpho_blue_multichain.morphoblue_evt_supply as supply
        JOIN spark_vaults
            on supply.onBehalf = spark_vaults.vault_address
            and supply.chain = spark_vaults.blockchain
        UNION
        SELECT DISTINCT
            withdraw.id as market_id,
            withdraw.chain as blockchain
        FROM morpho_blue_multichain.morphoblue_evt_withdraw as withdraw
        JOIN spark_vaults
            on withdraw.onBehalf = spark_vaults.vault_address
            and withdraw.chain = spark_vaults.blockchain
    ),

    -- Extract basic market information from creation events (filtered for Spark markets)
    raw_markets as (
        select
            c.evt_block_time as creation_ts,
            date(c.evt_block_time) as creation_dt,
            c.chain as blockchain,
            c.id as market_id,
            from_hex(lpad(json_query(c.marketParams, 'lax $.loanToken' omit quotes), 42, '0')) as loan_address,
            from_hex(lpad(json_query(c.marketParams, 'lax $.collateralToken' omit quotes), 42, '0')) as coll_address
        from morpho_blue_multichain.morphoblue_evt_createmarket c
        JOIN spark_markets sm 
            ON c.id = sm.market_id 
            AND c.chain = sm.blockchain
    ),
    
    -- Add loan token decimals information
    markets_with_decimals as (
        select
            m.creation_dt,
            m.creation_ts,
            m.blockchain,
            m.market_id,
            m.coll_address,
            coalesce(l.decimals, 18) as loan_decimals
        from raw_markets m
        left join dune.steakhouse.result_token_info l
            on m.loan_address = l.token_address
            and m.blockchain = l.blockchain
    ),

    -- Final markets table with only essential fields
    markets as (
        select 
            creation_dt,
            creation_ts,
            blockchain,
            market_id,
            coll_address,
            loan_decimals
        from markets_with_decimals
        order by creation_ts desc
    ),

    -- Generate daily time series for each market since creation
    market_series as (
        select 
            t.dt,
            m.blockchain,
            m.market_id,
            m.loan_decimals
        from markets m
        cross join unnest(sequence(m.creation_dt, current_date, interval '1' day)) as t(dt)
    ),

    -- Extract and normalize supply events (supply + withdraw) - filtered for Spark markets
    supply_events as (
        -- SUPPLY events add liquidity to market 
        select 
            s.evt_block_time as ts,
            date(s.evt_block_time) as dt,
            s.chain as blockchain,
            s.id as market_id,
            s.shares / power(10, m.loan_decimals + 6) as shares,
            s.assets / power(10, m.loan_decimals) as amount
        from morpho_blue_multichain.morphoblue_evt_supply s
        join markets m
            on s.id = m.market_id
            and s.chain = m.blockchain
        union all
        -- WITHDRAW events remove liquidity from market
        select 
            w.evt_block_time as ts,
            date(w.evt_block_time) as dt,
            w.chain as blockchain,
            w.id as market_id,
            -(w.shares / power(10, m.loan_decimals + 6)) as shares,
            -(w.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_withdraw w
        join markets m
            on w.id = m.market_id
            and w.chain = m.blockchain
    ),

    -- Extract and normalize borrow events (borrow + repay + liquidate) - filtered for Spark markets
    borrow_events as (
        -- BORROW events increase borrower liability 
        select 
            b.evt_block_time as ts,
            date(b.evt_block_time) as dt,
            b.chain as blockchain,
            b.id as market_id,
            b.shares / power(10, m.loan_decimals + 6) as shares,
            (b.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_borrow b
        join markets m
            on b.id = m.market_id
            and b.chain = m.blockchain
        union all
        -- REPAY events reduce borrower liability
        select 
            r.evt_block_time as ts,
            date(r.evt_block_time) as dt,
            r.chain as blockchain,
            r.id as market_id,
            -(r.shares / power(10, m.loan_decimals + 6)) as shares,
            -(r.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_repay r
        join markets m
            on r.id = m.market_id
            and r.chain = m.blockchain
        union all
        -- LIQUIDATE events recover assets and absorb bad debt
        select 
            l.evt_block_time as ts,
            date(l.evt_block_time) as dt,
            l.chain as blockchain,
            l.id as market_id,
            -(l.repaidShares + badDebtShares) / power(10, m.loan_decimals + 6) as shares,
            -(l.repaidAssets + badDebtAssets) / power(10, m.loan_decimals) as amount
        from morpho_blue_multichain.morphoblue_evt_liquidate l
        join markets m
            on l.id = m.market_id
            and l.chain = m.blockchain
    ),

    -- Calculate daily share prices using last transaction of the day
    daily_supply_share_prices as (
        select
            dt,
            blockchain,
            market_id,
            -- Use the last transaction's price of the day (similar to vault logic)
            max_by(amount/shares, ts) as daily_supply_share_price
        from supply_events
        where shares>1 and amount>1
        group by 1, 2, 3
    ),

    daily_borrow_share_prices as (
        select
            dt,
            blockchain,
            market_id,
            -- Use the last transaction's price of the day (similar to vault logic)
            max_by(amount/shares, ts) as daily_borrow_share_price
        from borrow_events
        where shares>1 and amount>1
        group by 1, 2, 3
    ),
    
    -- Generate complete price series using forward fill (similar to vault logic)
    supply_share_price_daily as (
        select
            ms.dt,
            ms.blockchain,
            ms.market_id,
            -- Use current day price, otherwise use last known price, default to 1.0
            coalesce(
                dssp.daily_supply_share_price,
                last_value(dssp.daily_supply_share_price) ignore nulls over (
                    partition by ms.blockchain, ms.market_id
                    order by ms.dt
                    rows between unbounded preceding and current row
                ),
                1.0
            ) as supply_share_price
        from market_series ms
        left join daily_supply_share_prices dssp
            on ms.dt = dssp.dt
            and ms.blockchain = dssp.blockchain
            and ms.market_id = dssp.market_id
    ),

    borrow_share_price_daily as (
        select
            ms.dt,
            ms.blockchain,
            ms.market_id,
            -- Use current day price, otherwise use last known price, default to 1.0
            coalesce(
                dbsp.daily_borrow_share_price,
                last_value(dbsp.daily_borrow_share_price) ignore nulls over (
                    partition by ms.blockchain, ms.market_id
                    order by ms.dt
                    rows between unbounded preceding and current row
                ),
                1.0
            ) as borrow_share_price
        from market_series ms
        left join daily_borrow_share_prices dbsp
            on ms.dt = dbsp.dt
            and ms.blockchain = dbsp.blockchain
            and ms.market_id = dbsp.market_id
    ),
    
    supply_flows_daily as (
        select 
            dt,
            blockchain,
            market_id,
            sum(shares) as supply_delta_shares
        from supply_events
        group by 1, 2, 3
    ),

    borrow_flows_daily as (
        select 
            dt,
            blockchain,
            market_id,
            sum(shares) as borrow_delta_shares
        from borrow_events
        group by 1, 2, 3
    ),
    
    -- Calculate cumulative balances
    market_balance_daily as (
        select 
            ms.dt,
            ms.blockchain,
            ms.market_id,
            coalesce(sfd.supply_delta_shares, 0) as supply_delta_shares,
            coalesce(bfd.borrow_delta_shares, 0) as borrow_delta_shares,
            sum(coalesce(sfd.supply_delta_shares, 0)) over (
                partition by ms.blockchain, ms.market_id 
                order by ms.dt
            ) as supply_total_shares,
            sum(coalesce(bfd.borrow_delta_shares, 0)) over (
                partition by ms.blockchain, ms.market_id 
                order by ms.dt
            ) as borrow_total_shares
        from market_series ms
        left join supply_flows_daily sfd
            on ms.dt = sfd.dt 
            and ms.market_id = sfd.market_id 
            and ms.blockchain = sfd.blockchain 
        left join borrow_flows_daily bfd
            on ms.dt = bfd.dt 
            and ms.market_id = bfd.market_id 
            and ms.blockchain = bfd.blockchain 
    )

-- Final output with simplified fields
select 
    mbd.dt,
    mbd.blockchain,
    mbd.market_id,
    sspd.supply_share_price * mbd.supply_total_shares as supply_amount,
    bspd.borrow_share_price * mbd.borrow_total_shares as borrow_amount,
    sspd.supply_share_price as supply_index
from market_balance_daily mbd
join supply_share_price_daily sspd
    on mbd.dt = sspd.dt 
    and mbd.market_id = sspd.market_id 
    and mbd.blockchain = sspd.blockchain
join borrow_share_price_daily bspd
    on mbd.dt = bspd.dt 
    and mbd.market_id = bspd.market_id 
    and mbd.blockchain = bspd.blockchain
order by mbd.dt desc, mbd.market_id
