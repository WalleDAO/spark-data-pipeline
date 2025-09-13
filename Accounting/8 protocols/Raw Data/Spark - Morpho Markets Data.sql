/*
---dune.steakhouse.result_morpho_markets_data
---https://dune.com/queries/4703784
---dune.steakhouse.result_morpho_markets
---https://dune.com/queries/4847727
*/

with
    raw_markets as (
        select
            c.evt_block_time as creation_ts,
            date(c.evt_block_time) as creation_dt,
            c.chain as blockchain,
            c.id as market_id,
            from_hex(lpad(json_query(c.marketParams, 'lax $.loanToken' omit quotes), 42, '0')) as loan_address,
            from_hex(lpad(json_query(c.marketParams, 'lax $.collateralToken' omit quotes), 42, '0')) as coll_address
        from morpho_blue_multichain.morphoblue_evt_createmarket c
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

    -- Extract and normalize supply events (supply + withdraw)
    supply_events as (
        -- SUPPLY events add liquidity to market 
        select 
            s.evt_block_time as ts,
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
            w.chain as blockchain,
            w.id as market_id,
            -(w.shares / power(10, m.loan_decimals + 6)) as shares,
            -(w.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_withdraw w
        join markets m
            on w.id = m.market_id
            and w.chain = m.blockchain
    ),

    -- Extract and normalize borrow events (borrow + repay + liquidate)
    borrow_events as (
        -- BORROW events increase borrower liability 
        select 
            b.evt_block_time as ts,
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
            r.chain as blockchain,
            r.id as market_id,
            -(r.shares / power(10, m.loan_decimals + 6)) as shares,
            -(r.assets / power(10, m.loan_decimals)) as amount
        from morpho_blue_multichain.morphoblue_evt_repay r
        join markets m
            on r.id = m.market_id
            and r.chain = m.blockchain
         WHERE NOT ( r.evt_tx_hash = 0xcedb342be7f0cb3de98e6eb52163891714603cdf9933e836a5155c2e8b812b69
         and r.evt_index = 528 )
        union all
        -- LIQUIDATE events recover assets and absorb bad debt
        select 
            l.evt_block_time as ts,
            l.chain as blockchain,
            l.id as market_id,
            -(l.repaidShares + badDebtShares) / power(10, m.loan_decimals + 6) as shares,
            -(l.repaidAssets + badDebtAssets) / power(10, m.loan_decimals) as amount
        from morpho_blue_multichain.morphoblue_evt_liquidate l
        join markets m
            on l.id = m.market_id
            and l.chain = m.blockchain
    ),

    -- Calculate share prices from assets/shares ratio
    shares_pricing as (
        SELECT 
            ts,
            blockchain,
            market_id,
            side,
            round(abs(amount) / abs(shares), 15) as share_price,
            to_unixtime(ts) as share_unix_ts
        from (
            select ts, blockchain, market_id, shares, amount, 'supply' as side from supply_events
            union all
            select ts, blockchain, market_id, shares, amount, 'borrow' as side from borrow_events
        )       
        WHERE abs(shares) > 1e-4 -- Filter out dust transactions
        union all
        -- Initialize markets with share price 1.0 at creation
        select 
            creation_ts as ts,
            blockchain,
            market_id,
            side,
            1.0 as share_price,
            to_unixtime(creation_ts) as share_unix_ts
        from markets
        cross join (values ('supply'), ('borrow')) as t(side)
        WHERE (t.side = 'borrow' and coll_address != 0x0000000000000000000000000000000000000000)  -- Exclude idle markets for borrow
        or t.side = 'supply'
    ),
    
    -- Pivot borrow share pricing data
    borrow_shares_pricing_pivot as (
        select 
            ts,
            'event' as row_type,
            blockchain,
            market_id,
            keccak(to_utf8(CAST(market_id as VARCHAR) || blockchain)) as market_hash,
            MAX(share_unix_ts) as borrow_unix_ts,
            MAX(share_price) as borrow_share_price
        from shares_pricing
        WHERE side = 'borrow'
        GROUP BY 1, 3, 4
    ),
    
    -- Pivot supply share pricing data
    supply_shares_pricing_pivot as (
        select 
            ts,
            'event' as row_type,
            blockchain,
            market_id,
            keccak(to_utf8(CAST(market_id as VARCHAR) || blockchain)) as market_hash,
            MAX(share_unix_ts) as supply_unix_ts,
            MAX(share_price) as supply_share_price
        from shares_pricing
        WHERE side = 'supply'
        GROUP BY 1, 3, 4
    ),

    -- Create time periods for borrow events using window functions
    borrow_event_periods as (
        select
            market_id,
            blockchain,
            LAG(borrow_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_borrow_ts,
            LAG(borrow_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_borrow_price,
            borrow_unix_ts as previous_borrow_ts,
            borrow_share_price as previous_borrow_price,
            LEAD(borrow_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_borrow_ts,
            LEAD(borrow_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_borrow_price,
            LEAD(borrow_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_borrow_ts,
            LEAD(borrow_share_price, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_borrow_price
        FROM borrow_shares_pricing_pivot
    ),
    
    -- Create time periods for supply events using window functions
    supply_event_periods as (
        select
            market_id,
            blockchain,
            LAG(supply_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_supply_ts,
            LAG(supply_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as previous_previous_supply_price,
            supply_unix_ts as previous_supply_ts,
            supply_share_price as previous_supply_price,
            LEAD(supply_unix_ts, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_supply_ts,
            LEAD(supply_share_price, 1) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_supply_price,
            LEAD(supply_unix_ts, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_supply_ts,
            LEAD(supply_share_price, 2) IGNORE NULLS OVER (PARTITION BY market_hash ORDER BY ts) as next_next_supply_price
        FROM supply_shares_pricing_pivot
    ),
    
    -- Map daily time series to supply event periods
    daily_filled_supply_periods as (
        select
            s.dt,
            s.market_id,
            s.blockchain,
            previous_previous_supply_ts, 
            previous_previous_supply_price,
            previous_supply_ts, 
            previous_supply_price,
            next_supply_ts, 
            next_supply_price,
            next_next_supply_ts, 
            next_next_supply_price,
            row_number() over (partition by s.dt, s.market_id, s.blockchain order by p.previous_supply_ts desc) as rn
        from market_series s
        left join supply_event_periods p
            on s.market_id = p.market_id
            and s.blockchain = p.blockchain
            and to_unixtime(s.dt) >= p.previous_supply_ts
    ),
    
    -- Get latest supply period for each day
    daily_supply_periods as (
        select * from daily_filled_supply_periods where rn = 1
    ),
    
    -- Map daily time series to borrow event periods
    daily_filled_borrow_periods as (
        select
            s.dt,
            s.market_id,
            s.blockchain,
            previous_previous_borrow_ts, 
            previous_previous_borrow_price,
            previous_borrow_ts, 
            previous_borrow_price,
            next_borrow_ts, 
            next_borrow_price,
            next_next_borrow_ts, 
            next_next_borrow_price,
            row_number() over (partition by s.dt, s.market_id, s.blockchain order by p.previous_borrow_ts desc) as rn
        from market_series s
        left join borrow_event_periods p
            on s.market_id = p.market_id
            and s.blockchain = p.blockchain
            and to_unixtime(s.dt) >= p.previous_borrow_ts
    ),
    
    -- Get latest borrow period for each day
    daily_borrow_periods as (
        select * from daily_filled_borrow_periods where rn = 1
    ),

    -- Interpolate share prices for daily time series
    share_price_series as (
        select
            dsp.dt,
            dsp.market_id,
            dsp.blockchain,
            CASE
                -- Base case: interpolate between previous and next price
                WHEN previous_supply_price IS NOT NULL AND next_supply_price IS NOT NULL
                    THEN previous_supply_price + (next_supply_price - previous_supply_price)/(next_supply_ts - previous_supply_ts) * (to_unixtime(dsp.dt) - previous_supply_ts)
                -- Extrapolate from future prices when no previous data
                WHEN next_supply_price IS NOT NULL AND next_next_supply_price IS NOT NULL
                    THEN GREATEST(1, next_supply_price - (next_next_supply_price - next_supply_price)/(next_next_supply_ts - next_supply_ts) * (next_supply_ts - to_unixtime(dsp.dt)))
                -- Extrapolate from previous prices when no future data
                WHEN previous_supply_price IS NOT NULL AND previous_previous_supply_price IS NOT NULL
                    THEN previous_supply_price + (previous_supply_price - previous_previous_supply_price)/(previous_supply_ts - previous_previous_supply_ts) * (to_unixtime(dsp.dt) - previous_previous_supply_ts)
                -- Fallback to available price
                ELSE COALESCE(previous_supply_price, next_supply_price)
            END as supply_share_price,
            CASE
                -- Base case: interpolate between previous and next price
                WHEN previous_borrow_price IS NOT NULL AND next_borrow_price IS NOT NULL
                    THEN previous_borrow_price + (next_borrow_price - previous_borrow_price)/(next_borrow_ts - previous_borrow_ts) * (to_unixtime(dsp.dt) - previous_borrow_ts)
                -- Extrapolate from future prices when no previous data
                WHEN next_borrow_price IS NOT NULL AND next_next_borrow_price IS NOT NULL
                    THEN GREATEST(1, next_borrow_price - (next_next_borrow_price - next_borrow_price)/(next_next_borrow_ts - next_borrow_ts) * (next_borrow_ts - to_unixtime(dsp.dt)))
                -- Extrapolate from previous prices when no future data
                WHEN previous_borrow_price IS NOT NULL AND previous_previous_borrow_price IS NOT NULL
                    THEN previous_borrow_price + (previous_borrow_price - previous_previous_borrow_price)/(previous_borrow_ts - previous_previous_borrow_ts) * (to_unixtime(dsp.dt) - previous_previous_borrow_ts)
                -- Fallback to available price
                ELSE COALESCE(previous_borrow_price, next_borrow_price)
            END as borrow_share_price
        from daily_supply_periods as dsp
        LEFT JOIN daily_borrow_periods as dsb
            ON dsp.dt = dsb.dt and dsp.blockchain = dsb.blockchain and dsp.market_id = dsb.market_id
    ),
    
    -- Final daily share prices with previous day values for rate calculation
    final_share_price_daily as (
        SELECT 
            dt,
            market_id, 
            blockchain,
            supply_share_price,
            borrow_share_price,
            LAG(supply_share_price) IGNORE NULLS OVER (partition by blockchain, market_id ORDER BY dt) as prev_supply_share_price,
            LAG(borrow_share_price) IGNORE NULLS OVER (partition by blockchain, market_id ORDER BY dt) as prev_borrow_share_price
        FROM share_price_series
        WHERE supply_share_price is not null
        union all
        -- Add market creation day with initial prices
        select 
            m.creation_dt,
            m.market_id,
            m.blockchain,
            1, 1, NULL, NULL
        from markets m
    ),
    
    -- Aggregate daily flows by market
    market_flows_daily as (
        select 
            date_trunc('day', ts) as dt,
            blockchain,
            market_id,
            SUM(if(side = 'borrow', shares, 0)) as borrow_delta_shares,
            SUM(if(side = 'supply', shares, 0)) as supply_delta_shares
        from (
            select ts, blockchain, market_id, shares, amount, 'supply' as side from supply_events
            union all
            select ts, blockchain, market_id, shares, amount, 'borrow' as side from borrow_events
        )
        GROUP BY 1, 2, 3
    ),
    
    -- Calculate cumulative balances
    market_balance_daily as (
        select 
            s.dt,
            s.blockchain,
            s.market_id,
            COALESCE(d.supply_delta_shares, 0) as supply_delta_shares,
            COALESCE(d.borrow_delta_shares, 0) as borrow_delta_shares,
            sum(coalesce(d.supply_delta_shares, 0)) over (partition by s.blockchain, s.market_id order by s.dt) as supply_total_shares,
            sum(coalesce(d.borrow_delta_shares, 0)) over (partition by s.blockchain, s.market_id order by s.dt) as borrow_total_shares
        from market_series s
        left join market_flows_daily d
            on s.dt = d.dt and s.market_id = d.market_id and s.blockchain = d.blockchain 
    ),
    
    -- Combine balances with share prices for final calculations
    market_daily as (
        select
            m.dt,
            m.blockchain,
            m.market_id,
            s.supply_share_price,
            s.borrow_share_price,
            m.supply_total_shares,
            m.borrow_total_shares,
            s.prev_supply_share_price,
            s.prev_borrow_share_price,
            s.supply_share_price * m.supply_total_shares as supply_total_amount,
            s.borrow_share_price * m.borrow_total_shares as borrow_total_amount
        from market_balance_daily m
        join final_share_price_daily s 
            on m.dt = s.dt and m.market_id = s.market_id and m.blockchain = s.blockchain
    )

-- Final output with 8 essential fields
select 
    dt,
    blockchain,
    market_id,
    supply_total_amount as supply_amount,
    borrow_total_amount as borrow_amount,
    ((supply_share_price / prev_supply_share_price) - 1) * 365 as supply_rate,
    ((borrow_share_price / prev_borrow_share_price) - 1) * 365 as borrow_rate,
    supply_share_price as supply_index
from market_daily
order by dt desc, market_id
