-- goal: all debt (supplied amount) in the Spark DAI vault in Morpho on market PT-USDS-14AUG2025

with
    -- get supply and withdraw amounts on markets
    market_ops as (
        select
            blockchain,
            dt,
            vault_address,
            market_id,
            sum(amount) as amount,
            sum(shares) as shares
        from (
            select
                chain as blockchain,
                date(evt_block_time) as dt,
                onBehalf as vault_address,
                id as market_id,
                assets as amount,
                shares
            from morpho_blue_multichain.morphoblue_evt_supply
            union all
            select
                chain as blockchain,
                date(evt_block_time) as dt,
                onBehalf as vault_address,
                id as market_id,
                -assets as amount,
                -shares
            from morpho_blue_multichain.morphoblue_evt_withdraw
        )
        where market_id = 0xa458018cf1a6e77ebbcc40ba5776ac7990e523b7cc5d0c1e740a4bbc13190d8f -- PT-USDS-14AUG2025
          and vault_address = 0x73e65DBD630f90604062f6E02fAb9138e713edD9 -- Spark DAI Vault
        group by 1,2,3,4
    ),
    -- get target markets (that had any supply and/or withdraw event)
    target_markets as (
        select distinct
            blockchain,
            market_id,
            loan_address,
            loan_decimals,
            coll_address
        from market_ops o
        join query_4847727 m using (blockchain, market_id) -- Markets
    ),
    -- get first event date per market (to generate a time sequence afterwards)
    target_markets_per_vault as (
        select
            blockchain,
            market_id,
            vault_address,
            min(o.dt) as start_dt
        from market_ops o
        join query_4847727 m using (blockchain, market_id) -- Markets
        group by 1,2,3
    ),
    -- create a time sequence for every vault & market starting from the first supplied amount
    -- and union it with market ops aggregated at day level
    market_ops_sum as (
        select
            blockchain,
            dt,
            vault_address,
            market_id,
            sum(amount) as amount,
            sum(shares) as shares
        from (
            select
                m.blockchain,
                s.dt,
                m.vault_address,
                m.market_id,
                0 as amount,
                0 as shares
            from target_markets_per_vault m
            cross join unnest(sequence(m.start_dt, current_date, interval '1' day)) as s(dt)
            union all
            select * from market_ops
        )
        group by 1,2,3,4
    ),
    -- get share price to deterime the USD value of each market shares
    market_share_price as (
        select
            blockchain,
            dt,
            market_id,
            sp.supply_index as supply_share_price
        from dune.steakhouse.result_morpho_markets_data sp
        join target_markets m using(blockchain, market_id)
    ),
    -- Cumulated shares per market
    market_ops_cum as (
        select
            blockchain,
            dt,
            vault_address,
            market_id,
            sum(shares) over (partition by blockchain, vault_address, market_id order by dt asc) as shares
        from market_ops_sum
    ),
    -- calculate usd price of market shares and market share per vault (allocation of the total market's supplied amount per vault)
    market_balance as (
        select
            dt,
            blockchain,
            'Morpho' as protocol_name,
            'PT-USDS/DAI' as token_symbol,
            o.shares * power(10, -m.loan_decimals - 6) * sp.supply_share_price as amount
        from market_ops_cum o
        left join market_share_price sp using (blockchain, dt, market_id)
        left join target_markets m using (blockchain, market_id)
    ),
    market_per_rates as (
        select
            b.*,
            'NA' as reward_code, -- r.reward_code,
            0 as reward_per,     -- r.reward_per,
            'NA' as interest_code,
            0 as interest_per
        from market_balance b
        cross join query_5353955 r-- Spark - Accessibility Rewards - Rates
        where r.reward_code = 'AR'
          and b.dt between r.start_dt and r.end_dt
    )

select * from market_per_rates order by dt desc
