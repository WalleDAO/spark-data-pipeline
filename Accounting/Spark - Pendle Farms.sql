/*
-- @title: Spark - Pendle Farms
-- @description: Retrieves USDS balance from Pendle pools: USDS-14AUG2025 and USDS-SPK
-- @dev: for Pendle USDS points (USDS-14AUG2025), since Spark offers leverage on it via
--       the Morpho DAI vault against PT-USDS, we need to deduct off 40bps. Given it expires
--       in Aug'25, we set the value to 0.2%
-- @version:
    - 1.0 - 2025-07-17 - Initial version [SJS]
    - 1.1 - 2025-07-28 - Setting 0.2% to pool USDS-14AUG2025
*/

with
    tokens (blockchain, contract_address, "type", symbol, decimals, start_date) as (
        values
        -- USDS-14AUG2025
        ('ethereum', 0xdace1121e10500e9e29d071f01593fd76b000f08, 'lp', 'LP-USDS', 18, date '2025-05-13'), -- pool
        ('ethereum', 0x508defdb5dd2adeefe36f58fdcd75d6efa36697b, 'sy', 'SY-USDS', 18, date '2025-05-13'), -- sy
        -- USDS-SPK
        ('ethereum', 0xff43e751f2f07bbf84da1fc1fa12ce116bf447e5, 'lp', 'LP-USDS-SPK', 18, date '2025-07-04'), -- pool
        ('ethereum', 0x0ee69a11b4391c5af5eb2fb088c2df5dd2a0d075, 'sy', 'SY-USDS-SPK', 18, date '2025-07-04')  -- sy
    ),
    transfers as (
        select
            tk."type",
            contract_address,
            tk.symbol,
            tr.block_date as dt,
            tr."to" as addr,
            tr.amount_raw * power(10, -tk.decimals) as amount
        from tokens.transfers tr
        join tokens tk using (blockchain, contract_address)
        where tr.block_date >= tk.start_date
          and tk."type" = 'sy'
          and tr."to" in (select contract_address from tokens where "type" = 'lp')
        union all
        select
            tk."type",
            contract_address,
            tk.symbol,
            tr.block_date as dt,
            tr."from" as addr,
            -tr.amount_raw * power(10, -tk.decimals) as amount
        from tokens.transfers tr
        join tokens tk using (blockchain, contract_address)
        where tr.block_date >= tk.start_date
          and tk."type" = 'sy'
          and tr."from" in (select contract_address from tokens where "type" = 'lp')
    ),
    supply_sum as (
        select
            "type",
            contract_address,
            symbol,
            dt,
            sum(amount) as amount
        from transfers
        group by 1,2,3,4
    ),
    supply_cum as (
        select
            dt,
            tk."type",
            tk.symbol,
            sum(coalesce(ss.amount, 0)) over (partition by tk.symbol order by dt asc) as amount
        from unnest(sequence(date '2025-05-13', current_date, interval '1' day)) as s(dt)
        cross join tokens tk
        left join supply_sum ss using (dt, contract_address)
        where tk."type" = 'sy'
    ),
    supply_rates as (
        select
            b.dt,
            'ethereum' as blockchain,
            'Pendle' as protocol_name,
            b.symbol as token_symbol,
            if(b.symbol = 'SY-USDS', concat(r.reward_code, '*'), r.reward_code) as reward_code,
            -- Pendle USDS points -> since Spark offers leverage on it via the Morpho DAI vault against PT-USDS, we need to deduct off 40bps
            if(b.symbol = 'SY-USDS', 0.002, r.reward_per) as reward_per,
            i.reward_code as interest_code,
            i.reward_per as interest_per,
            amount as amount,
            amount as amount_usd
        from supply_cum b
        cross join query_5353955 r-- Spark - Accessibility Rewards - Rates -> rebates
        cross join query_5353955 i-- Spark - Accessibility Rewards - Rates -> interest
        where r.reward_code = 'XR'
          and b.dt between r.start_dt and r.end_dt
          and i.reward_code = 'NA'
          and b.dt between i.start_dt and i.end_dt
    )

select * from supply_rates order by dt desc
