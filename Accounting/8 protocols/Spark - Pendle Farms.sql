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
            tr.contract_address,
            tk.symbol,
            date(tr.evt_block_time) as dt,
            tr."to" as addr,
            cast(tr.value as double) / power(10, tk.decimals) as amount
        from erc20_ethereum.evt_Transfer tr
        join tokens tk on (tr.contract_address = tk.contract_address)
        where date(tr.evt_block_time) >= tk.start_date
          and tk."type" = 'sy'
          and tr."to" in (select contract_address from tokens where "type" = 'lp')
        
        union all
        
        -- SY tokens transferred FROM LP pools (withdrawals)
        select
            tk."type",
            tr.contract_address,
            tk.symbol,
            date(tr.evt_block_time) as dt,
            tr."from" as addr,
            -cast(tr.value as double) / power(10, tk.decimals) as amount
        from erc20_ethereum.evt_Transfer tr
        join tokens tk on (tr.contract_address = tk.contract_address)
        where date(tr.evt_block_time) >= tk.start_date
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
            if(b.symbol = 'SY-USDS', concat(r.reward_code, '*'), r.reward_code) as gross_yield_code,
            -- Pendle USDS points -> deduct 40bps using APR conversion
            if(b.symbol = 'SY-USDS', 365 * (exp(ln(1 + 0.002) / 365) - 1), r.reward_per) as gross_yield_apr,
            amount as amount
        from supply_cum b
        cross join query_5353955 r-- Spark - Accessibility Rewards - Rates -> rebates (now APR)
        where r.reward_code = 'XR'
          and b.dt between r.start_dt and r.end_dt
    )

select * from supply_rates order by dt desc