with
    token_targets (blockchain, token_symbol, token_addr, decimals, start_date) as (
        values
            -- sUSDS
            ('ethereum', 'sUSDS', 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 18, date '2024-09-04'),
            ('base', 'sUSDS', 0x5875eEE11Cf8398102FdAd704C9E96607675467a, 18, date '2024-10-10'),
            ('arbitrum', 'sUSDS', 0xdDb46999F8891663a8F2828d25298f70416d7610, 18, date '2025-01-29'),
            ('optimism', 'sUSDS', 0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0, 18, date '2025-04-30'),
            ('unichain', 'sUSDS', 0xA06b10Db9F390990364A3984C04FaDf1c13691b5, 18, date '2025-04-30'),
            -- USDS
            ('ethereum', 'USDS', 0xdC035D45d973E3EC169d2276DDab16f1e407384F, 18, date '2024-09-02'),
            ('base', 'USDS', 0x820C137fa70C8691f0e44Dc420a5e53c168921Dc, 18, date '2024-10-10'),
            ('arbitrum', 'USDS', 0x6491c05A82219b8D1479057361ff1654749b876b, 18, date '2025-01-22'),
            ('optimism', 'USDS', 0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3, 18, date '2025-04-30'),
            ('unichain', 'USDS', 0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C, 18, date '2025-05-13'),
            -- USDC
            ('ethereum', 'USDC', 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, 6, date '2018-08-03'),
            ('base', 'USDC', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 6, date '2023-08-18'),
            ('arbitrum', 'USDC', 0xaf88d065e77c8cc2239327c5edb3a432268e5831, 6, date '2022-10-31'),
            ('optimism', 'USDC', 0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85, 6, date '2022-11-14'),
            ('unichain', 'USDC', 0x078D782b760474a361dDA0AF3839290b0EF57AD6, 6, date '2024-11-05'),
            -- USDT
            ('ethereum', 'USDT', 0xdAC17F958D2ee523a2206206994597C13D831ec7, 6, date '2017-11-28'),
            -- pyUSD
            ('ethereum', 'PYUSD',0x6c3ea9036406852006290770bedfcaba0e23a0e8, 6, date '2022-11-08')
    ),
    -- Protocol targets 
    spark_targets (code, gross_yield_code, borrow_cost_code, blockchain, protocol_name, protocol_addr, start_date) as (
        values
            -- PSM3
            (1, 'BR', 'BR', 'base', 'PSM3', 0x1601843c5E9bC251A3272907010AFa41Fa18347E, date '2024-10-23'),
            (1, 'BR', 'BR', 'arbitrum', 'PSM3', 0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266, date '2025-02-03'),
            (1, 'BR', 'BR', 'optimism', 'PSM3', 0xe0F9978b907853F354d79188A3dEfbD41978af62, date '2025-05-07'),
            (1, 'BR', 'BR', 'unichain', 'PSM3', 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f, date '2025-05-13'),
            -- ALM Proxy
            (2, 'BR', 'BR', 'ethereum', 'ALM Proxy', 0x1601843c5E9bC251A3272907010AFa41Fa18347E, date '2024-10-23'),
            (2, 'BR', 'BR', 'base', 'ALM Proxy', 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA, date '2024-10-23'),
            (2, 'BR', 'BR', 'arbitrum', 'ALM Proxy', 0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709, date '2025-02-24'),
            (2, 'BR', 'BR', 'optimism', 'ALM Proxy', 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB, date '2025-05-07'),
            (2, 'BR', 'BR', 'unichain', 'ALM Proxy', 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869, date '2025-05-15'),
            -- Spark Foundation Multisig
            (4, 'AR', 'BR', 'ethereum', 'Foundation', 0x92e4629a4510AF5819d7D1601464C233599fF5ec, date '2025-04-07')
    ),
    protocol_balances as (
        -- Ethereum transfers
        SELECT
            'ethereum' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_date) as dt,
            tr."to" as protocol_addr,
            CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_ethereum.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'ethereum'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'ethereum'
            AND tr."to" = st.protocol_addr
        WHERE DATE(tr.evt_block_date) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        SELECT
            'ethereum' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_date) as dt,
            tr."from" as protocol_addr,
            -CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_ethereum.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'ethereum'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'ethereum'
            AND tr."from" = st.protocol_addr
        WHERE DATE(tr.evt_block_date) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        -- Base transfers
        SELECT
            'base' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."to" as protocol_addr,
            CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_base.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'base'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'base'
            AND tr."to" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        SELECT
            'base' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."from" as protocol_addr,
            -CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_base.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'base'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'base'
            AND tr."from" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        -- Arbitrum transfers
        SELECT
            'arbitrum' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."to" as protocol_addr,
            CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_arbitrum.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'arbitrum'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'arbitrum'
            AND tr."to" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        SELECT
            'arbitrum' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."from" as protocol_addr,
            -CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_arbitrum.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'arbitrum'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'arbitrum'
            AND tr."from" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        -- Optimism transfers
        SELECT
            'optimism' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."to" as protocol_addr,
            CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_optimism.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'optimism'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'optimism'
            AND tr."to" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        SELECT
            'optimism' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."from" as protocol_addr,
            -CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_optimism.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'optimism'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'optimism'
            AND tr."from" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        -- Unichain transfers
        SELECT
            'unichain' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."to" as protocol_addr,
            CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_unichain.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'unichain'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'unichain'
            AND tr."to" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
        
        UNION ALL
        
        SELECT
            'unichain' as blockchain,
            tt.token_addr,
            DATE(tr.evt_block_time) as dt,
            tr."from" as protocol_addr,
            -CAST(tr.value AS DOUBLE) / POWER(10, tt.decimals) as amount
        FROM erc20_unichain.evt_Transfer tr
        JOIN token_targets tt
            ON tt.blockchain = 'unichain'
            AND tr.contract_address = tt.token_addr
        JOIN spark_targets st
            ON st.blockchain = 'unichain'
            AND tr."from" = st.protocol_addr
        WHERE DATE(tr.evt_block_time) >= tt.start_date
          AND tt.token_symbol IN ('sUSDS', 'USDS', 'USDC', 'USDT', 'PYUSD')
          AND st.code IN (1, 2, 4)
    ),
    
    protocol_balances_sum as (
        select
            blockchain,
            protocol_addr,
            token_addr,
            dt,
            sum(amount) as amount
        from protocol_balances
        group by 1,2,3,4
    ),

    seq as (
        select
            b.blockchain,
            b.protocol_addr,
            b.token_addr,
            s.dt
        from (
            select blockchain, protocol_addr, token_addr, min(dt) as start_dt 
            from protocol_balances_sum 
            group by 1,2,3
        ) b
        cross join unnest(sequence(b.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    
    protocol_balances_cum as (
        select
            s.blockchain,
            s.protocol_addr,
            tt.token_symbol,
            s.dt,
            sum(coalesce(b.amount, 0)) over (partition by s.blockchain, s.protocol_addr, s.token_addr order by s.dt asc) as amount
        from seq s
        join token_targets tt on s.blockchain=tt.blockchain and s.token_addr=tt.token_addr
        left join protocol_balances_sum b on s.blockchain=b.blockchain and s.token_addr=b.token_addr
        and s.protocol_addr=b.protocol_addr and s.dt=b.dt
    ),
    protocol_balances_cum_filter as (
        select
            b.dt,
            blockchain,
            st.code,
            st.gross_yield_code,
            st.borrow_cost_code,
            st.protocol_name,
            b.token_symbol,
            if(b.amount > 1e-6, b.amount, 0) as amount
        from protocol_balances_cum b
        join spark_targets st using (blockchain, protocol_addr)
        where (st.code = 1 and b.token_symbol in ('sUSDS', 'USDS', 'USDC'))
           or (st.code = 2 and b.token_symbol in ('sUSDS', 'USDS', 'USDC', 'USDT','PYUSD'))
           or (st.code = 4 and b.token_symbol = 'USDS')
    ),
    protocol_rates as (
        select
            b.dt,
            b.blockchain,
            b.protocol_name,
            b.token_symbol,
            b.gross_yield_code,
            r.reward_per as gross_yield_per,
            b.borrow_cost_code,
            i.reward_per as borrow_cost_per,
            b.amount
        from protocol_balances_cum_filter b
        left join query_5353955 r
            on b.gross_yield_code = r.reward_code
            and b.dt between r.start_dt and r.end_dt
        left join query_5353955 i
            on b.borrow_cost_code = i.reward_code
            and b.dt between i.start_dt and i.end_dt
    )

select *
from protocol_rates
union all 
SELECT * 
from query_5713872
order by dt desc, protocol_name asc, 
blockchain asc, token_symbol asc