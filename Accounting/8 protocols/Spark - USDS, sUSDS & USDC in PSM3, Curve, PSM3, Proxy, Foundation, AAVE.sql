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
    spark_targets (code, reward_code, interest_code, blockchain, protocol_name, protocol_addr, start_date) as (
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
            (4, 'AR', 'BR', 'ethereum', 'Foundation', 0x92e4629a4510AF5819d7D1601464C233599fF5ec, date '2025-04-07'),
            -- Curve USDT
            (6, 'BR', 'BR', 'ethereum', 'Curve', 0x00836Fe54625BE242BcFA286207795405ca4fD10, date '2025-04-07'),
            -- AAVE aEthUSDS - Track ALM supply to aETHUSDS
            (7, 'AR', 'BR', 'ethereum', 'AAVE aETHUSDS', 0x32a6268f9Ba3642Dda7892aDd74f1D34469A4259, date '2024-10-02')
    ),
    protocol_balances as (
        -- Standard protocol balance tracking for codes 1,2,4,6
        select
            tr.blockchain,
            tt.token_addr,
            tr.block_date as dt,
            tr."to" as protocol_addr,
            tr.amount_raw / power(10, tt.decimals) as amount
        from tokens.transfers tr
        join token_targets tt
            on tr.blockchain = tt.blockchain
            and tr.contract_address = tt.token_addr
        join spark_targets st
            on tr.blockchain = st.blockchain
            and tr."to" = st.protocol_addr
        where date(tr.block_date) >= tt.start_date
          and tt.token_symbol in ('sUSDS', 'USDS', 'USDC', 'USDT')
          and st.code in (1, 2, 4, 6)
        
        union all
        
        select
            tr.blockchain,
            tt.token_addr,
            tr.block_date as dt,
            tr."from" as protocol_addr,
            -tr.amount_raw / power(10, tt.decimals) as amount
        from tokens.transfers tr
        join token_targets tt
            on tr.blockchain = tt.blockchain
            and tr.contract_address = tt.token_addr
        join spark_targets st
            on tr.blockchain = st.blockchain
            and tr."from" = st.protocol_addr
        where date(tr.block_date) >= tt.start_date
          and tt.token_symbol in ('sUSDS', 'USDS', 'USDC', 'USDT')
          and st.code in (1, 2, 4, 6)
        
        union all
        
        -- Special tracking for AAVE aETHUSDS: Track ALM Proxy supplies to aETHUSDS
        select
            tr.blockchain,
            tt_usds.token_addr,
            tr.block_date as dt,
            st_aave.protocol_addr as protocol_addr,
            tr.amount_raw / power(10, tt_usds.decimals) as amount
        from tokens.transfers tr
        join token_targets tt_usds
            on tr.blockchain = tt_usds.blockchain
            and tr.contract_address = tt_usds.token_addr
            and tt_usds.token_symbol = 'USDS'
        join spark_targets st_alm
            on tr.blockchain = st_alm.blockchain
            and tr."from" = st_alm.protocol_addr
            and st_alm.code = 2  -- ALM Proxy
        join spark_targets st_aave
            on tr.blockchain = st_aave.blockchain
            and tr."to" = st_aave.protocol_addr
            and st_aave.code = 7  -- AAVE aETHUSDS
        where date(tr.block_date) >= st_aave.start_date
        
        union all
        
        select
            tr.blockchain,
            tt_usds.token_addr,
            tr.block_date as dt,
            st_aave.protocol_addr as protocol_addr,
            -tr.amount_raw / power(10, tt_usds.decimals) as amount
        from tokens.transfers tr
        join token_targets tt_usds
            on tr.blockchain = tt_usds.blockchain
            and tr.contract_address = tt_usds.token_addr
            and tt_usds.token_symbol = 'USDS'
        join spark_targets st_alm
            on tr.blockchain = st_alm.blockchain
            and tr."to" = st_alm.protocol_addr
            and st_alm.code = 2  -- ALM Proxy
        join spark_targets st_aave
            on tr.blockchain = st_aave.blockchain
            and tr."from" = st_aave.protocol_addr
            and st_aave.code = 7  -- AAVE aETHUSDS
        where date(tr.block_date) >= st_aave.start_date
    ),
    totals_check as (
        select blockchain, token_addr, protocol_addr, sum(amount) as amount from protocol_balances group by 1,2,3
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
        from (select blockchain, protocol_addr, token_addr, min(dt) as start_dt from protocol_balances_sum group by 1,2,3) b
        cross join unnest(sequence(b.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    -- Calculate cumulative balance
    protocol_balances_cum as (
        select
            blockchain,
            protocol_addr,
            tt.token_symbol,
            dt,
            sum(coalesce(b.amount, 0)) over (partition by blockchain, protocol_addr, token_addr order by dt asc) as amount
        from seq s
        join token_targets tt using (blockchain, token_addr)
        join spark_targets st using (blockchain, protocol_addr)
        left join protocol_balances_sum b using (blockchain, protocol_addr, token_addr, dt)
    ),
    protocol_balances_cum_filter as (
        select
            b.dt,
            blockchain,
            st.code,
            case
                when st.reward_code != '?' then st.reward_code
                when b.token_symbol = 'USDS' then 'AR'
                when b.token_symbol = 'sUSDS' then 'XR'
                when b.token_symbol = 'USDC' then 'BR'
            end as reward_code,
            st.interest_code,
            st.protocol_name,
            b.token_symbol,
            if(b.amount > 1e-6, b.amount, 0) as amount
        from protocol_balances_cum b
        join spark_targets st using (blockchain, protocol_addr)
        where (st.code = 1 and b.token_symbol in ('sUSDS', 'USDS', 'USDC'))
           or (st.code = 2 and b.token_symbol in ('sUSDS', 'USDS', 'USDC', 'USDT','PYUSD'))
           or (st.code = 4 and b.token_symbol = 'USDS')
           or (st.code = 6 and b.token_symbol in ('USDT','sUSDS'))
           or (st.code = 7 and b.token_symbol = 'USDS')
    ),
    protocol_rates as (
        select
            b.dt,
            b.blockchain,
            b.protocol_name,
            b.token_symbol,
            r.reward_code,
            r.reward_per,
            b.interest_code,
            -i.reward_per as interest_per,
            b.amount
        from protocol_balances_cum_filter b
        left join query_5353955 r
            on b.reward_code = r.reward_code
            and b.dt between r.start_dt and r.end_dt
        left join query_5353955 i
            on b.interest_code = i.reward_code
            and b.dt between i.start_dt and i.end_dt
    )

select *
from protocol_rates
order by dt desc, protocol_name asc, 
blockchain asc, token_symbol asc
