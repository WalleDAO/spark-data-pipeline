--header

with
    -- Referral codes
    ref_codes (ref_code, integrator_name) as (
        select * from query_5354195 -- Spark - Accessibility Rewards - Referrals
    ),
    -- Token targets: sUSDS and sUSDC in all chains
    token_targets (blockchain, symbol, contract_address, start_date) as (
        values
            -- sUSDS
            ('ethereum', 'sUSDS', 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, date '2024-09-04'),
            ('base', 'sUSDS', 0x5875eEE11Cf8398102FdAd704C9E96607675467a, date '2024-10-10'), -- No sUSDS deposits on base, but PSM3 swaps
            ('arbitrum', 'sUSDS', 0xdDb46999F8891663a8F2828d25298f70416d7610, date '2025-01-29'), -- No sUSDS deposits on arbitrum, but PSM3 swaps
            ('optimism', 'sUSDS', 0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0, date '2025-04-30'), -- No sUSDS deposits on optimism, but PSM3 swaps
            ('unichain', 'sUSDS', 0xA06b10Db9F390990364A3984C04FaDf1c13691b5, date '2025-04-30'), -- No sUSDS deposits on unichain, but PSM3 swaps
            -- sUSDC
            ('ethereum', 'sUSDC', 0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE, date '2025-03-03'),
            ('base', 'sUSDC', 0x3128a0F7f0ea68E7B7c9B00AFa7E41045828e858, date '2025-03-03'),
            ('arbitrum', 'sUSDC', 0x940098b108fB7D0a7E374f6eDED7760787464609, date '2025-03-03'),
            ('optimism', 'sUSDC', 0xCF9326e24EBfFBEF22ce1050007A43A3c0B6DB55, date '2025-05-26'),
            ('unichain', 'sUSDC', 0x14d9143BEcC348920b68D123687045db49a016C6, date '2025-05-14')
    ),
    -- field <start_date> as the starting day of sUSDS balance in PSM3 & ALM Proxy contracts  
    spark_targets (blockchain, symbol, contract_address, start_date) as (
        values
        -- PSM3
            ('base', 'PSM3', 0x1601843c5E9bC251A3272907010AFa41Fa18347E, date '2024-11-05'),
            ('arbitrum', 'PSM3', 0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266, date '2025-03-01'),
            ('optimism', 'PSM3', 0xe0F9978b907853F354d79188A3dEfbD41978af62, date '2025-06-02'),
            ('unichain', 'PSM3', 0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f, date '2025-06-02'),
        -- ALM Proxy
            ('base', 'ALM Proxy', 0x2917956eFF0B5eaF030abDB4EF4296DF775009cA, date '2024-11-18'),
            ('arbitrum', 'ALM Proxy', 0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709, date '2025-02-24'),
            ('optimism', 'ALM Proxy', 0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB, date '2025-06-02'),
            ('unichain', 'ALM Proxy', 0x345E368fcCd62266B3f5F37C9a131FD1c39f5869, date '2025-06-02')
    ),
    ref_events as (
        select
            blockchain,
            date(ts) as dt,
            contract_address,
            user_addr,
            max_by(ref_code, ts) as ref_code
        from (
            select
                blockchain,
                evt_block_time as ts,
                owner as user_addr,
                contract_address,
                referral as ref_code
            from (
                -- refs from sUSDS deposits on ethereum
                select 'ethereum' as blockchain, evt_block_time, owner, contract_address, referral
                from sky_ethereum.susds_evt_referral
                union all
                -- refs from sUSDC deposits on ethereum
                select 'ethereum' as blockchain, evt_block_time, owner, contract_address, referral
                from sky_ethereum.usdcvault_evt_referral
                union all
                -- refs from sUSDC deposits on arbitrum, base, optimism & unichain
                select chain as blockchain, evt_block_time, owner, contract_address, referral
                from sky_multichain.usdcvaultl2_evt_referral
                union all
                -- refs from sUSDS swaps in COW on base, arbitrum, optimism & unichain (flow PSM3->user/protocol)
                select
                    chain as blockchain,
                    evt_block_time,
                    receiver as owner,
                    tt.contract_address, -- sUSDS
                    cast(referralCode as int256) as referral
                from spark_protocol_multichain.psm3_evt_swap s
                join token_targets tt on s.chain = tt.blockchain
                where tt.contract_address = assetOut -- sUSDS
                  and tt.symbol = 'sUSDS'
                union all
                -- labeling PSM3 and ALM Proxy
                select
                    blockchain,
                    st.start_date as evt_block_time,
                    st.contract_address as owner, -- PSM3 & ALM Proxy
                    tt.contract_address, -- sUSDS addr
                    if(st.symbol='PSM3', 100, 101) as referral
                from spark_targets st
                join token_targets tt using (blockchain)
                where tt.symbol = 'sUSDS'
            )
        )
        --where ref_code between 100 and 999
        group by 1,2,3,4
    ),
    ref_users as (
        select 
            blockchain,
            contract_address,
            user_addr,
            min(dt) as start_date
        from ref_events
        group by 1,2,3
    ),
    token_transfers as (
        select
            blockchain,
            contract_address,
            tr.block_date as dt,
            tr."to" as user_addr,
            tr.amount_raw / 1e18 as amount
        from tokens.transfers tr
        join token_targets ta using (blockchain, contract_address)
        where date(tr.block_date) >= ta.start_date
          and "to" != 0x0000000000000000000000000000000000000000
          and contract_address != 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD -- sUSDS @ ethereum (retrieved further below)
        union all
        select
            blockchain,
            contract_address,
            tr.block_date as dt,
            tr."from" as user_addr,
            -tr.amount_raw / 1e18 as amount
        from tokens.transfers tr
        join token_targets ta using (blockchain, contract_address)
        where date(tr.block_date) >= ta.start_date
          and "from" != 0x0000000000000000000000000000000000000000
          and contract_address != 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD -- sUSDS @ ethereum (retrieved further below)
        union all
        select
            'ethereum' as blockchain,
            contract_address,
            tr.evt_block_date as dt,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from sky_ethereum.susds_evt_transfer tr
        join token_targets ta using (contract_address)
        where "to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'ethereum'
        union all
        select
            'ethereum' as blockchain,
            contract_address,
            tr.evt_block_date as dt,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from sky_ethereum.susds_evt_transfer tr
        join token_targets ta using (contract_address)
        where "from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'ethereum'
    ),
    token_transfers_sum as (
        select
            blockchain,
            contract_address,
            dt,
            user_addr,
            sum(amount) as amount
        from token_transfers
        group by 1,2,3,4
    ),
    -- just to double check totalSupply is correct
    total_supply_check as (
        select
            blockchain,
            contract_address,
            sum(amount) as amount
        from token_transfers
        group by 1,2
    ),
    -- create time sequence for each token, chain and user, starting from the first date of referral x user
    time_seq_balances as (
        select
            s.dt,
            blockchain,
            contract_address,
            user_addr
        from ref_users u
        join (select blockchain, contract_address, user_addr, min(dt) as start_date from token_transfers_sum group by 1,2,3) tt
            using (blockchain, contract_address, user_addr)
        cross join unnest(sequence(tt.start_date, current_date, interval '1' day)) as s(dt)
    ),
    cum_balances as (
        select
            blockchain,
            contract_address,
            dt,
            user_addr,
            sum(coalesce(t.amount, 0)) over (partition by blockchain, contract_address, user_addr order by dt asc) as amount
        from time_seq_balances s
        left join token_transfers_sum t using (dt, blockchain, contract_address, user_addr)
    ),
    cum_balances_ref as (
        select
            blockchain,
            contract_address,
            dt,
            user_addr,
            amount,
            coalesce(r.ref_code, last_value(r.ref_code) ignore nulls over (partition by blockchain, contract_address, user_addr order by dt asc)) as ref_code
        from cum_balances b
        left join ref_events r using (blockchain, contract_address, dt, user_addr)
    ),
    total_balance_ref as (
        select
            b.dt,
            blockchain,
            contract_address,
            tt.symbol as token,
            b.ref_code,
            coalesce(r.integrator_name, 'untagged') as integrator_name,
            sum(b.amount) as amount
        from cum_balances_ref b
        join token_targets tt using (blockchain, contract_address)
        left join ref_codes r on b.ref_code = r.ref_code
        where b.ref_code is not null
          and b.ref_code not in (100, 101) -- Already retrievred at protocol-level
        group by 1,2,3,4,5,6
    )

select * from total_balance_ref order by dt desc, blockchain asc, token asc, ref_code asc
