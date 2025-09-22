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
            ('base', 'sUSDS', 0x5875eEE11Cf8398102FdAd704C9E96607675467a, date '2024-10-10'),
            ('arbitrum', 'sUSDS', 0xdDb46999F8891663a8F2828d25298f70416d7610, date '2025-01-29'),
            ('optimism', 'sUSDS', 0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0, date '2025-04-30'),
            ('unichain', 'sUSDS', 0xA06b10Db9F390990364A3984C04FaDf1c13691b5, date '2025-04-30'),
            -- sUSDC
            ('ethereum', 'sUSDC', 0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE, date '2025-03-03'),
            ('base', 'sUSDC', 0x3128a0F7f0ea68E7B7c9B00AFa7E41045828e858, date '2025-03-03'),
            ('arbitrum', 'sUSDC', 0x940098b108fB7D0a7E374f6eDED7760787464609, date '2025-03-03'),
            ('optimism', 'sUSDC', 0xCF9326e24EBfFBEF22ce1050007A43A3c0B6DB55, date '2025-05-26'),
            ('unichain', 'sUSDC', 0x14d9143BEcC348920b68D123687045db49a016C6, date '2025-05-14')
    ),
    -- Protocol contracts that need special referral codes
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
    
    -- Enhanced balance data with protocol referral codes
    enhanced_balances as (
        select 
            b.dt,
            b.blockchain,
            b.contract_address,
            b.user_addr,
            b.time_weighted_avg_balance,
            -- Override referral code for protocol contracts
            case 
                when st.contract_address is not null then
                    if(st.symbol = 'PSM3', 100, 101)
                else b.ref_code
            end as ref_code
        from dune.sparkdotfi.result_spark_s_usds_s_usdc_time_weighted_average_balance b
        left join spark_targets st on (
            b.blockchain = st.blockchain 
            and b.user_addr = st.contract_address -- PSM3/ALM Proxy as user
        )
    ),
    
    -- Final aggregation by referral code
    total_balance_ref as (
        select
            b.dt,
            b.blockchain,
            b.contract_address,
            tt.symbol as token,
            case when tt.symbol='sUSDC' and  b.ref_code=-999999 then 127 
            when tt.symbol='sUSDS' and b.ref_code=-999999 then 99 else b.ref_code end as  ref_code,
            coalesce(r.integrator_name, 'untagged') as integrator_name,
            sum(b.time_weighted_avg_balance) as amount
        from enhanced_balances b
        join token_targets tt on (
            b.blockchain = tt.blockchain 
            and b.contract_address = tt.contract_address
        )
        left join ref_codes r on case when tt.symbol='sUSDC' and  b.ref_code=-999999 then 127 
            when tt.symbol='sUSDS' and b.ref_code=-999999 then 99 else b.ref_code end  = r.ref_code
        where b.ref_code is not null
          and b.ref_code not in (100, 101) 
        group by 1,2,3,4,5,6
    )

select * from total_balance_ref 
order by dt desc, blockchain asc, token asc, ref_code asc
