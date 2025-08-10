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
    -- Use pre-computed raw transfer data
    raw_transfers as (
        select 
            blockchain,
            contract_address,
            dt,
            ts, -- Now we have precise timestamps!
            user_addr,
            amount_change
        from dune.sparkdotfi.result_spark_s_usds_s_usdc_balance_raw_data
        where amount_change != 0 -- Only process records with balance changes
    ),
    -- Data validation: check total supply
    total_supply_check as (
        select
            blockchain,
            contract_address,
            sum(amount_change) as total_amount
        from raw_transfers
        group by 1,2
    ),
    -- Referral events
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
        group by 1,2,3,4
    ),
    -- First, calculate running balances for all transactions
    running_balances as (
        select 
            blockchain,
            contract_address,
            user_addr,
            date(ts) as dt,
            ts,
            sum(amount_change) over (
                partition by blockchain, contract_address, user_addr 
                order by ts asc
                rows unbounded preceding
            ) as running_balance
        from raw_transfers
    ),
    -- Get the last balance of each day for each user
    daily_end_balances as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            max_by(running_balance, ts) as end_of_day_balance
        from running_balances
        group by 1,2,3,4
    ),
    -- Calculate start-of-day balances (previous day's end balance)
    daily_start_balances as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt + interval '1' day as next_dt,
            end_of_day_balance as start_of_day_balance
        from daily_end_balances
    ),
    -- Combine all balance change points with daily start points
    time_weighted_balances_with_daily_start as (
        -- Original balance change records
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            ts,
            running_balance
        from running_balances
        
        union all
        
        -- Add 00:00:00 starting points for each day
        select distinct
            r.blockchain,
            r.contract_address,
            r.user_addr,
            r.dt,
            cast(r.dt as timestamp) as ts, -- 00:00:00 of the day
            coalesce(s.start_of_day_balance, 0) as running_balance
        from running_balances r
        left join daily_start_balances s 
            on r.blockchain = s.blockchain 
            and r.contract_address = s.contract_address
            and r.user_addr = s.user_addr
            and r.dt = s.next_dt
        where not exists (
            -- Only add if there's no transaction exactly at 00:00:00
            select 1 from running_balances r2 
            where r2.blockchain = r.blockchain 
              and r2.contract_address = r.contract_address
              and r2.user_addr = r.user_addr
              and r2.dt = r.dt
              and r2.ts = cast(r.dt as timestamp)
        )
    ),
    -- Calculate time intervals for each balance period
    time_weighted_balances_with_end as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            ts,
            running_balance,
            -- Calculate end time for each balance period
            coalesce(
                lead(ts, 1) over (
                    partition by blockchain, contract_address, user_addr, dt
                    order by ts asc
                ),
                dt + interval '1' day -- End of day for last balance
            ) as end_time
        from time_weighted_balances_with_daily_start
    ),
    -- Calculate daily time-weighted average balance
    daily_time_weighted_avg as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            -- Time-weighted average = Σ(balance × duration) / total_time
            case 
                when sum(date_diff('second', ts, end_time)) > 0 then
                    sum(running_balance * date_diff('second', ts, end_time)) / 
                    sum(date_diff('second', ts, end_time))
                else 0
            end as time_weighted_avg_balance
        from time_weighted_balances_with_end
        where date(ts) = dt -- Ensure within the same day
        group by 1,2,3,4
    ),
    -- Get referral users with their start dates
    ref_users as (
        select 
            blockchain,
            contract_address,
            user_addr,
            min(dt) as start_date
        from ref_events
        group by 1,2,3
    ),
    -- Create time sequence for each token, chain and user, starting from the first date of referral x user
    time_seq_balances as (
        select
            date_seq.dt,
            u.blockchain,
            u.contract_address,
            u.user_addr
        from ref_users u
        join (select blockchain, contract_address, user_addr, min(dt) as start_date from daily_time_weighted_avg group by 1,2,3) tt
            on u.blockchain = tt.blockchain 
            and u.contract_address = tt.contract_address 
            and u.user_addr = tt.user_addr
        cross join unnest(sequence(tt.start_date, current_date, interval '1' day)) as date_seq(dt)
    ),
    -- Get cumulative time-weighted balances with referral codes
    cum_balances_ref as (
        select
            tsb.blockchain,
            tsb.contract_address,
            tsb.dt,
            tsb.user_addr,
            coalesce(b.time_weighted_avg_balance, 0) as amount,
            coalesce(r.ref_code, last_value(r.ref_code) ignore nulls over (partition by tsb.blockchain, tsb.contract_address, tsb.user_addr order by tsb.dt asc)) as ref_code
        from time_seq_balances tsb
        left join daily_time_weighted_avg b 
            on tsb.blockchain = b.blockchain 
            and tsb.contract_address = b.contract_address
            and tsb.dt = b.dt
            and tsb.user_addr = b.user_addr
        left join ref_events r 
            on tsb.blockchain = r.blockchain 
            and tsb.contract_address = r.contract_address
            and tsb.dt = r.dt
            and tsb.user_addr = r.user_addr
    ),
    -- Aggregate total balance by referral code
    total_balance_ref as (
        select
            b.dt,
            b.blockchain,
            b.contract_address,
            tt.symbol as token,
            b.ref_code,
            coalesce(r.integrator_name, 'untagged') as integrator_name,
            sum(b.amount) as amount
        from cum_balances_ref b
        join token_targets tt 
            on b.blockchain = tt.blockchain 
            and b.contract_address = tt.contract_address
        left join ref_codes r on b.ref_code = r.ref_code
        where b.ref_code is not null
          and b.ref_code not in (100, 101) -- Already retrieved at protocol-level
        group by 1,2,3,4,5,6
    )

select * from total_balance_ref order by dt desc, blockchain asc, token asc, ref_code asc
