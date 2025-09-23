with -- Token targets
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
-- Protocol targets
spark_targets (reward_code,ref_code,blockchain,protocol_name,protocol_addr,start_date) as (
    values 
        ( 'XR', 185,'ethereum','Euler',0x1e548CfcE5FCF17247E024eF06d32A01841fF404,date '2024-10-17'),
        ( 'XR', 185,'ethereum','Euler',0x1cA03621265D9092dC0587e1b50aB529f744aacB,date '2024-12-18'),
        ( 'XR', 185,'unichain','Euler',0x7650D7ae1981f2189d352b0EC743b9099D24086F,date '2025-06-02'),
        ( 'XR', 185,'arbitrum','Euler',0xa7a9B773f139010f284E825a74060648d91dE37a,date '2025-07-07'),
        ( 'XR', 185,'arbitrum','Euler',0x8E8FA7c73BeF97205552456A7D41E14cAf57404D,date '2025-08-31'),
        ( 'XR', 185,'arbitrum','Euler',0x0EE8D628411F446BFbbe08BDeF53E42414C8fBC4,date '2025-07-07'),
        ( 'XR', 185,'arbitrum','Euler',0x7007415237e9b1c729404A3363293e0a44EAF585,date '2025-08-31'),
        ( 'XR', 185,'base','Euler',0x65cFEF3Efbc5586f0c05299343b8BeFb3fF5d81a,date '2025-02-07'),
        ( 'XR', ,'ethereum','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-02-16'),
        ( 'XR', ,'arbitrum','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-06-10'),
        ( 'XR', ,'base','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-07-21')
),
raw_transfers as (
    select tr.chain as blockchain,
        tr.contract_address,
        tr.evt_block_date as dt,
        tr.evt_block_time as ts,
        tr.evt_block_number,
        tr.evt_tx_hash,
        tr.evt_index,
        case
            when tr."to" = st.protocol_addr then st.protocol_addr
            when tr."from" = st.protocol_addr then st.protocol_addr
        end as user_addr,
        case
            when tr."to" = st.protocol_addr then tr.value / power(10, tt.decimals) 
            when tr."from" = st.protocol_addr then - tr.value / power(10, tt.decimals) 
        end as amount_change
    from sky_multichain.usds_evt_transfer tr
        join token_targets tt on tr.chain = tt.blockchain
        and tr.contract_address = tt.token_addr
        join spark_targets st on tr.chain = st.blockchain
        and (
            tr."to" = st.protocol_addr
            or tr."from" = st.protocol_addr
        )
),
   -- Calculate running balances for all transactions
    running_balances as (
        select 
            blockchain,
            contract_address,
            user_addr,
            date(ts) as dt,
            ts,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            sum(amount_change) over (
                partition by blockchain, contract_address, user_addr 
                order by evt_block_number asc, evt_index asc
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
            running_balance as end_of_day_balance
        from (
            select 
                blockchain,
                contract_address,
                user_addr,
                dt,
                running_balance,
                row_number() over (
                    partition by blockchain, contract_address, user_addr, dt
                    order by evt_block_number desc, evt_index desc
                ) as rn
            from running_balances
        ) t
        where rn = 1
    ),
    -- Get unique user-day combinations
    user_days as (
        select distinct
            blockchain,
            contract_address,
            user_addr,
            dt
        from running_balances
    ),
    -- Calculate start-of-day balances using window function approach
    daily_start_balances as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            -- Use lag to get previous day's end balance, default to 0 if none exists
            coalesce(
                lag(end_of_day_balance, 1) over (
                    partition by blockchain, contract_address, user_addr
                    order by dt
                ), 
                0
            ) as start_of_day_balance
        from (
            -- Create a complete timeline by joining user_days with daily_end_balances
            select 
                ud.blockchain,
                ud.contract_address,
                ud.user_addr,
                ud.dt,
                deb.end_of_day_balance
            from user_days ud
            left join daily_end_balances deb
                on ud.blockchain = deb.blockchain
                and ud.contract_address = deb.contract_address
                and ud.user_addr = deb.user_addr
                and ud.dt = deb.dt
        ) timeline
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
            evt_block_number,
            evt_tx_hash,
            evt_index,
            running_balance
        from running_balances
        
        union all
        
        -- Add 00:00:00 starting points for each user-day combination
        select 
            s.blockchain,
            s.contract_address,
            s.user_addr,
            s.dt,
            cast(s.dt as timestamp) as ts, -- 00:00:00 of the day
            0 as evt_block_number, -- Special block number for daily start points
            from_hex('0000000000000000000000000000000000000000000000000000000000000000') as evt_tx_hash,
            -1 as evt_index, -- Special evt_index for daily start points
            s.start_of_day_balance as running_balance
        from daily_start_balances s
        where s.start_of_day_balance is not null -- Only add if we have a valid start balance
    ),
    -- Calculate time intervals for each balance period
    time_weighted_balances_with_end as (
        select 
            blockchain,
            contract_address,
            user_addr,
            dt,
            ts,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            running_balance,
            -- Calculate end time for each balance period
            coalesce(
                lead(ts, 1) over (
                    partition by blockchain, contract_address, user_addr, dt
                    order by evt_block_number asc, evt_index asc
                ),
                dt + interval '1' day -- End of day for last balance
            ) as end_time
        from time_weighted_balances_with_daily_start
    ),
    -- Calculate daily time-weighted average balance for transaction days only
    transaction_days_avg as (
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
    -- Get user date ranges for generating complete date sequences
    user_date_ranges as (
        select 
            blockchain,
            contract_address,
            user_addr,
            min(dt) as first_transaction_date,
            greatest(max(dt), current_date) as last_transaction_date
        from daily_end_balances
        group by 1,2,3
    ),
    -- Generate complete date sequences for each user
    complete_user_dates as (
        select 
            u.blockchain,
            u.contract_address,
            u.user_addr,
            d.dt
        from user_date_ranges u
        cross join unnest(
            sequence(u.first_transaction_date, u.last_transaction_date, interval '1' day)
        ) as d(dt)
    ),
    -- Fill in non-transaction days with previous day's end balance
    complete_daily_balances as (
        select 
            c.blockchain,
            c.contract_address,
            c.user_addr,
            c.dt,
            case 
                when t.time_weighted_avg_balance is not null then 
                    t.time_weighted_avg_balance  -- Transaction day: use time-weighted average
                else 
                    -- Non-transaction day: use previous transaction day's end balance
                    last_value(d.end_of_day_balance) ignore nulls over (
                        partition by c.blockchain, c.contract_address, c.user_addr
                        order by c.dt
                        rows unbounded preceding
                    )
            end as time_weighted_avg_balance,
            case 
                when t.time_weighted_avg_balance is not null then 'transaction_day'
                else 'no_transaction_day'
            end as day_type
        from complete_user_dates c
        left join transaction_days_avg t
            on c.blockchain = t.blockchain
            and c.contract_address = t.contract_address
            and c.user_addr = t.user_addr
            and c.dt = t.dt
        left join daily_end_balances d
            on c.blockchain = d.blockchain
            and c.contract_address = d.contract_address
            and c.user_addr = d.user_addr
            and c.dt = d.dt
    )

-- Final output: complete daily time-weighted average balances including non-transaction days
select 
    blockchain,
    contract_address,
    user_addr,
    dt,
    time_weighted_avg_balance,
    day_type
from complete_daily_balances
where time_weighted_avg_balance > 0 -- Only include users with positive balances
order by blockchain, contract_address, user_addr, dt;