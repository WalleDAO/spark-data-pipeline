-----https://dune.com/queries/5613332
with 
    -- Use pre-computed raw transfer data
    raw_transfers as (
        select 
            blockchain,
            contract_address,
            dt,
            ts,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            user_addr,
            amount_change
        from dune.sparkdotfi.result_spark_s_usds_s_usdc_balance_raw_data
        where amount_change != 0 -- Only process records with balance changes
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
