-- for daily version, refer to https://dune.com/queries/5289441

with
    claims as (
        select
            case
                when contract_address = 0xCBA0C0a2a0B6Bb11233ec4EA85C5bFfea33e724d and epoch = 1 then 'ignition'
                when contract_address = 0xCBA0C0a2a0B6Bb11233ec4EA85C5bFfea33e724d and epoch = 2 then 'overdrive'
                when contract_address = 0xe9eaE48Ed66C63fD4D12e315BC7d31Aacd89a909 then 's1-points'
                when contract_address = 0x7ac96180C4d6b2A328D3a19ac059D0E7Fc3C6d41 then 'prefarming-layer3'
                when contract_address = 0x9107F5f940226A9f21433F373A4f938228d20e1A then 'other'
                else 'unknown'
            end as batch,
            date_trunc('hour', evt_block_time) as dt,
            account as addr,
            amount / 1e18 as amount
        from spark_ethereum.sparkrewards_evt_claimed
    ),
    claims_sum as (
        select
            dt,
            sum(if(batch = 'ignition', amount, 0)) as amount_ignition,
            sum(if(batch = 'overdrive', amount, 0)) as amount_overdrive,
            sum(if(batch = 's1-points', amount, 0)) as amount_s1_points,
            sum(if(batch = 'prefarming-layer3', amount, 0)) as amount_prefarming,
            sum(if(batch = 'other', amount, 0)) as amount_other,
            sum(amount) as amount
        from claims
        group by 1
    ),
    time_seq as (
        select date_trunc('hour', s.dt) as dt
        from unnest(sequence(timestamp '2025-06-16 00:00:00', cast(current_timestamp as timestamp), interval '1' hour)) as s(dt)
    ),
    user_claims as (
        select
            s.dt,
            count(distinct if(c.batch = 'ignition', c.addr)) as num_users_ignition,
            count(distinct if(c.batch = 'overdrive', c.addr)) as num_users_overdrive,
            count(distinct if(c.batch = 's1-points', c.addr)) as num_users_s1_points,
            count(distinct if(c.batch = 'prefarming-layer3', c.addr)) as num_users_prefarming,
            count(distinct if(c.batch = 'other', c.addr)) as num_users_other,
            count(distinct c.addr) as num_users
        from time_seq s
        join claims c
            on c.dt <= s.dt
        group by 1
    ),
    staked as (
        select
            date_trunc('hour', evt_block_time) as dt,
            if("to" = 0xc6132FAF04627c8d05d6E759FAbB331Ef2D8F8fD, "from", "to") as addr,
            if("to" = 0xc6132FAF04627c8d05d6E759FAbB331Ef2D8F8fD, "value", -"value") / 1e18 as amount
        from spark_ethereum.spk_evt_transfer
        where 0xc6132FAF04627c8d05d6E759FAbB331Ef2D8F8fD in ("from", "to") -- SPK staking contract
    ),
    staked_sum as (
        select
            dt,
            sum(amount) as amount
        from staked
        group by 1
    ),
    time_seq_stakes as (
        select date_trunc('hour', s.dt) as dt
        from unnest(sequence(timestamp '2025-06-16 00:00:00', cast(current_timestamp as timestamp), interval '1' hour)) as s(dt)
    ),
    user_stakes as (
        select
            dt,
            count(distinct addr) as num_users
        from (
            select
                s.dt,
                st.addr
            from time_seq_stakes s
            join staked st
                on st.dt <= s.dt
            group by 1,2
        )
        group by 1
    ),
    -- @todo: get USD price from an oracle
    claims_cum as (
        select
            dt,
            -- # user claims
            coalesce(u.num_users, 0) as num_users_claimed,
            coalesce(u.num_users_ignition, 0) as num_users_claimed_ignition,
            coalesce(u.num_users_overdrive, 0) as num_users_claimed_overdrive,
            coalesce(u.num_users_s1_points, 0) as num_users_claimed_s1_points,
            coalesce(u.num_users_prefarming, 0) as num_users_claimed_prefarming,
            coalesce(u.num_users_other, 0) as num_users_claimed_other,
            -- amount claimed
            coalesce(c.amount, 0) as amount_claimed,
            coalesce(c.amount_ignition, 0) as amount_ignition,
            coalesce(c.amount_overdrive, 0) as amount_overdrive,
            coalesce(c.amount_s1_points, 0) as amount_s1_points,
            coalesce(c.amount_prefarming, 0) as amount_prefarming,
            coalesce(c.amount_other, 0) as amount_other,
            -- cumulative amount claimed
            sum(coalesce(c.amount, 0)) over (order by dt asc) as total_amount_claimed,
            sum(coalesce(c.amount_ignition, 0)) over (order by dt asc) as total_amount_claimed_ignition,
            sum(coalesce(c.amount_overdrive, 0)) over (order by dt asc) as total_amount_claimed_overdrive,
            sum(coalesce(c.amount_s1_points, 0)) over (order by dt asc) as total_amount_claimed_s1_points,
            sum(coalesce(c.amount_prefarming, 0)) over (order by dt asc) as total_amount_claimed_prefarming,
            sum(coalesce(c.amount_other, 0)) over (order by dt asc) as total_amount_claimed_other,
            -- staking
            coalesce(us.num_users, 0) as num_users_staked,
            coalesce(st.amount, 0) as amount_staked,
            sum(coalesce(st.amount, 0)) over (order by dt asc) as total_amount_staked,
            -- max amounts for each campaign
            300_000_000 as max_amount_ignition,
            170_000_000 as max_amount_overdrive,
            170_000_000 as max_amount_s1_points
        from time_seq s
        left join claims_sum c using (dt)
        left join user_claims u using (dt)
        left join staked_sum st using (dt)
        left join user_stakes us using (dt)
    ),
    claims_per as (
        select
            dt,
            amount_claimed,
            amount_ignition,
            amount_overdrive,
            amount_s1_points,
            amount_prefarming,
            amount_other,
            total_amount_claimed,
            total_amount_claimed_ignition,
            total_amount_claimed_overdrive,
            total_amount_claimed_s1_points,
            total_amount_claimed_prefarming,
            total_amount_claimed_other,
            -- max amounts
            max_amount_ignition,
            max_amount_overdrive,
            max_amount_s1_points,
            -- claim percentages
            if(total_amount_claimed_ignition / max_amount_ignition < 1e-6, 0, total_amount_claimed_ignition / max_amount_ignition) as total_per_ignition,
            if(total_amount_claimed_overdrive / max_amount_overdrive < 1e-6, 0, total_amount_claimed_overdrive / max_amount_overdrive) as total_per_overdrive,
            if(total_amount_claimed_s1_points / max_amount_s1_points < 1e-6, 0, total_amount_claimed_s1_points / max_amount_s1_points) as total_per_s1_points,
            -- user counts
            num_users_claimed,
            num_users_claimed_ignition,
            num_users_claimed_overdrive,
            num_users_claimed_s1_points,
            num_users_claimed_prefarming,
            num_users_claimed_other,
            -- staking data
            amount_staked,
            total_amount_staked,
            num_users_staked
        from claims_cum
    )

select * from claims_per order by dt desc
