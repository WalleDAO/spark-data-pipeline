/*
-- @title: Spark - Staked USDS balances by Referrals
-- @description: Retrieves staked USDS balances by referral code in SPK and SKY farms
-- @notes: Uses pre-computed time-weighted average balances from query_5595724
-- @version:
    - 1.0 - 2025-07-11 - Initial version [SJS]
    - 2.0 - 2025-08-11 - Updated to use time-weighted averages [SJS]
*/

with
    -- Farm addresses
    farm_addr (farm_code, blockchain, contract_address, farm_name) as (
        values
        (1, 'ethereum', 0x173e314C7635B45322cd8Cb14f44b312e079F3af, 'USDS-SPK'), -- USDS to SPK Farm
        (2, 'ethereum', 0x0650CAF159C5A49f711e8169D4336ECB9b950275, 'USDS-SKY')  -- USDS to SKY Farm
    ),
    -- Referral codes
    ref_codes (ref_code, integrator_name) as (
        select * from query_5354195 -- Spark - Accessibility Rewards - Referrals
    ),
    -- Referral events
    ref_events as (
        select
            r.evt_block_date as dt,
            r.contract_address,
            r."user" as user_addr,
            max_by(r.referral, r.evt_block_time) as ref_code
        from sky_ethereum.stakingrewards_evt_referral r
        join farm_addr f on r.contract_address = f.contract_address
        group by 1,2,3
    ),
    -- Get time-weighted average staked USDS balances from pre-computed table
    staked_balances as (
        select
            t.dt,
            t.contract_address,
            t.user_addr,
            t.time_weighted_avg_balance as amount
        from query_5595724 t -- Pre-computed time-weighted average balances
        join farm_addr f on t.contract_address = f.contract_address
        where t.time_weighted_avg_balance > 0 -- Filter out dust amounts
    ),
    -- Create time sequence for each user with time-weighted balances
    time_seq_balances as (
        select
            s.user_addr,
            s.contract_address,
            t.dt
        from (select user_addr, contract_address, min(dt) as start_dt from staked_balances group by 1,2) s
        cross join unnest(sequence(s.start_dt, current_date, interval '1' day)) as t(dt)
    ),
    -- User balances by referral with forward-filled referral codes
    staked_cum_ref as (
        select
            s.dt,
            s.user_addr,
            s.contract_address,
            coalesce(b.amount, 0) as amount,
            coalesce(r.ref_code, last_value(r.ref_code) ignore nulls over (partition by s.contract_address, s.user_addr order by s.dt asc rows unbounded preceding)) as ref_code
        from time_seq_balances s
        left join staked_balances b on s.dt = b.dt and s.contract_address = b.contract_address and s.user_addr = b.user_addr
        left join ref_events r on s.dt = r.dt and s.contract_address = r.contract_address and s.user_addr = r.user_addr
    ),
    -- Final aggregation by integrator name, removing dust amounts
    staked_final as (
        select
            s.dt,
            'ethereum' as blockchain,
            0xdC035D45d973E3EC169d2276DDab16f1e407384F as contract_address, -- USDS
            f.farm_name as token,
            coalesce(
                s.ref_code,
                if(f.farm_code = 1, 127, 99) -- If SPK farm, 127 ('Spark unknown'); otherwise, 99 ('Unknown')
            ) as ref_code,
            case
                when s.ref_code is null then 'Spark unknown'
                when r.integrator_name is null then 'Others'
                else r.integrator_name
            end as integrator_name,
            sum(s.amount) as amount -- hide dust
        from staked_cum_ref s
        join farm_addr f on s.contract_address = f.contract_address
        left join ref_codes r on s.ref_code = r.ref_code
        group by 1,4,5,6
    )

select * from staked_final order by dt desc, token desc, amount desc
