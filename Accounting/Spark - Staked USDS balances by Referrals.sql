/*
-- @title: Spark - Staked USDS balances by Referrals (Time-Weighted)
-- @description: Retrieves time-weighted staked USDS balances by referral code in SPK and SKY farms
-- @author: Steakhouse Financial
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
            contract_address,
            r."user" as user_addr,
            max_by(r.referral, r.evt_block_time) as ref_code
        from sky_ethereum.stakingrewards_evt_referral r
        join farm_addr f using (contract_address)
        group by 1,2,3
    ),
    -- Get time-weighted average staked USDS balances from pre-computed table
    time_weighted_balances as (
        select
            t.blockchain,
            t.contract_address,
            t.user_addr,
            t.dt,
            t.time_weighted_avg_balance as amount
        from query_5595724 t -- Pre-computed time-weighted average balances
        join farm_addr f on t.contract_address = f.contract_address
        where t.time_weighted_avg_balance > 1e-6 -- Filter out dust amounts
    ),
    -- Create complete time sequence for each user with time-weighted balances
    time_seq_balances as (
        select
            t.user_addr,
            t.contract_address,
            seq.dt
        from (
            select 
                user_addr, 
                contract_address, 
                min(dt) as start_dt,
                max(dt) as end_dt
            from time_weighted_balances 
            group by 1,2
        ) t
        cross join unnest(sequence(t.start_dt, t.end_dt, interval '1' day)) as seq(dt)
    ),
    -- Fill forward balances and referral codes
    balances_with_ref as (
        select
            s.dt,
            s.user_addr,
            s.contract_address,
            coalesce(b.amount, 0) as amount,
            coalesce(
                r.ref_code, 
                last_value(r.ref_code) ignore nulls over (
                    partition by s.contract_address, s.user_addr 
                    order by s.dt asc 
                    rows unbounded preceding
                )
            ) as ref_code
        from time_seq_balances s
        left join time_weighted_balances b using (dt, contract_address, user_addr)
        left join ref_events r using (dt, contract_address, user_addr)
    ),
    -- Final aggregation by referral integrator
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
            sum(s.amount) as amount -- Sum time-weighted averages across users
        from balances_with_ref s
        join farm_addr f using (contract_address)
        left join ref_codes r on s.ref_code = r.ref_code
        where s.amount > 0 -- Only include positive balances
        group by 1,2,3,4,5,6
    )

select * 
from staked_final 
order by dt desc, token desc, amount desc;
