/*
-- @title: Spark - Staked USDS balances by Referrals
-- @description: Retrieves staked USDS balances by referral code in SPK and SKY farms
-- @author: Steakhouse Financial
-- @notes: N/A
-- @version:
    - 1.0 - 2025-07-11 - Initial version [SJS]
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
    -- Get staked USDS amount from farms
    staked_sum as (
        select
            t.evt_block_date as dt,
            f.contract_address,
            if(t."to" = f.contract_address, t."from", t."to") as user_addr,
            sum(
                if("to" = f.contract_address, t."value", -t."value")
            ) / 1e18 as amount
        from sky_ethereum.usds_evt_transfer t
        cross join farm_addr f
        where f.contract_address in (t."from", t."to") -- Sky: Staking Reward
        group by 1,2,3
    ),
    -- Create time sequence for each user with staked USDS
    time_seq_balances as (
        select
            s.user_addr,
            s.contract_address,
            seq.dt
        from (select user_addr, contract_address, min(dt) as start_dt from staked_sum group by 1,2) s
        cross join unnest(sequence(s.start_dt, current_date, interval '1' day)) as seq(dt)
    ),
    -- Cumulative user balances by referral
    staked_cum_ref as (
        select
            dt,
            user_addr,
            contract_address,
            sum(coalesce(b.amount, 0)) over (partition by contract_address, user_addr order by dt asc) as amount,
            coalesce(r.ref_code, last_value(r.ref_code) ignore nulls over (partition by contract_address, user_addr order by dt asc)) as ref_code
        from time_seq_balances s
        left join staked_sum b using (dt, contract_address, user_addr)
        left join ref_events r using (dt, contract_address, user_addr)
    ),
    -- Cumulative user balances by integrator name, removing dust amounts
    staked_final as (
        select
            s.dt,
            'ethereum' as blockchain,
            0xdC035D45d973E3EC169d2276DDab16f1e407384F as contract_address, -- USDS
            f.farm_name as token,
            --coalesce(s.ref_code, 127) as ref_code,
            coalesce(
                s.ref_code,
                if(f.farm_code = 1, 127, 99) -- If SPK farm, 127 ('Spark unknown'); otherwise, 99 ('Unknown')
            ) as ref_code,
            case
                when s.ref_code is null then 'Spark unknown'
                when r.integrator_name is null then 'Others'
                else r.integrator_name
            end as integrator_name,
            if(sum(s.amount) > 1e-6, sum(s.amount), 0) as amount -- hide dust
        from staked_cum_ref s
        join farm_addr f using (contract_address)
        left join ref_codes r on s.ref_code = r.ref_code
        group by 1,4,5,6
    )

select * from staked_final order by dt desc, token desc, amount desc

