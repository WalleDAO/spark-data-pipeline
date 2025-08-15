with
    daily_susds as (
        select
            b.dt,
            b.blockchain,
            b.ref_code,
            b.integrator_name,
            sum(b.amount) as amount,
            avg(avg((b.amount / 365) * p.reward_per)) over (
                partition by b.blockchain, b.ref_code, b.integrator_name
                order by b.dt rows between 6 preceding and current row
            ) as tw_reward_7d_avg
        from dune.sparkdotfi.result_spark_susds_susdc_refs b -- Spark - sUSDS & sUSDC balances by Referrals
        cross join query_5353955 p -- Spark - Accessibility Rewards - Rates
        where b.blockchain = 'ethereum'
          and b.token = 'sUSDS'
          and b.ref_code between 100 and 999
          and p.reward_code = 'XR'
          and b.dt between p.start_dt and p.end_dt
        group by 1,2,3,4
    ),
    monthly_susds as (
        select
            date_trunc('month', b.dt) as dt,
            b.blockchain,
            b.ref_code,
            b.integrator_name,
            p.reward_code,
            p.reward_per,
            sum(b.amount) / 365 as tw_amount,
            sum((b.amount / 365) * p.reward_per) as tw_reward,
            max_by(b.tw_reward_7d_avg, dt) as tw_reward_7d_avg,
            max_by(b.tw_reward_7d_avg, dt) * 365 as tw_reward_proj_1y
        from daily_susds b
        cross join query_5353955 p -- Spark - Accessibility Rewards - Rates
        where p.reward_code = 'XR'
          and b.dt between p.start_dt and p.end_dt
        group by 1,2,3,4,5,6
    )

select * from monthly_susds
where dt>= timestamp'2025-07-01 00:00'
order by dt desc, ref_code asc
