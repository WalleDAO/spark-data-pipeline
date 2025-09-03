with tokens as (
    select *
    from query_5531539 -- Spark - Rewards - Tokens
),
-- time sequence with the next rolling 12 months
seq as (
    select t.dt
    from unnest(
            sequence(
                date_trunc('month', current_date + interval '1' month),
                current_date + interval '12' month,
                interval '1' month
            )
        ) as t(dt)
),
monthly_raw as (
    select date_trunc('month', b.dt) as dt,
        b.blockchain,
        b.token,
        b.ref_code,
        b.integrator_name,
        b.referral_type,
        b.reward_code,
        b.reward_per,
        max_by(b.tw_reward_7d_avg, dt) * 365 as tw_reward_proj_1y
    from query_5615313 b
    group by 1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
),
users_monthly as (
    select dt,
        reward_code,
        reward_per,
        case
            when token in ('USDS-SPK', 'USDS-SKY') then 'stakedUSDS'
            else token
        end as token_symbol,
        sum(tw_reward_proj_1y) / 12 as tw_reward_proj_1y_monthly,
        sum(tw_reward_proj_1y) as tw_reward_proj_1y
    from monthly_raw
    where dt = date_trunc('month', current_date)
        and referral_type = 'Spark referrals'
    group by 1,
        2,
        3,
        4
    union all
    select dt,
        reward_code,
        reward_per,
        'USDS' as token_symbol,
        tw_reward_proj_1y / 12 as tw_reward_proj_1y_monthly,
        tw_reward_proj_1y
    from query_5531933 -- Spark - USDS in Treasury
    where dt = date_trunc('month', current_date)
),
protocols_monthly_projection as (
    select s.dt,
        p.token_symbol,
        p.reward_code,
        p.reward_per,
        r.reward_per as adjusted_reward_per,
        (r.reward_per / p.reward_per) as adjustment,
        --sum(p.tw_reward_proj_1y_monthly) over (partition by p.token_symbol order by s.dt) as tw_reward_proj
        sum(
            p.tw_reward_proj_1y_monthly * (r.reward_per / p.reward_per)
        ) over (
            partition by p.token_symbol
            order by s.dt
        ) as tw_reward_proj
    from seq s
        cross join users_monthly p
        join query_5353955 r -- Spark - Rewards - Rates
        on p.reward_code = r.reward_code
        and s.dt between r.start_dt and r.end_dt
),
protocols_monthly_projection_usd as (
    select b.dt,
        b.token_symbol,
        b.reward_code,
        b.reward_per,
        b.adjusted_reward_per,
        b.adjustment,
        p.price_usd,
        b.tw_reward_proj,
        b.tw_reward_proj * p.price_usd as tw_reward_proj_usd
    from protocols_monthly_projection b
        join tokens t on b.token_symbol = t.token_symbol
        left join dune.steakhouse.result_token_price p on t.blockchain = p.blockchain
        and t.token_address = p.token_address
        and p.dt = current_date - interval '1' day -- price as of yesterday
)
select *
from protocols_monthly_projection_usd
order by 1 asc,
    2 desc