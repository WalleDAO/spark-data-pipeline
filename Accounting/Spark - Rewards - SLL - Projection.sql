-- header
-- @dev: USD value for projection uses yesterday's price

with
    tokens as (
        select * from query_5531539 -- Spark - Rewards - Tokens
    ),
    -- time sequence with the next rolling 12 months
    seq as (
        select t.dt
        from unnest(sequence(date_trunc('month', current_date + interval '1' month), current_date + interval '12' month, interval '1' month)) as t(dt)
    ),
    protocols_monthly as (
        select
            dt,
            "protocol-token",
            token_symbol,
            reward_code,
            reward_per,
            interest_code,
            interest_per_aprox as interest_per,
            tw_nri_proj_1y / 12 as tw_nri_proj_monthly,
            tw_nri_proj_1y
        from query_5441979
        where dt = date_trunc('month', current_date)
    ),
    protocols_monthly_projection as (
        select
            s.dt,
            p."protocol-token",
            p.token_symbol,
            p.reward_code,
            p.reward_per,
            p.interest_code,
            p.interest_per,
            coalesce(r.reward_per, p.reward_per) as adjusted_reward_per,
            sign(p.interest_per) * coalesce(i.reward_per, abs(p.interest_per)) as adjusted_interest_per,
            case
                when p.reward_per + p.interest_per = 0 then 0
                else (
                    (coalesce(r.reward_per, p.reward_per) + (sign(p.interest_per) * coalesce(i.reward_per, abs(p.interest_per))))
                )   / (p.reward_per + p.interest_per)
            end as adjustment_per,
            sum(p.tw_nri_proj_monthly * 
                (
                    case
                        when p.reward_per + p.interest_per = 0 then 0
                        else (
                            (coalesce(r.reward_per, p.reward_per) + (sign(p.interest_per) * coalesce(i.reward_per, abs(p.interest_per))))
                        )   / (p.reward_per + p.interest_per)
                    end
                )) over (partition by p."protocol-token" order by s.dt
            ) as tw_net_rev_interest_proj
        from seq s
        cross join protocols_monthly p
        -- @dev: if reward or interest code not found for 2026+, we take the 2025 one
        left join query_5353955 r -- Spark - Rewards - Rates : Rebates
            on p.reward_code = r.reward_code
            and s.dt between r.start_dt and r.end_dt
        left join query_5353955 i -- Spark - Rewards - Rates : Interest
            on p.interest_code = i.reward_code
            and s.dt between i.start_dt and i.end_dt
    ),
    protocols_monthly_projection_usd as (
        select
            b.dt,
            b."protocol-token",
            b.token_symbol,
            b.reward_code,
            b.reward_per,
            b.interest_code,
            b.interest_per,
            b.adjusted_reward_per,
            b.adjusted_interest_per,
            b.adjustment_per,
            p.price_usd,
            b.tw_net_rev_interest_proj,
            b.tw_net_rev_interest_proj * p.price_usd as tw_net_rev_interest_proj_usd
        from protocols_monthly_projection b
        join tokens t
            on b.token_symbol = t.token_symbol
        left join dune.steakhouse.result_token_price p
            on t.blockchain = p.blockchain
            and t.token_address = p.token_address
            and p.dt = current_date - interval '1' day -- price as of yesterday
    )

select * from protocols_monthly_projection_usd order by 1 asc, 2 asc, 3 asc