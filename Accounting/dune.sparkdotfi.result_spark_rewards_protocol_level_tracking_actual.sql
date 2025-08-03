-- header

with
    tokens as (
        select * from query_5531539 -- Spark - Rewards - Tokens
    ),
    protocols_data as (
        select
            dt,
            p.blockchain,
            protocol_name,
            token_symbol,
            p.reward_code,
            p.reward_per,
            p.interest_code,
            p.interest_per,
            p.amount,
            case
                when protocol_name = 'Sparklend'
                    then sl.asset_rev_amount / 365
                when protocol_name = 'Morpho' and token_symbol in ('DAI','USDC')
                     then m.net_rev_interest / 365
                else (amount / 365) * p.reward_per + (p.amount / 365) * p.interest_per
            end as tw_net_rev_interest_day --> only for projection
            --amount_usd
        from (
            select * from dune.sparkdotfi.result_spark_rewards_protocol_level_tracking_1
            union all
            select * from dune.sparkdotfi.result_spark_rewards_protocol_level_tracking_2
        ) p
        left join query_5379492 sl using (dt, protocol_name, token_symbol) -- Spark - Idle DAI & USDS in Sparklend by ALM Proxy
        left join query_5411038 m using (dt, protocol_name, token_symbol)  -- Spark - Idle DAI & USDC in Morpho by ALM Proxy
        where amount is not null -- in case any underlying query is not refreshed (eg: morpho matviews)
          and dt >= date '2024-11-01'
    ),
    -- get the 7d trailing average net revenue interest
    daily_7d_trailing_avg as (
        select
            *,
            avg(tw_net_rev_interest_day) over (
                partition by protocol_name, token_symbol order by dt rows between 6 preceding and current row
            ) as tw_nri_7d_avg --> only for projection
        from protocols_data
    ),
    protocols_monthly as (
        select
            date_trunc('month', dt) as dt,
            token_symbol,
            concat(protocol_name, ' - ', token_symbol) as "protocol-token",
            p.reward_code,
            p.reward_per,
            p.interest_code,
            avg(p.interest_per) as interest_per_aprox, -- for APYs that change every day, it's an approximation
            sum(p.amount) / 365 as tw_amount, -- @DEV: keep temporarily
            sum((p.amount / 365) * p.reward_per) as tw_rebate,
            sum(
                case
                    when protocol_name = 'Sparklend' then sl.interest_amount / 365
                    when protocol_name = 'Morpho' and token_symbol in ('DAI', 'USDC') then m.interest_amount / 365
                    when protocol_name = 'ethena' then (e.actual_amount - e.actual_amount * 0.048) / 365 -- @dev: BR might change
                    else (p.amount / 365) * p.interest_per
                end
            ) as tw_interest,
            sum(
                case
                    when protocol_name = 'Sparklend' then sl.asset_rev_amount / 365
                    when protocol_name = 'Morpho' and token_symbol in ('DAI', 'USDC') then m.net_rev_interest / 365
                    when protocol_name = 'ethena' then (e.actual_amount - e.actual_amount * 0.048) / 365 -- @dev: BR might change
                    else (p.amount / 365) * p.reward_per + (p.amount / 365) * p.interest_per
                end
            ) as tw_net_rev_interest,
            max_by(p.tw_nri_7d_avg, dt) as tw_nri_7d_avg,  --> only for projection: latest 7d trailing average of the month on net revenue interest 
            max_by(p.tw_nri_7d_avg, dt) * 365 as tw_nri_proj_1y  --> only for projection
        from daily_7d_trailing_avg p
        left join query_5379492 sl using (dt, protocol_name, token_symbol) -- Spark - Idle DAI & USDS in Sparklend by ALM Proxy
        left join query_5411038 m using (dt, protocol_name, token_symbol)  -- Spark - Idle DAI & USDC in Morpho by ALM Proxy
        left join query_5548062 e using (dt, protocol_name, token_symbol) -- Spark - Ethena Payout + APY
        group by 1,2,3,4,5,6
    ),
    protocols_monthly_usd as (
        select
            b.*,
            p.price_usd,
            b.tw_rebate * p.price_usd as tw_rebate_usd,
            b.tw_net_rev_interest * p.price_usd as tw_net_rev_interest_usd
        from protocols_monthly b
        join tokens t
            on b.token_symbol = t.token_symbol
        left join dune.steakhouse.result_token_price p
            on t.blockchain = p.blockchain
            and t.token_address = p.token_address
            and b.dt = p.dt 
    )

select * from protocols_monthly_usd order by dt desc, "protocol-token" asc, reward_code asc