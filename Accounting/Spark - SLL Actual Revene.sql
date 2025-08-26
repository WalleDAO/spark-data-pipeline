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
        from (
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                alm_idle as amount
            from dune.sparkdotfi.result_spark_idle_dai_usds_in_sparklend_by_alm_proxy -- Spark - Idle DAI & USDS in Sparklend by ALM Proxy
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                amount
            from dune.sparkdotfi.result_spark_usds_s_usds_usdc_in_psm_3_curve_psm_3_proxy_foundation_aave -- Spark - USDS, sUSDS & USDC in PSM3, Curve, Maple, PSM3, Proxy, Treasury, Foundation
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                amount
            from dune.sparkdotfi.result_spark_pt_usds_14_aug_2025_in_sp_dai_vault_on_morpho -- Spark - PT-USDS-14AUG2025 in spDAI vault on Morpho
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                alm_idle as amount
            from dune.sparkdotfi.result_spark_idle_usds_in_aave_by_alm_proxy-- Spark - Idle USDS in AAVE by ALM Proxy
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                alm_idle as amount
            from dune.sparkdotfi.result_spark_idle_dai_usdc_in_morpho_by_alm_proxy -- Spark - Idle DAI & USDC in Morpho by ALM Proxy
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                amount
            from dune.sparkdotfi.result_spark_pendle_farms -- Spark - Pendle Pools
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                amount
            from dune.sparkdotfi.result_spark_maple_syrup_usdc_by_alm_proxy -- Spark - Maple syrupUSDC by ALM Proxy
            union all
            select dt,
                blockchain,
                protocol_name,
                token_symbol,
                reward_code,
                reward_per,
                interest_code,
                interest_per,
                amount
            from dune.sparkdotfi.result_spark_ethena_payout_apy -- Spark - Ethena Payout
        ) p
        left join dune.sparkdotfi.result_spark_idle_dai_usds_in_sparklend_by_alm_proxy sl using (dt, protocol_name, token_symbol) -- Spark - Idle DAI & USDS in Sparklend by ALM Proxy
        left join dune.sparkdotfi.result_spark_idle_dai_usdc_in_morpho_by_alm_proxy m using (dt, protocol_name, token_symbol)  -- Spark - Idle DAI & USDC in Morpho by ALM Proxy
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
            sum(p.reward_per*p.amount)/sum(p.amount) as reward_per,
            p.interest_code,
            sum(p.interest_per*p.amount)/sum(p.amount)as interest_per_aprox, 
            sum(p.amount) / 365 as tw_amount, 
            sum((p.amount / 365) * p.reward_per) as tw_rebate,
            sum(
                case
                    when protocol_name = 'Sparklend' then sl.asset_rev_amount / 365
                    when protocol_name = 'Morpho' and token_symbol in ('DAI', 'USDC') then m.net_rev_interest / 365
                    when protocol_name = 'ethena' then e.daily_actual_revenue-e.daily_BR_cost
                    else (p.amount / 365) * p.reward_per + (p.amount / 365) * p.interest_per
                end
            ) as tw_net_rev_interest,
            max_by(p.tw_nri_7d_avg, dt) * 365 as tw_nri_proj_1y  --> only for projection
        from daily_7d_trailing_avg p
        left join dune.sparkdotfi.result_spark_idle_dai_usds_in_sparklend_by_alm_proxy sl using (dt, protocol_name, token_symbol) -- Spark - Idle DAI & USDS in Sparklend by ALM Proxy
        left join dune.sparkdotfi.result_spark_idle_dai_usdc_in_morpho_by_alm_proxy m using (dt, protocol_name, token_symbol)  -- Spark - Idle DAI & USDC in Morpho by ALM Proxy
        left join dune.sparkdotfi.result_spark_ethena_payout_apy e using (dt, protocol_name, token_symbol) -- Spark - Ethena Payout + APY
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

select * from protocols_monthly_usd
where dt>= timestamp'2025-07-01 00:00'
order by dt desc, "protocol-token" asc, reward_code asc