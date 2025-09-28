WITH curve_pools AS (
    SELECT
        'ethereum' as blockchain,
        'Curve' as protocol_name,
        'sUSDS-USDT' as pool_name,
        'USDT' as token_symbol,
        0x00836Fe54625BE242BcFA286207795405ca4fD10 as lp_token_addr,
        0x1601843c5E9bC251A3272907010AFa41Fa18347E as spark_addr,
        0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD as token1_addr,
        0xdAC17F958D2ee523a2206206994597C13D831ec7 as token2_addr,
        'sUSDS' as token1_symbol,
        'USDT' as token2_symbol,
        18 as lp_decimals,
        18 as token1_decimals,
        6 as token2_decimals,
        DATE '2025-04-07' as start_date,
        DATE '2025-04-24' as apr_start_date,
        'BR' as gross_yield_code,
        'BR' as borrow_cost_code,
        true as has_conversion_rate
    UNION ALL
    SELECT
        'ethereum' as blockchain,
        'Curve' as protocol_name,
        'USDS-PYUSD' as pool_name,
        'PYUSD' as token_symbol,
        0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f as lp_token_addr,
        0x1601843c5E9bC251A3272907010AFa41Fa18347E as spark_addr,
        0xdc035d45d973e3ec169d2276ddab16f1e407384f as token1_addr,
        0x6c3ea9036406852006290770bedfcaba0e23a0e8 as token2_addr,
        'USDS' as token1_symbol,
        'PYUSD' as token2_symbol,
        18 as lp_decimals,
        18 as token1_decimals,
        6 as token2_decimals,
        DATE '2025-09-06' as start_date,
        DATE '2025-09-24' as apr_start_date,
        'BR' as gross_yield_code,
        'BR' as borrow_cost_code,
        false as has_conversion_rate
),
curve_daily_changes AS (
    SELECT
        cp.pool_name,
        DATE(evt_block_date) AS dt,
        'lp_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cp.spark_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, cp.lp_decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, cp.lp_decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    JOIN curve_pools cp
        ON t.contract_address = cp.lp_token_addr
    WHERE
        (
            t."to" = cp.spark_addr
            OR t."from" = cp.spark_addr
        )
        AND DATE(evt_block_date) >= cp.start_date
    GROUP BY cp.pool_name, DATE(evt_block_date)
    UNION ALL
    SELECT
        cp.pool_name,
        DATE(evt_block_date) AS dt,
        'supply_change' as change_type,
        SUM(
            CASE
                WHEN "to" = 0x0000000000000000000000000000000000000000
                    THEN - CAST(value AS DOUBLE) / POWER(10, cp.lp_decimals)
                WHEN "from" = 0x0000000000000000000000000000000000000000
                    THEN CAST(value AS DOUBLE) / POWER(10, cp.lp_decimals)
                ELSE 0
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    JOIN curve_pools cp
        ON t.contract_address = cp.lp_token_addr
    WHERE
        (
            "to" = 0x0000000000000000000000000000000000000000
            OR "from" = 0x0000000000000000000000000000000000000000
        )
        AND DATE(evt_block_date) >= cp.start_date
    GROUP BY cp.pool_name, DATE(evt_block_date)
    UNION ALL
    SELECT
        cp.pool_name,
        DATE(evt_block_date) AS dt,
        'token1_pool_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cp.lp_token_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, cp.token1_decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, cp.token1_decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    JOIN curve_pools cp
        ON t.contract_address = cp.token1_addr
    WHERE
        (
            t."to" = cp.lp_token_addr
            OR t."from" = cp.lp_token_addr
        )
        AND DATE(evt_block_date) >= cp.start_date
    GROUP BY cp.pool_name, DATE(evt_block_date)
    UNION ALL
    SELECT
        cp.pool_name,
        DATE(evt_block_date) AS dt,
        'token2_pool_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cp.lp_token_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, cp.token2_decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, cp.token2_decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    JOIN curve_pools cp
        ON t.contract_address = cp.token2_addr
    WHERE
        (
            t."to" = cp.lp_token_addr
            OR t."from" = cp.lp_token_addr
        )
        AND DATE(evt_block_date) >= cp.start_date
    GROUP BY cp.pool_name, DATE(evt_block_date)
),
curve_seq AS (
    SELECT
        cp.pool_name,
        dt
    FROM curve_pools cp
    CROSS JOIN UNNEST(
            SEQUENCE(cp.start_date, CURRENT_DATE, INTERVAL '1' DAY)
        ) AS t(dt)
),
curve_cumulative AS (
    SELECT
        s.pool_name,
        s.dt,
        SUM(COALESCE(cdc_lp.amount, 0)) OVER (
            PARTITION BY s.pool_name
            ORDER BY s.dt
        ) as cum_lp_balance,
        SUM(COALESCE(cdc_supply.amount, 0)) OVER (
            PARTITION BY s.pool_name
            ORDER BY s.dt
        ) as cum_total_supply,
        SUM(COALESCE(cdc_token1.amount, 0)) OVER (
            PARTITION BY s.pool_name
            ORDER BY s.dt
        ) as cum_token1_pool,
        SUM(COALESCE(cdc_token2.amount, 0)) OVER (
            PARTITION BY s.pool_name
            ORDER BY s.dt
        ) as cum_token2_pool
    FROM curve_seq s
    LEFT JOIN curve_daily_changes cdc_lp
        ON cdc_lp.pool_name = s.pool_name
        AND cdc_lp.dt = s.dt
        AND cdc_lp.change_type = 'lp_change'
    LEFT JOIN curve_daily_changes cdc_supply
        ON cdc_supply.pool_name = s.pool_name
        AND cdc_supply.dt = s.dt
        AND cdc_supply.change_type = 'supply_change'
    LEFT JOIN curve_daily_changes cdc_token1
        ON cdc_token1.pool_name = s.pool_name
        AND cdc_token1.dt = s.dt
        AND cdc_token1.change_type = 'token1_pool_change'
    LEFT JOIN curve_daily_changes cdc_token2
        ON cdc_token2.pool_name = s.pool_name
        AND cdc_token2.dt = s.dt
        AND cdc_token2.change_type = 'token2_pool_change'
),
main_data as (
    SELECT
        c.dt,
        cp.blockchain,
        cp.protocol_name,
        cp.pool_name,
        cp.token_symbol,
        c.cum_lp_balance as spark_shares,
        c.cum_total_supply as pool_total_shares,
        CASE
            WHEN cp.has_conversion_rate
                THEN c.cum_token1_pool * COALESCE(q.susds_conversion_rate, 1.0)
            ELSE c.cum_token1_pool
        END as token1_pool_usds_equivalent_balance,
        c.cum_token2_pool as token2_pool_balance,
        CASE
            WHEN cp.has_conversion_rate
                THEN c.cum_token1_pool * COALESCE(q.susds_conversion_rate, 1.0) + c.cum_token2_pool
            ELSE c.cum_token1_pool + c.cum_token2_pool
        END as pool_total_assets_balance,
        CASE
            WHEN cp.has_conversion_rate
                THEN (
                c.cum_token1_pool * COALESCE(q.susds_conversion_rate, 1.0) + c.cum_token2_pool
            ) * 1.0 / NULLIF(c.cum_total_supply, 0)
            ELSE (c.cum_token1_pool + c.cum_token2_pool) * 1.0 / NULLIF(c.cum_total_supply, 0)
        END as index_value,
        CASE
            WHEN cp.has_conversion_rate
                THEN c.cum_lp_balance * 1.0 / NULLIF(c.cum_total_supply, 0) * (
                c.cum_token1_pool * COALESCE(q.susds_conversion_rate, 1.0) + c.cum_token2_pool
            )
            ELSE c.cum_lp_balance * 1.0 / NULLIF(c.cum_total_supply, 0) * (c.cum_token1_pool + c.cum_token2_pool)
        END as SLL_total_assets_balance,
        c.cum_lp_balance * 1.0 / NULLIF(c.cum_total_supply, 0) * c.cum_token2_pool as SLL_allocated_assets_balance,
        cp.apr_start_date
    FROM curve_cumulative c
    JOIN curve_pools cp
        ON c.pool_name = cp.pool_name
    LEFT JOIN query_5752873 q
        ON c.dt = q.dt
)
SELECT
    dt,
    blockchain,
    protocol_name,
    pool_name,
    token_symbol,
    spark_shares,
    pool_total_shares,
    token1_pool_usds_equivalent_balance,
    token2_pool_balance,
    pool_total_assets_balance,
    index_value,
    CASE
        WHEN dt >= apr_start_date
            THEN (
            index_value / LAG(index_value, 1) OVER (
                PARTITION BY pool_name
                ORDER BY dt ASC
            ) - 1
        ) * 365
        ELSE 0
    END as supply_rate_apr_pool,
    if(
        COALESCE(SLL_allocated_assets_balance, 0) <= 0,
        0,
        CASE
            WHEN dt >= apr_start_date
                THEN (
                index_value / LAG(index_value, 1) OVER (
                    PARTITION BY pool_name
                    ORDER BY dt ASC
                ) - 1
            ) * 365
            ELSE 0
        END * COALESCE(SLL_total_assets_balance, 0) / COALESCE(SLL_allocated_assets_balance, 0)
    ) as supply_rate_apr_target_token,
    case when token_symbol='PYUSD' then 365 * (exp(ln(1 + 0.045) / 365) - 1) else 0 end as paypal_yield_rate,
    r.reward_code as rebate_yield_code,
    r.reward_per as rebate_yield_apr,
    i.reward_code as borrow_cost_code,
    i.reward_per as borrow_cost_apr,
    COALESCE(SLL_total_assets_balance, 0) as SLL_total_assets_balance,
    COALESCE(SLL_allocated_assets_balance, 0) as SLL_allocated_assets_balance,
    COALESCE(SLL_total_assets_balance, 0) - COALESCE(SLL_allocated_assets_balance, 0) as alm_idle
FROM main_data
CROSS JOIN query_5353955 i
CROSS JOIN query_5353955 r
WHERE
    i.reward_code = 'BR'
    AND dt BETWEEN i.start_dt
    AND i.end_dt
    AND r.reward_code = 'AR'
    AND dt BETWEEN r.start_dt
    AND r.end_dt
ORDER BY dt DESC,
    pool_name