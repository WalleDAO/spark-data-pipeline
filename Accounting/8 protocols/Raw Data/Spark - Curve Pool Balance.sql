with -- Configuration tables
curve_config (
    blockchain,
    protocol_name,
    lp_token_addr,
    spark_addr,
    susds_addr,
    usdt_addr,
    start_date,
    gross_yield_code,
    borrow_cost_code
) as (
    values
        (
            'ethereum',
            'Curve',
            0x00836Fe54625BE242BcFA286207795405ca4fD10,
            -- LP token
            0x1601843c5E9bC251A3272907010AFa41Fa18347E,
            -- Spark address
            0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD,
            -- sUSDS
            0xdAC17F958D2ee523a2206206994597C13D831ec7,
            -- USDT
            date '2025-04-07',
            'BR',
            'BR'
        )
),
token_decimals (token_addr, decimals) as (
    values
        (0x00836Fe54625BE242BcFA286207795405ca4fD10, 18),
        -- LP token
        (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, 18),
        -- sUSDS
        (0xdAC17F958D2ee523a2206206994597C13D831ec7, 6) -- USDT
),
curve_daily_changes AS (
    -- LP token changes
    SELECT
        DATE(evt_block_date) AS dt,
        'lp_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cc.spark_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, td.decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, td.decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    CROSS JOIN curve_config cc
    JOIN token_decimals td
        ON t.contract_address = td.token_addr
    WHERE
        t.contract_address = cc.lp_token_addr
        AND (
            t."to" = cc.spark_addr
            OR t."from" = cc.spark_addr
        )
        AND DATE(evt_block_date) >= cc.start_date
    GROUP BY DATE(evt_block_date)
    UNION ALL
    -- LP supply changes
    SELECT
        DATE(evt_block_date) AS dt,
        'supply_change' as change_type,
        SUM(
            CASE
                WHEN "to" = 0x0000000000000000000000000000000000000000
                    THEN - CAST(value AS DOUBLE) / POWER(10, td.decimals) -- burn
                WHEN "from" = 0x0000000000000000000000000000000000000000
                    THEN CAST(value AS DOUBLE) / POWER(10, td.decimals) -- mint
                ELSE 0
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    CROSS JOIN curve_config cc
    JOIN token_decimals td
        ON t.contract_address = td.token_addr
    WHERE
        t.contract_address = cc.lp_token_addr
        AND (
            "to" = 0x0000000000000000000000000000000000000000
            OR "from" = 0x0000000000000000000000000000000000000000
        )
        AND DATE(evt_block_date) >= cc.start_date
    GROUP BY DATE(evt_block_date)
    UNION ALL
    -- sUSDS pool changes
    SELECT
        DATE(evt_block_date) AS dt,
        'susds_pool_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cc.lp_token_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, td.decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, td.decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    CROSS JOIN curve_config cc
    JOIN token_decimals td
        ON t.contract_address = td.token_addr
    WHERE
        t.contract_address = cc.susds_addr
        AND (
            t."to" = cc.lp_token_addr
            OR t."from" = cc.lp_token_addr
        )
        AND DATE(evt_block_date) >= cc.start_date
    GROUP BY DATE(evt_block_date)
    UNION ALL
    -- USDT pool changes
    SELECT
        DATE(evt_block_date) AS dt,
        'usdt_pool_change' as change_type,
        SUM(
            CASE
                WHEN t."to" = cc.lp_token_addr
                    THEN CAST(value AS DOUBLE) / POWER(10, td.decimals)
                ELSE - CAST(value AS DOUBLE) / POWER(10, td.decimals)
            END
        ) AS amount
    FROM erc20_ethereum.evt_Transfer t
    CROSS JOIN curve_config cc
    JOIN token_decimals td
        ON t.contract_address = td.token_addr
    WHERE
        t.contract_address = cc.usdt_addr
        AND (
            t."to" = cc.lp_token_addr
            OR t."from" = cc.lp_token_addr
        )
        AND DATE(evt_block_date) >= cc.start_date
    GROUP BY DATE(evt_block_date)
),
-- Generate daily sequence for Curve
curve_seq AS (
    SELECT
        dt
    FROM UNNEST(
            SEQUENCE(
                (
                    SELECT
                        start_date
                    FROM curve_config
                    LIMIT 1
                ), CURRENT_DATE, INTERVAL '1' DAY
            )
        ) AS t(dt)
),
-- Calculate cumulative values
curve_cumulative AS (
    SELECT
        s.dt,
        SUM(COALESCE(cdc_lp.amount, 0)) OVER (
            ORDER BY s.dt
        ) as cum_lp_balance,
        SUM(COALESCE(cdc_supply.amount, 0)) OVER (
            ORDER BY s.dt
        ) as cum_total_supply,
        SUM(COALESCE(cdc_susds.amount, 0)) OVER (
            ORDER BY s.dt
        ) as cum_susds_pool,
        SUM(COALESCE(cdc_usdt.amount, 0)) OVER (
            ORDER BY s.dt
        ) as cum_usdt_pool
    FROM curve_seq s
    LEFT JOIN curve_daily_changes cdc_lp
        ON cdc_lp.dt = s.dt
        AND cdc_lp.change_type = 'lp_change'
    LEFT JOIN curve_daily_changes cdc_supply
        ON cdc_supply.dt = s.dt
        AND cdc_supply.change_type = 'supply_change'
    LEFT JOIN curve_daily_changes cdc_susds
        ON cdc_susds.dt = s.dt
        AND cdc_susds.change_type = 'susds_pool_change'
    LEFT JOIN curve_daily_changes cdc_usdt
        ON cdc_usdt.dt = s.dt
        AND cdc_usdt.change_type = 'usdt_pool_change'
),
-- Calculate final Curve balances
curve_balances AS (
    SELECT
        cc.dt,
        cfg.blockchain,
        cfg.protocol_name,
        'sUSDS' as token_symbol,
        cfg.gross_yield_code,
        cfg.borrow_cost_code,
        CASE
            WHEN cc.cum_total_supply > 0
                THEN (cc.cum_lp_balance / cc.cum_total_supply) * cc.cum_susds_pool
            ELSE 0
        END as amount
    FROM curve_cumulative cc
    CROSS JOIN curve_config cfg
    WHERE
        cc.cum_total_supply > 0
    UNION ALL
    SELECT
        cc.dt,
        cfg.blockchain,
        cfg.protocol_name,
        'USDT' as token_symbol,
        cfg.gross_yield_code,
        cfg.borrow_cost_code,
        CASE
            WHEN cc.cum_total_supply > 0
                THEN (cc.cum_lp_balance / cc.cum_total_supply) * cc.cum_usdt_pool
            ELSE 0
        END as amount
    FROM curve_cumulative cc
    CROSS JOIN curve_config cfg
    WHERE
        cc.cum_total_supply > 0
),
all_balances AS (
    SELECT
        dt,
        blockchain,
        gross_yield_code,
        borrow_cost_code,
        protocol_name,
        token_symbol,
        IF(amount > 1e-6, amount, 0) as amount
    FROM curve_balances
),
protocol_rates as (
    SELECT
        b.dt,
        b.blockchain,
        cb.protocol_name,
        b.token_symbol,
        b.gross_yield_code,
        r.reward_per as gross_yield_apr,
        b.borrow_cost_code,
        i.reward_per as borrow_cost_apr,
        b.amount
    FROM all_balances b
    CROSS JOIN curve_config cb
    LEFT JOIN query_5353955 r
        ON b.gross_yield_code = r.reward_code
        AND b.dt BETWEEN r.start_dt
        AND r.end_dt
    LEFT JOIN query_5353955 i
        ON b.borrow_cost_code = i.reward_code
        AND b.dt BETWEEN i.start_dt
        AND i.end_dt
)
SELECT
    *
FROM protocol_rates
ORDER BY dt DESC,
    protocol_name ASC,
    blockchain ASC,
    token_symbol ASC