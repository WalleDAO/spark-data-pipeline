WITH seq AS (
    SELECT
        dt
    FROM unnest(
            sequence(
                date '2025-10-20',
                current_date,
                interval '1' day
            )
        ) AS t(dt)
),
price_seq AS (
    SELECT
        dt
    FROM unnest(
            sequence(
                date '2024-07-12',
                current_date,
                interval '1' day
            )
        ) AS t(dt)
),
daily_prices AS (
    SELECT
        ps.dt,
        nav.net_asset_value as raw_price,
        COALESCE(
            nav.net_asset_value,
            LAG(nav.net_asset_value) IGNORE NULLS OVER (ORDER BY ps.dt)
        ) AS uscc_price
    FROM price_seq ps
    LEFT JOIN query_5582308 nav 
        ON ps.dt = DATE(nav.net_asset_value_date)
),
uscc_transfers AS (
    SELECT
        evt_block_time,
        evt_tx_hash,
        CASE
            WHEN "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN value / 1e6
            WHEN "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN - value / 1e6
            ELSE 0
        END AS uscc_flow,
        CASE
            WHEN "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN 'deposit'
            WHEN "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN 'withdrawal'
        END AS flow_type
    FROM erc20_ethereum.evt_Transfer
    WHERE
        contract_address = 0x14d60E7FDC0D71d8611742720E4C50E7a974020c
        AND (
            "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
            OR "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
        )
        AND evt_block_time >= TIMESTAMP '2025-10-20'
),
usdc_transfers AS (
    SELECT
        evt_block_time,
        evt_tx_hash,
        CASE
            WHEN "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
            AND "to" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
                THEN value / 1e6
            WHEN "from" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
            AND "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN - value / 1e6
            ELSE 0
        END AS usdc_flow,
        CASE
            WHEN "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
            AND "to" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
                THEN 'sent_to_target'
            WHEN "from" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
            AND "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                THEN 'received_from_target'
        END AS usdc_flow_type
    FROM erc20_ethereum.evt_Transfer
    WHERE
        contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
        AND (
            (
                "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                AND "to" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
            )
            OR (
                "from" = 0xDB48AC0802F9A79145821A5430349cAff6d676f7
                AND "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
            )
        )
        AND evt_block_time >= TIMESTAMP '2025-10-20'
),
daily_flows AS (
    SELECT
        DATE(evt_block_time) AS date,
        SUM(uscc_flow) AS daily_uscc_flow
    FROM uscc_transfers
    GROUP BY DATE(evt_block_time)
),
daily_usdc_flows AS (
    SELECT
        DATE(evt_block_time) AS date,
        SUM(usdc_flow) AS daily_usdc_flow
    FROM usdc_transfers
    GROUP BY DATE(evt_block_time)
),
daily_holdings AS (
    SELECT
        s.dt AS date,
        SUM(COALESCE(df.daily_uscc_flow, 0)) OVER (
            ORDER BY s.dt ROWS UNBOUNDED PRECEDING
        ) AS cumulative_uscc_balance,
        SUM(COALESCE(duf.daily_usdc_flow, 0)) OVER (
            ORDER BY s.dt ROWS UNBOUNDED PRECEDING
        ) AS cumulative_usdc_sent
    FROM seq s
    LEFT JOIN daily_flows df
        ON s.dt = df.date
    LEFT JOIN daily_usdc_flows duf
        ON s.dt = duf.date
),
final_data AS (
    SELECT
        dh.date AS dt,
        dh.cumulative_uscc_balance AS uscc_holdings,
        dh.cumulative_usdc_sent AS usdc_deployed,
        dp.uscc_price,
        dp_prev.uscc_price AS uscc_price_prev,
        dp.uscc_price - dp_prev.uscc_price AS price_diff,
        dh.cumulative_uscc_balance * (dp.uscc_price - dp_prev.uscc_price) AS gross_yield_usd,
        CASE
            WHEN dh.cumulative_uscc_balance * dp.uscc_price <= 50000000 
                THEN dh.cumulative_uscc_balance * dp.uscc_price * 0.0075 / 365
            WHEN dh.cumulative_uscc_balance * dp.uscc_price <= 100000000 
                THEN (50000000 * 0.0075 + (dh.cumulative_uscc_balance * dp.uscc_price - 50000000) * 0.005) / 365
            ELSE 
                (50000000 * 0.0075 + 50000000 * 0.005 + (dh.cumulative_uscc_balance * dp.uscc_price - 100000000) * 0.0025) / 365
        END AS management_fee,
        dh.cumulative_usdc_sent * i.reward_per / 365 AS borrow_cost_usd,
        i.reward_code as borrow_cost_code,
        i.reward_per as borrow_cost_apr
    FROM daily_holdings dh
    LEFT JOIN daily_prices dp
        ON dh.date = dp.dt
    LEFT JOIN daily_prices dp_prev
        ON dh.date - interval '1' day = dp_prev.dt
    CROSS JOIN query_5353955 i
    WHERE
        i.reward_code = 'BR'
        AND dh.date BETWEEN i.start_dt AND i.end_dt
)
SELECT
    dt,
    'ethereum' as blockchain,
    'Superstate' as protocol_name,
    'USDC' as token_symbol,
    'Superstate - USDC' as "protocol-token",
    uscc_holdings,
    uscc_price,
    uscc_holdings * uscc_price as uscc_holdings_usd,
    uscc_price_prev,
    price_diff,
    price_diff / uscc_price_prev * 365 as supply_rate_apr,
    CASE
        WHEN uscc_holdings * uscc_price <= 50000000 
            THEN 0.0075
        WHEN uscc_holdings * uscc_price <= 100000000 
            THEN (50000000 * 0.0075 + (uscc_holdings * uscc_price - 50000000) * 0.005) / (uscc_holdings * uscc_price)
        ELSE 
            (50000000 * 0.0075 + 50000000 * 0.005 + (uscc_holdings * uscc_price - 100000000) * 0.0025) / (uscc_holdings * uscc_price)
    END as management_fee_apr,
    usdc_deployed,
    borrow_cost_code,
    borrow_cost_apr,
     1 as price_usd,
     'uscc_holdings * (uscc_price - uscc_price_prev)' as gross_yield_formula,
    gross_yield_usd,
    'uscc_holdings_usd * (0.75% first $50M + 0.5% next $50M + 0.25% over $100M)' as management_fee_formula,
    management_fee,
    'usdc_deployed * base_rate_apr' as borrow_cost_formula,
    borrow_cost_usd,
    gross_yield_usd - management_fee - borrow_cost_usd as revenue_usd
FROM final_data
ORDER BY dt DESC;