----https://dune.com/queries/5353955
------ forked from https://dune.com/queries/3702505
WITH
    ssr AS (
        SELECT
            date(call_block_time) AS dt,
            power((output_0 / 1e27), 365 * 24 * 60 * 60) - 1 AS ssr_rate
        FROM (
            SELECT
                call_block_time,
                output_0,
                lag(output_0) OVER (ORDER BY call_block_time) AS lag_output_0
            FROM sky_ethereum.sUSDS_call_ssr
            WHERE call_success
        )
        WHERE output_0 <> coalesce(lag_output_0, cast(0 AS uint256))
    ),
    ssr_periods AS (
        SELECT
            ssr_rate,
            dt AS start_date,
            CASE 
                WHEN lead(dt) OVER (ORDER BY dt) IS NULL THEN date '2030-12-31' 
                ELSE lead(dt) OVER (ORDER BY dt) - interval '1' day
            END AS end_date
        FROM ssr
        WHERE dt >= date '2024-09-25'
    ),
    dynamic_rewards AS (
        SELECT 
            t.reward_code,
            t.reward_description,
            CASE 
                WHEN t.reward_code = 'SR' THEN s.ssr_rate
                WHEN t.reward_code = 'AR' AND r.is_2025_special THEN s.ssr_rate + 0.006
                WHEN t.reward_code = 'AR' AND NOT r.is_2025_special THEN s.ssr_rate + 0.002
                WHEN t.reward_code = 'BR' THEN s.ssr_rate + 0.003
            END AS reward_per,
            GREATEST(s.start_date, r.start_dt) AS start_dt,
            LEAST(s.end_date, r.end_dt) AS end_dt
        FROM ssr_periods s
        CROSS JOIN (
            VALUES
                ('SR', 'Savings Rate'),
                ('AR', 'Agent Rate'),
                ('BR', 'Base Rate')
        ) AS t(reward_code, reward_description)
        CROSS JOIN (
            VALUES
                (date '2024-01-01', date '2025-12-31', true), 
                (date '2026-01-01', date '2030-12-31', false)  
        ) AS r(start_dt, end_dt, is_2025_special)
        WHERE s.start_date <= r.end_dt 
          AND s.end_date >= r.start_dt
    ),
    static_rewards AS (
        SELECT 
            reward_code, 
            reward_description, 
            reward_per, 
            start_dt, 
            end_dt
        FROM (
            VALUES
                ('XR', 'Accessibility Rewards', 0.006, date '2024-01-01', date '2025-12-31'),
                ('XR', 'Accessibility Rewards', 0.002, date '2026-01-01', date '2030-12-31'),
                ('NA', 'Not Applicable', 0, date '2024-01-01', date '2030-12-31')
        ) AS t(reward_code, reward_description, reward_per, start_dt, end_dt)
    )

SELECT * FROM dynamic_rewards
UNION ALL
SELECT * FROM static_rewards
ORDER BY start_dt DESC, reward_code;
