----https://dune.com/queries/5353955
with
    spark_rewards (reward_code, reward_description, reward_per, start_dt, end_dt) as (
        values
            ('SR', 'Savings Rate', 0.045, date '2024-01-01', date '2030-12-31'),
            ('AR', 'Agent Rate', 0.045 + 0.006, date '2024-01-01', date '2025-12-31'),
            ('AR', 'Agent Rate', 0.045 + 0.002, date '2026-01-01', date '2030-12-31'),
            ('BR', 'Base Rate', 0.045 + 0.003, date '2024-01-01', date '2030-12-31'),
            ('XR', 'Accessibility Rewards', 0.006, date '2024-01-01', date '2025-12-31'),
            ('XR', 'Accessibility Rewards', 0.002, date '2026-01-01', date '2030-12-31'),
            ('NA', 'Not Applicable', 0, date '2024-01-01', date '2030-12-31')
    )

select * from spark_rewards