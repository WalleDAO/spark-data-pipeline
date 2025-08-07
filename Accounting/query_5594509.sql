-- Overdrive eligibility: Correctly handle users with no transactions during the period
WITH 
overdrive_qualification_list AS (
    SELECT 
        user_address, 
        ignition_spk_amount, 
        balance_at_date_1 as stspk_balance_at_date_1,
        min_balance_during_period as stspk_min_balance_during_period,
        effective_min_balance as stspk_effective_min_balance
    FROM query_5576861
),

date_constants AS (
    SELECT
        TIMESTAMP '2025-07-29 14:00:00' AS date_1,
        TIMESTAMP '2025-08-11 14:00:00' AS date_2
),

savings_balances AS (
    SELECT 
        user_address, 
        evt_block_time,
        evt_index, 
        cumulative_total_balance,
        susds_conversion_rate_at_transaction
    FROM query_5594509
),

susds_realtime_rates AS (
    SELECT 
        evt_block_time,
        evt_index,
        cast(assets as double) / cast(shares as double) as susds_conversion_rate
    FROM (
        SELECT evt_block_time, evt_index, assets, shares 
        FROM sky_ethereum.susds_evt_deposit 
        CROSS JOIN date_constants dc
        WHERE shares > 0
        AND evt_block_time <= dc.date_1
        AND evt_block_time >= date_trunc('day', dc.date_1) - INTERVAL '1' DAY
        
        UNION ALL
        
        SELECT evt_block_time, evt_index, assets, shares 
        FROM sky_ethereum.susds_evt_withdraw 
        CROSS JOIN date_constants dc
        WHERE shares > 0
        AND evt_block_time <= dc.date_1
        AND evt_block_time >= date_trunc('day', dc.date_1) - INTERVAL '1' DAY
    )
),

susds_rate_at_date_1 AS (
    SELECT 
        susds_conversion_rate,
        ROW_NUMBER() OVER (ORDER BY evt_block_time DESC, evt_index DESC) as rn
    FROM susds_realtime_rates sr
    CROSS JOIN date_constants dc
    WHERE sr.evt_block_time <= dc.date_1
),

-- Get the last balance before or at DATE_1 for each user
balance_before_date_1 AS (
    SELECT 
        sb.user_address,
        sb.cumulative_total_balance,
        ROW_NUMBER() OVER (
            PARTITION BY sb.user_address 
            ORDER BY sb.evt_block_time DESC, sb.evt_index DESC
        ) as rn
    FROM savings_balances sb
    CROSS JOIN date_constants dc
    WHERE sb.evt_block_time <= dc.date_1
),

balance_at_date_1 AS (
    SELECT 
        oq.user_address,
        oq.ignition_spk_amount,
        oq.stspk_balance_at_date_1,
        oq.stspk_min_balance_during_period,
        oq.stspk_effective_min_balance,
        COALESCE(b1.cumulative_total_balance, 0) AS savings_susds_susdc_balance_at_date_1,
        sr1.susds_conversion_rate AS susds_rate_at_date_1,
        COALESCE(b1.cumulative_total_balance * sr1.susds_conversion_rate, 0) AS savings_usds_usdc_balance_at_date_1
    FROM overdrive_qualification_list oq
    LEFT JOIN balance_before_date_1 b1 ON oq.user_address = b1.user_address AND b1.rn = 1
    CROSS JOIN susds_rate_at_date_1 sr1
    WHERE sr1.rn = 1
),

-- Get minimum balance during the period (if any transactions occurred)
min_balance_during_period AS (
    SELECT 
        oq.user_address,
        MIN(sb.cumulative_total_balance) AS savings_susds_susdc_min_balance_during_period,
        MIN(sb.cumulative_total_balance * sb.susds_conversion_rate_at_transaction) AS savings_min_usds_usdc_balance_during_period
    FROM overdrive_qualification_list oq
    LEFT JOIN savings_balances sb ON oq.user_address = sb.user_address
    CROSS JOIN date_constants dc
    WHERE sb.evt_block_time > dc.date_1 
        AND sb.evt_block_time <= dc.date_2
    GROUP BY oq.user_address
),

-- Combine and determine final eligibility
user_eligibility AS (
    SELECT 
        b1.user_address,
        b1.ignition_spk_amount,
        b1.stspk_balance_at_date_1,
        b1.stspk_min_balance_during_period,
        b1.stspk_effective_min_balance,
        b1.savings_susds_susdc_balance_at_date_1,
        b1.susds_rate_at_date_1,
        b1.savings_usds_usdc_balance_at_date_1,
        mb.savings_susds_susdc_min_balance_during_period,
        mb.savings_min_usds_usdc_balance_during_period,
        COALESCE(mb.savings_susds_susdc_min_balance_during_period, b1.savings_susds_susdc_balance_at_date_1) AS savings_susds_susdc_effective_min_balance,
        COALESCE(mb.savings_min_usds_usdc_balance_during_period, b1.savings_usds_usdc_balance_at_date_1) AS savings_effective_min_usds_usdc_balance,
        CASE 
            WHEN COALESCE(mb.savings_min_usds_usdc_balance_during_period, b1.savings_usds_usdc_balance_at_date_1) >= 1000 
            THEN 'Qualified for Official Boost' 
            ELSE 'Not Qualified' 
        END AS boost_qualification_status
    FROM balance_at_date_1 b1
    LEFT JOIN min_balance_during_period mb ON b1.user_address = mb.user_address
),

-- Calculate Overdrive units and related metrics
overdrive_calculations AS (
    SELECT 
        *,
        -- Base units: 1 unit for each SPK received in Ignition (since they're already qualified)
        ignition_spk_amount AS base_units,
        
        -- Overdrive units: base units * boost multiplier
        CASE 
            WHEN boost_qualification_status = 'Qualified for Official Boost' 
            THEN ignition_spk_amount * 2  -- Double for boost
            ELSE ignition_spk_amount      -- No boost
        END AS overdrive_units,
        
        -- Max amount: 10x Ignition airdrop amount
        ignition_spk_amount * 10 AS max_amount
    FROM user_eligibility
),

-- Calculate total Overdrive units for proportional distribution
total_units AS (
    SELECT SUM(overdrive_units) AS total_overdrive_units
    FROM overdrive_calculations
)

-- Return users with all calculated fields
SELECT 
    oc.user_address,
    oc.ignition_spk_amount,
    oc.stspk_balance_at_date_1,
    oc.stspk_min_balance_during_period,
    oc.stspk_effective_min_balance,
    oc.savings_susds_susdc_balance_at_date_1,
    oc.susds_rate_at_date_1,
    oc.savings_usds_usdc_balance_at_date_1,
    oc.savings_susds_susdc_min_balance_during_period,
    oc.savings_min_usds_usdc_balance_during_period,
    oc.savings_susds_susdc_effective_min_balance,
    oc.savings_effective_min_usds_usdc_balance,
    oc.boost_qualification_status,
    oc.base_units,
    oc.overdrive_units,
    tu.total_overdrive_units AS overdrive_units_total,
    oc.max_amount,
    269838000 AS spk_unclaimed,
    LEAST(
        (oc.overdrive_units * 269838000.0) / tu.total_overdrive_units,
        oc.max_amount
    ) AS spk_user
FROM overdrive_calculations oc
CROSS JOIN total_units tu
ORDER BY oc.ignition_spk_amount DESC;
