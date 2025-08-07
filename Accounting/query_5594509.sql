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

savings_balances AS (
    SELECT 
        user_address, 
        evt_block_time,
        evt_index, 
        cumulative_total_balance
    FROM query_5577578
),

date_constants AS (
    SELECT
        TIMESTAMP '2025-07-29 14:00:00' AS date_1,
        TIMESTAMP '2025-08-11 14:00:00' AS date_2
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
        COALESCE(b1.cumulative_total_balance, 0) AS savings_balance_at_date_1
    FROM overdrive_qualification_list oq
    LEFT JOIN balance_before_date_1 b1 ON oq.user_address = b1.user_address AND b1.rn = 1
),

-- Get minimum balance during the period (if any transactions occurred)
min_balance_during_period AS (
    SELECT 
        oq.user_address,
        MIN(sb.cumulative_total_balance) AS savings_min_balance_during_period
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
        b1.savings_balance_at_date_1,
        mb.savings_min_balance_during_period,
        COALESCE(mb.savings_min_balance_during_period, b1.savings_balance_at_date_1) AS savings_effective_min_balance,
        CASE 
            WHEN COALESCE(mb.savings_min_balance_during_period, b1.savings_balance_at_date_1) >= 1000 
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
    oc.savings_balance_at_date_1,
    oc.savings_min_balance_during_period,
    oc.savings_effective_min_balance,
    oc.boost_qualification_status,
    oc.base_units,
    oc.overdrive_units,
    tu.total_overdrive_units AS overdrive_units_total,
    oc.max_amount
FROM overdrive_calculations oc
CROSS JOIN total_units tu
ORDER BY oc.ignition_spk_amount DESC;
