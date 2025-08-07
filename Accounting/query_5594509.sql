WITH 
date_constants AS (
    SELECT
        TIMESTAMP '2025-07-29 14:00:00' AS date_1,
        TIMESTAMP '2025-08-11 14:00:00' AS date_2
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
        AND evt_block_time >= dc.date_1 
        AND evt_block_time <= dc.date_2
        
        UNION ALL
        
        SELECT evt_block_time, evt_index, assets, shares 
        FROM sky_ethereum.susds_evt_withdraw 
        CROSS JOIN date_constants dc
        WHERE shares > 0
        AND evt_block_time >= dc.date_1 
        AND evt_block_time <= dc.date_2
    )
),

base_data AS (
    SELECT 
        b.user_address,
        b.evt_block_time,
        b.evt_block_date,
        b.evt_tx_hash,
        b.evt_index,
        b.blockchain,
        b.contract_address,
        b.amount_change,
        b.cumulative_total_balance
    FROM query_5577578 b
),

base_with_rates AS (
    SELECT 
        bd.*,
        r.susds_conversion_rate,
        ROW_NUMBER() OVER (
            PARTITION BY bd.user_address, bd.evt_block_time, bd.evt_index, bd.evt_tx_hash
            ORDER BY r.evt_block_time DESC, r.evt_index DESC
        ) as rn
    FROM base_data bd
    LEFT JOIN susds_realtime_rates r 
       ON (r.evt_block_time < bd.evt_block_time 
           OR (r.evt_block_time = bd.evt_block_time AND r.evt_index < bd.evt_index))
)

SELECT 
    user_address,
    evt_block_time,
    evt_block_date,
    evt_tx_hash,
    evt_index,
    blockchain,
    contract_address,
    amount_change,
    cumulative_total_balance,
    susds_conversion_rate as susds_conversion_rate_at_transaction
FROM base_with_rates
WHERE rn = 1
ORDER BY user_address, evt_block_time, evt_index, evt_tx_hash;
