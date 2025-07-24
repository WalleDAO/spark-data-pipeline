-- query_5379492
-- ********************************** SPARK PROTOCOL REVENUE ANALYSIS **********************************
-- Purpose: Calculate protocol revenue from DAI/USDS lending with accessibility rewards impact
-- Key Metrics: Gross revenue (reserve factor) vs Net revenue (after borrower rebates)
-- Data Scope: Daily time series from token launch to current date for DAI/USDS on Ethereum
-- ********************************** 

-- Step 1: Extract aToken and debtToken contracts for DAI/USDS
-- Purpose: Get contract addresses and metadata for supply/borrow tracking
-- Coverage: Only DAI and USDS stablecoins on Spark Protocol
with spark_tokens as (
    SELECT 
        'ethereum' as blockchain,
        'aToken' as category,
        t.symbol,
        rc.aToken as contract_address,
        t.decimals,
        rc.evt_block_date as start_date
    FROM spark_protocol_ethereum.PoolConfigurator_evt_ReserveInitialized rc
    JOIN tokens.erc20 t ON t.contract_address = rc.asset AND t.blockchain = 'ethereum'
    WHERE t.symbol IN ('DAI', 'USDS')
    
    UNION ALL
    
    SELECT 
        'ethereum' as blockchain,
        'debtToken' as category,
        t.symbol,
        rc.variableDebtToken as contract_address,
        t.decimals,
        rc.evt_block_date as start_date
    FROM spark_protocol_ethereum.PoolConfigurator_evt_ReserveInitialized rc
    JOIN tokens.erc20 t ON t.contract_address = rc.asset AND t.blockchain = 'ethereum'
    WHERE t.symbol IN ('DAI', 'USDS')
),

-- Step 2: Get latest reserve factors (protocol's share of borrower interest)
-- Business Logic: Protocol Gross Revenue = Total Borrower Interest × Reserve Factor
-- Example: If reserve_factor = 0.10, protocol keeps 10% of borrower interest payments
sparklend_reserve_factor as (
    select 
        t.symbol as token_symbol,
        cast(rf.newReserveFactor as double) / 10000 as reserve_factor,  -- Convert basis points to decimal
        rf.evt_block_date as last_updated_date
    from (
        select 
            asset,
            newReserveFactor,
            evt_block_date,
            row_number() over (partition by asset order by evt_block_time desc) as rn
        from spark_protocol_ethereum.poolconfigurator_evt_reservefactorchanged
        where evt_block_date <= current_date
    ) rf
    inner join tokens.erc20 t 
        on rf.asset = t.contract_address and t.blockchain = 'ethereum'
    where rf.rn = 1
),

-- Step 3: Calculate standardized principal amounts from token events
-- Key Concept: Use liquidity index to separate principal from accrued interest
-- Formula: Standardized Principal = (Token Value ± Balance Increase) ÷ Liquidity Index
-- Purpose: Track normalized "base units" that can be multiplied by current index for actual balance
sp_transfers_totals as (
    select
        dt,
        symbol,
        sum(case when category = 'aToken' then amount else 0 end) as supply_amount,
        sum(case when category = 'debtToken' then amount else 0 end) as borrow_amount
    from (
        -- aToken Mint: User deposits, receives aTokens (supply side)
        -- Calculation: Extract principal by removing accrued interest and dividing by current index
        select
            t.evt_block_date as dt,
            st.category,
            st.symbol,
            ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.atoken_evt_mint t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
        
        union all
        
        -- aToken Burn: User withdraws, burns aTokens (supply side)
        -- Negative amount represents principal reduction
        select
            t.evt_block_date as dt,
            st.category,
            st.symbol,
            -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.atoken_evt_burn t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
        
        union all
        
        -- debtToken Mint: User borrows, receives debt tokens (borrow side)
        -- Calculation: Extract borrowed principal by removing accrued interest
        select
            t.evt_block_date as dt,
            st.category,
            st.symbol,
            ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.variabledebttoken_evt_mint t
        join spark_tokens st using (contract_address)
        where st.category = 'debtToken'
        
        union all
        
        -- debtToken Burn: User repays, burns debt tokens (borrow side)
        -- Negative amount represents debt principal reduction
        select
            t.evt_block_date as dt,
            st.category,
            st.symbol,
            -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.variabledebttoken_evt_burn t
        join spark_tokens st using (contract_address)
        where st.category = 'debtToken'
    )
    group by 1, 2
),

-- Step 4: Track ALM (Algorithmic Liquidity Management) activity
-- ALM Address: 0x1601843c5E9bC251A3272907010AFa41Fa18347E
-- Business Role: Protocol acts as both platform operator AND major liquidity provider
-- Purpose: Protocol-owned funds for yield optimization and liquidity management
sp_transfers_alm as (
    select
        symbol,
        dt,
        sum(amount) as alm_supply_amount
    from (
        -- ALM aToken Mints: Protocol deposits own funds to earn supply yield
        -- These are protocol-owned funds, not user deposits
        select
            t.evt_block_date as dt,
            st.symbol,
            ((cast(t."value" as int256) - cast(t.balanceIncrease as int256)) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.atoken_evt_mint t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
          and t.onBehalfOf = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
          and t.evt_block_date >= st.start_date
        
        union all
        
        -- ALM aToken Burns: Protocol withdraws own funds for liquidity management
        select
            t.evt_block_date as dt,
            st.symbol,
            -((t."value" + t.balanceIncrease) / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.atoken_evt_burn t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
          and t.target = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
          and t.evt_block_date >= st.start_date
        
        union all
        
        -- ALM aToken Transfers: Direct protocol fund movements (rare edge case)
        select
            t.evt_block_date as dt,
            st.symbol,
            case when t."to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E then 1 else -1 end 
            * (t."value" / 1e18) / (index / 1e27) as amount
        from spark_protocol_ethereum.atoken_evt_balancetransfer t
        join spark_tokens st using (contract_address)
        where st.category = 'aToken'
          and 0x1601843c5E9bC251A3272907010AFa41Fa18347E in (t."from", t."to")
          and t.evt_block_date >= st.start_date
    )
    group by 1, 2
),

-- Step 5: Generate continuous daily time series
-- Purpose: Ensure no missing dates in final output, fill gaps with previous day's balance
-- Coverage: From token launch date to current date for each token
seq as (
    select
        t.symbol,
        s.dt
    from (
        select symbol, min(dt) as start_dt 
        from sp_transfers_totals 
        group by 1
    ) t
    cross join unnest(sequence(t.start_dt, current_date, interval '1' day)) as s(dt)
),

-- Step 6: Calculate running cumulative standardized principal balances
-- Method: Running sum of daily principal changes to get cumulative standardized balances
-- Note: These are "standardized principals" - multiply by current index to get actual USD values
sp_balances as (
    select
        dt,
        symbol,
        sum(coalesce(ta.alm_supply_amount, 0)) over (
            partition by symbol order by dt rows unbounded preceding
        ) as alm_supply_amount,
        sum(coalesce(tt.supply_amount, 0)) over (
            partition by symbol order by dt rows unbounded preceding
        ) as supply_amount,
        sum(coalesce(tt.borrow_amount, 0)) over (
            partition by symbol order by dt rows unbounded preceding
        ) as borrow_amount
    from seq s
    left join sp_transfers_alm ta using (dt, symbol)
    left join sp_transfers_totals tt using (dt, symbol) 
),

-- Step 7: Convert standardized principals to current USD values using compound interest indices
-- Key Transformation: standardized_principal × current_index = actual_current_balance
-- Data Source: dune.steakhouse.result_spark_protocol_markets_data provides daily indices and rates
sp_balances_index as (
    select
        b.dt,
        'ethereum' as blockchain,
        'Sparklend' as protocol_name,
        b.symbol as token_symbol,
        i.supply_index,
        i.borrow_index,
        -- 7-day annualized borrow rate: actual realized borrowing cost based on index changes
        (i.borrow_index / lag(i.borrow_index, 7) over (partition by b.symbol order by b.dt asc) - 1) / 7 * 365 as apy_7d,
        i.borrow_rate,
        i.borrow_usd,
        -- Convert standardized principals to current actual balances
        b.alm_supply_amount * i.supply_index as alm_supply_amount,
        b.supply_amount * i.supply_index as supply_amount,
        b.borrow_amount * i.borrow_index as borrow_amount,
        -- Idle liquidity: supplied funds not currently borrowed (earning lower yield)
        (b.supply_amount * i.supply_index) - (b.borrow_amount * i.borrow_index) as idle_amount
    from sp_balances b
    left join dune.steakhouse.result_spark_protocol_markets_data i
        on b.symbol = i.symbol and b.dt = i."period"
    where i.symbol in ('DAI', 'USDS')
),

-- Step 8: Integrate accessibility rewards system (BR program)
-- BR (Base Rate): 4.8% borrower rebate program (4.5% base + 0.3% accessibility component)
-- Purpose: Reduce effective borrowing costs to improve protocol accessibility
-- Impact: Reduces net protocol revenue but increases user adoption
sp_balances_rates as (
    select
        b.*,
        case when supply_amount > 0 then (alm_supply_amount / supply_amount) else 0 end as alm_share,
        r.reward_code,
        r.reward_per,
        case when supply_amount > 0 
             then (idle_amount * (alm_supply_amount / supply_amount)) 
             else 0 end as alm_idle,
        'BR*' as interest_code,
        i.reward_per as interest_per
    from sp_balances_index b
    cross join query_5353955 r -- Borrower rebate rates configuration
    cross join query_5353955 i -- Interest calculation component  
    left join sparklend_reserve_factor rf on b.token_symbol = rf.token_symbol
    where r.reward_code = 'BR' and i.reward_code = 'BR'
      and b.dt between r.start_dt and r.end_dt
      and b.dt between i.start_dt and i.end_dt
),

-- Step 9: Final revenue calculation with gross vs net distinction
-- Gross Revenue = Total Borrower Interest Payments × Reserve Factor (protocol's share)
-- Net Revenue = Gross Revenue - Borrower Accessibility Rebates
-- Business Impact: BR program reduces net revenue but improves user acquisition
sp_balances_interest as (
    select
        s.dt,
        s.blockchain,
        s.protocol_name,
        token_symbol,
        s.supply_index,
        s.borrow_index,
        s.apy_7d,
        s.borrow_rate,
        s.borrow_usd,
        s.alm_supply_amount,
        s.supply_amount,
        s.borrow_amount,
        s.idle_amount,
        s.alm_share,
        s.reward_code,
        s.reward_per,
        s.alm_idle,
        s.interest_code,
        s.interest_per,
        rf.reserve_factor,
        -- Gross protocol revenue: Total potential revenue before any rebates
        s.borrow_usd * s.borrow_rate * rf.reserve_factor as asset_rev_amount,
        -- Net protocol revenue: Actual revenue after deducting borrower accessibility rewards
        s.borrow_usd * s.borrow_rate * rf.reserve_factor - s.interest_per * s.borrow_usd as interest_amount
    from sp_balances_rates s
    join sparklend_reserve_factor rf using (token_symbol)
)

-- Final Output: Daily revenue analysis for DAI and USDS with accessibility rewards impact
-- Key Insights: Compare asset_rev_amount (gross) vs interest_amount (net) to see BR program impact
select * from sp_balances_interest order by dt desc
