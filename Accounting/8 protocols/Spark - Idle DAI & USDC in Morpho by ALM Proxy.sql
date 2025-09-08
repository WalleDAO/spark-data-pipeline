with sp_vault_addr (
    blockchain,
    category,
    symbol,
    vault_addr,
    start_date
) as (
    values
        (
            'ethereum',
            'vault',
            'spDAI',
            0x73e65dbd630f90604062f6e02fab9138e713edd9,
            date '2024-03-15'
        ),
        ---Spark DAI Vault @ Ethereum
        (
            'base',
            'vault',
            'spUSDC',
            0x7BfA7C4f149E7415b73bdeDfe609237e29CBF34A,
            date '2024-12-30'
        ),
        -- Spark USDC Vault @ Base
        (
            'ethereum',
            'vault',
            'spUSDS',
            0xe41a0583334f0dc4e023acd0bfef3667f6fe0597,
            date '2025-07-16'
        ) -- Spark USDS Vault @ Ethereum
),
sp_alm_addr (
    blockchain,
    category,
    symbol,
    alm_addr,
    start_date
) as (
    values
        (
            'ethereum',
            'user',
            'ALM Proxy',
            0x1601843c5E9bC251A3272907010AFa41Fa18347E,
            date '2024-10-23'
        ),
        -- ALM Proxy @ Ethereum
        (
            'base',
            'user',
            'ALM Proxy',
            0x2917956eFF0B5eaF030abDB4EF4296DF775009cA,
            date '2024-10-23'
        ) -- ALM Proxy @ Base
),
-- get all shares transfers from morpho vault spDAI
vault_transfers_sum as (
    select
        t.chain as blockchain,
        t.evt_block_date as dt,
        t.contract_address as vault_addr,
        t.owner as user_addr,
        sum(t."value" / 1e18) as shares
    from (
            select
                chain,
                evt_block_date,
                contract_address,
                "to" as owner,
                "value"
            from metamorpho_vaults_multichain.metamorpho_evt_transfer
            where
                "to" <> 0x0000000000000000000000000000000000000000
            union all
            select
                chain,
                evt_block_date,
                contract_address,
                "to" as owner,
                "value"
            from metamorpho_vaults_multichain.metamorphov1_1_evt_transfer
            where
                "to" <> 0x0000000000000000000000000000000000000000
            union all
            select
                chain,
                evt_block_date,
                contract_address,
                "from" as owner,
                - "value"
            from metamorpho_vaults_multichain.metamorpho_evt_transfer
            where
                "from" <> 0x0000000000000000000000000000000000000000
            union all
            select
                chain,
                evt_block_date,
                contract_address,
                "from" as owner,
                - "value"
            from metamorpho_vaults_multichain.metamorphov1_1_evt_transfer
            where
                "from" <> 0x0000000000000000000000000000000000000000
        ) t
    join sp_vault_addr v
        on t.chain = v.blockchain
        and t.contract_address = v.vault_addr
    join sp_alm_addr u
        on t.chain = u.blockchain
        and t.owner = u.alm_addr
    group by 1, 2, 3, 4
),
seq as (
    select
        blockchain,
        vault_addr,
        user_addr,
        s.dt
    from (
            select
                blockchain,
                vault_addr,
                user_addr,
                min(dt) as start_dt
            from vault_transfers_sum
            group by 1, 2, 3
        ) t
    cross join unnest(
            sequence(t.start_dt, current_date, interval '1' day)
        ) as s(dt)
),
vault_data as (
    select
        v.blockchain,
        v.vault_address as vault_addr,
        v.token_symbol,
        v.dt,
        v.supply_index,
        v.performance_fee
    from query_5739088 v
    join sp_vault_addr va
        on v.blockchain = va.blockchain
        and v.vault_address = va.vault_addr
),
vault_data_part2 as (
    select
        dt,
        vault_address as vault_addr,
        blockchain,
        vault_supply_amount,
        vault_borrow_amount,
        vault_utilization,
        vault_supply_rate,
        vault_borrow_rate
    from query_5728806
),
vault_balances as (
    select
        dt,
        blockchain,
        'Morpho' as protocol_name,
        v.token_symbol,
        v.supply_index,
        va.vault_utilization as util_rate,
        v.performance_fee,
        va.vault_supply_amount as supply_amount,
        va.vault_borrow_amount as borrow_amount,
        va.vault_supply_rate as supply_rate,
        va.vault_borrow_rate as borrow_rate,
        va.vault_supply_amount - va.vault_borrow_amount as idle_amount,
        sum(coalesce(t.shares, 0)) over (
            partition by blockchain,
            vault_addr,
            user_addr
            order by dt asc
        ) * v.supply_index as alm_supply_amount
    from seq s
    left join vault_data v
        using (blockchain, vault_addr, dt)
    left join vault_transfers_sum t
        using (blockchain, vault_addr, user_addr, dt)
    left join vault_data_part2 va
        using (blockchain, vault_addr, dt)
),
vault_balances_rates as (
    select
        b.dt,
        b.blockchain,
        b.protocol_name,
        b.token_symbol,
        b.supply_amount,
        b.borrow_amount,
        b.util_rate,
        b.idle_amount,
        b.alm_supply_amount,
        b.alm_supply_amount * b.util_rate as alm_borrow_amount,
        b.alm_supply_amount - b.alm_supply_amount * b.util_rate as alm_idle,
        supply_rate,
        borrow_rate,
        i.reward_code,
        i.reward_per,
        'APR-BR' as interest_code,
        supply_rate-i.reward_per as interest_per,
        b.performance_fee
    from vault_balances b
    cross join query_5353955 i -- Spark - Accessibility Rewards - Rates
    where
        i.reward_code = 'BR'
        and b.dt between i.start_dt
        and i.end_dt
),
vault_balances_interest as (
    select
        *,
        case
            when token_symbol in ('USDS', 'DAI')
                then alm_borrow_amount * borrow_rate
            when token_symbol in ('USDC')
                then alm_supply_amount * supply_rate
        end as interest_amount,
        case
            when token_symbol in ('USDS', 'DAI')
                then alm_borrow_amount * reward_per
            when token_symbol in ('USDC')
                then alm_supply_amount * reward_per
        end as BR_cost
    from vault_balances_rates
)
select
    *
from vault_balances_interest
order by dt desc