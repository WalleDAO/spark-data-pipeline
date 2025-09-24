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
        (
            'base',
            'vault',
            'spUSDC',
            0x7BfA7C4f149E7415b73bdeDfe609237e29CBF34A,
            date '2024-12-30'
        ),
        (
            'ethereum',
            'vault',
            'spUSDS',
            0xe41a0583334f0dc4e023acd0bfef3667f6fe0597,
            date '2025-07-16'
        ),
        (
            'ethereum',
            'vault',
            'spUSDC',
            0x56A76b428244a50513ec81e225a293d128fd581D,
            date '2025-09-08'
        )
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
        (
            'base',
            'user',
            'ALM Proxy',
            0x2917956eFF0B5eaF030abDB4EF4296DF775009cA,
            date '2024-10-23'
        )
),
seq as (
    select
        v.blockchain,
        v.vault_addr,
        s.dt
    from sp_vault_addr v
    cross join unnest(
            sequence(v.start_date, current_date, interval '1' day)
        ) as s(dt)
),
vault_total_supply as (
    select
        v.blockchain,
        v.vault_addr,
        t.evt_block_date as dt,
        sum(
            case
                when t."to" = 0x0000000000000000000000000000000000000000
                    then - cast(t.value as double) / 1e18
                when t."from" = 0x0000000000000000000000000000000000000000
                    then cast(t.value as double) / 1e18
                else 0
            end
        ) as net_shares_change
    from sp_vault_addr v
    join (
            select
                *
            from erc20_ethereum.evt_transfer
            union all
            select
                *
            from erc20_base.evt_transfer
        ) t
        on t.contract_address = v.vault_addr
    where
        (
            t."to" = 0x0000000000000000000000000000000000000000
            or t."from" = 0x0000000000000000000000000000000000000000
        )
    group by 1, 2, 3
),
alm_holdings as (
    select
        v.blockchain,
        v.vault_addr,
        t.evt_block_date as dt,
        sum(
            case
                when t."to" = a.alm_addr
                    then cast(t.value as double) / 1e18
                when t."from" = a.alm_addr
                    then - cast(t.value as double) / 1e18
                else 0
            end
        ) as net_alm_shares_change
    from sp_vault_addr v
    join sp_alm_addr a
        on v.blockchain = a.blockchain
    join (
            select
                *
            from erc20_ethereum.evt_transfer
            union all
            select
                *
            from erc20_base.evt_transfer
        ) t
        on t.contract_address = v.vault_addr
    where
        (
            t."to" = a.alm_addr
            or t."from" = a.alm_addr
        )
    group by 1, 2, 3
),
total_supply_cumulative as (
    select
        seq.blockchain,
        seq.vault_addr,
        seq.dt,
        sum(coalesce(vts.net_shares_change, 0)) over (
            partition by seq.blockchain,
            seq.vault_addr
            order by seq.dt rows between unbounded preceding
                and current row
        ) as total_supply
    from seq
    left join vault_total_supply vts
        on seq.blockchain = vts.blockchain
        and seq.vault_addr = vts.vault_addr
        and seq.dt = vts.dt
),
alm_holdings_cumulative as (
    select
        seq.blockchain,
        seq.vault_addr,
        seq.dt,
        sum(coalesce(ah.net_alm_shares_change, 0)) over (
            partition by seq.blockchain,
            seq.vault_addr
            order by seq.dt rows between unbounded preceding
                and current row
        ) as alm_holdings
    from seq
    left join alm_holdings ah
        on seq.blockchain = ah.blockchain
        and seq.vault_addr = ah.vault_addr
        and seq.dt = ah.dt
),
supply_shares as (
    select
        tsc.blockchain,
        tsc.vault_addr,
        tsc.dt,
        tsc.total_supply,
        coalesce(ahc.alm_holdings, 0) as alm_holdings
    from total_supply_cumulative tsc
    left join alm_holdings_cumulative ahc
        on tsc.blockchain = ahc.blockchain
        and tsc.vault_addr = ahc.vault_addr
        and tsc.dt = ahc.dt
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
),
vault_data_part2 as (
    select
        dt,
        vault_address as vault_addr,
        blockchain,
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
        s.total_supply * v.supply_index as supply_amount,
        s.total_supply * v.supply_index * va.vault_utilization as borrow_amount,
        va.vault_supply_rate as supply_rate,
        va.vault_borrow_rate as borrow_rate,
        s.total_supply * v.supply_index - s.total_supply * v.supply_index * va.vault_utilization as idle_amount,
        s.alm_holdings * v.supply_index as alm_supply_amount
    from supply_shares s
    left join vault_data v
        using (blockchain, vault_addr, dt)
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
        i.reward_code as borrow_cost_code,
        i.reward_per as borrow_cost_per,
        b.performance_fee
    from vault_balances b
    cross join query_5353955 i
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
                then alm_borrow_amount * borrow_cost_per
            when token_symbol in ('USDC')
                then alm_supply_amount * borrow_cost_per
        end as BR_cost
    from vault_balances_rates
)
select
    *
from vault_balances_interest
order by dt desc