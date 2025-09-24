with seq as (
    select
        s.dt,
        t.symbol,
        t.amount
    from unnest(
            sequence(
                date '2024-10-23',
                current_date,
                interval '1' day
            )
        ) as s(dt)
    cross join (
            select
                'USDe' as symbol,
                0 as amount
            union all
            select
                'USDe_withdrawal' as symbol,
                0 as amount
            union all
            select
                'USDe_pay' as symbol,
                0 as amount
            union all
            select
                'sUSDe' as symbol,
                0 as amount
            union all
            select
                'u-USDe' as symbol,
                0 as amount
        ) t
),
balance_sll as (
    -- USDe balance on Spark (SLL)
    select
        date(evt_block_time) as dt,
        'USDe' as "symbol",
        sum(
            case
                when "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                    then "value"
                when "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                    then - "value"
                else 0
            end
        ) / 1e18 as amount
    from ethena_labs_ethereum.usde_evt_transfer
    group by 1
    union all
    select
        date(evt_block_time) as dt,
        'USDe_withdrawal' as "symbol",
        sum("value") / 1e18 as amount
    from ethena_labs_ethereum.usde_evt_transfer
    where
        "to" = 0x0000000000000000000000000000000000000000
        and "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
    group by 1
    union all
    -- USDe pay value to Spark (SSL)
    select
        date(evt_block_time) as dt,
        'USDe_pay' as "symbol",
        sum("value") / 1e18 as amount
    from ethena_labs_ethereum.usde_evt_transfer
    where
        "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- SSL
        and "from" not in (
            0x7FC7c91D556B400AFa565013E3F32055a0713425,
            -- USDeSilo
            0x0000000000000000000000000000000000000000 -- mints
        )
    group by 1
    union all
    -- sUSDe balance on Spark (SLL)
    select
        date(evt_block_time) as dt,
        'sUSDe' as "symbol",
        sum(
            case
                when "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                    then "value"
                when "from" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E
                    then - "value"
                else 0
            end
        ) / 1e18 as amount
    from ethena_labs_ethereum.stakedusdev2_evt_transfer
    group by 1
    union all
    -- unbounding USDe sent to the Silo
    select
        dt,
        'u-USDe' as "symbol",
        sum(amount) as amount
    from (
            select
                date(evt_block_time) as dt,
                sum(assets) / 1e18 as amount
            from ethena_labs_ethereum.stakedusdev2_evt_withdraw
            where
                sender = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- SLL
            group by 1
            union all
            -- Subtract u-USDe when USDe is returned from Silo to SLL
            select
                date(evt_block_time) as dt,
                - sum(value) / 1e18 as amount
            from ethena_labs_ethereum.usde_evt_transfer
            where
                "from" = 0x7FC7c91D556B400AFa565013E3F32055a0713425 -- Silo
                and "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- SLL
            group by 1
        )
    group by 1
),
balance_ssl_cum as (
    select
        dt,
        symbol,
        case
            when symbol = 'USDe_withdrawal'
                then coalesce(b.amount, 0)
            else sum(coalesce(b.amount, 0)) over (
                partition by symbol
                order by dt asc
            )
        end as amount
    from seq s
    left join balance_sll b
        using (dt, symbol)
),
balance_ssl_pivot as (
    select
        dt,
        sum(if(symbol = 'USDe', amount, 0)) as usde_amount,
        sum(if(symbol = 'USDe_withdrawal', amount, 0)) as USDe_withdrawal_amount,
        sum(if(symbol = 'USDe_pay', amount, 0)) as usde_pay_amount,
        sum(if(symbol = 'sUSDe', amount, 0)) as susde_amount,
        sum(if(symbol = 'u-USDe', amount, 0)) as u_usde_amount
    from balance_ssl_cum
    group by 1
),
balance_ssl_pivot_clean as (
    select
        dt,
        if(usde_amount > 0, usde_amount, 0) as usde_amount,
        if(
            USDe_withdrawal_amount > 0,
            USDe_withdrawal_amount,
            0
        ) as USDe_withdrawal_amount,
        if(usde_pay_amount > 0, usde_pay_amount, 0) as usde_pay_amount,
        if(susde_amount > 0, susde_amount, 0) as susde_amount,
        if(u_usde_amount > 0, u_usde_amount, 0) as u_usde_amount
    from balance_ssl_pivot
),
susde as (
    select
        date(dt) as dt,
        max_by(assets, dt) as assets,
        max_by(shares, dt) as shares
    from (
            select
                evt_block_time as dt,
                assets,
                shares
            from ethena_labs_ethereum.stakedusdev2_evt_deposit
            where
                shares > 1
                and assets > 1
            union all
            select
                evt_block_time as dt,
                assets,
                shares
            from ethena_labs_ethereum.stakedusdev2_evt_withdraw
            where
                shares > 1
                and assets > 1
        )
    group by 1
),
share_pricing as (
    select
        dt,
        assets / cast(shares as double) as share_price
    from susde
),
susde_price_usd as (
    select
        dt,
        share_price as susde_price,
        (
            share_price / lag(share_price) over (
                order by dt
            )
        ) - 1 as susde_yield
    from share_pricing
)
select
    dt,
    p.susde_price,
    p.susde_yield,
    b.usde_amount as usde_value,
    b.USDe_withdrawal_amount as usde_withdrawal_value,
    b.usde_pay_amount as usde_pay_value,
    b.susde_amount * p.susde_price as susde_value,
    b.u_usde_amount as u_usde_value,
    coalesce(
        b.usde_pay_amount - lag(b.usde_pay_amount) over (
            order by dt
        ),
        0
    ) as daily_usde_pay_value,
    b.susde_amount * p.susde_price * coalesce(p.susde_yield, 0) as daily_payout_value
from balance_ssl_pivot_clean b
left join susde_price_usd p
    using (dt)