/*
 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3 -> USDe
 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 -> sUSDe
 0x7FC7c91D556B400AFa565013E3F32055a0713425 -> USDeSilo 
 0x1601843c5E9bC251A3272907010AFa41Fa18347E -> Spark Liquidity Layer (SLL)
 
 sUSDe to USDe:
 Step 1: sUSDe withdrawal process (receiver = Silo addr)
 1) sUSDe is burnt
 2) USDe is sent to Silo
 Step 2: once the cooldown process is completed
 3) USDe is sent back to Spark Liquidity Layer
 */
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
    -- sUSDe payments to Spark (SSL)
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
        sum(coalesce(b.amount, 0)) over (
            partition by symbol
            order by dt asc
        ) as amount
    from seq s
    left join balance_sll b
        using (dt, symbol)
),
balance_ssl_pivot as (
    select
        dt,
        sum(if(symbol = 'USDe', amount, 0)) as usde_amount,
        sum(if(symbol = 'USDe_pay', amount, 0)) as usde_pay_amount,
        sum(if(symbol = 'sUSDe', amount, 0)) as susde_amount,
        sum(if(symbol = 'u-USDe', amount, 0)) as u_usde_amount
    from balance_ssl_cum
    group by 1
),
balance_ssl_pivot_clean as (
    select
        dt,
        if(usde_amount > 1e-6, usde_amount, 0) as usde_amount,
        if(usde_pay_amount > 1e-6, usde_pay_amount, 0) as usde_pay_amount,
        if(susde_amount > 1e-6, susde_amount, 0) as susde_amount,
        if(u_usde_amount > 1e-6, u_usde_amount, 0) as u_usde_amount
    from balance_ssl_pivot
),
susde as (
    select
        date(dt) as dt,
        'ethereum' as blockchain,
        'sUSDe' as symbol,
        0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 as token_address,
        -- sUSDe
        0x4c9EDD5852cd905f086C759E8383e09bff1E68B3 as price_address,
        -- USDe
        true as use_share_price,
        max_by(assets, dt) as assets,
        max_by(shares, dt) as shares
    from (
            select
                evt_block_time as dt,
                assets,
                shares
            from ethena_labs_ethereum.stakedusdev2_evt_deposit
            where
                shares > 1e10
            union all
            select
                evt_block_time as dt,
                assets,
                shares
            from ethena_labs_ethereum.stakedusdev2_evt_withdraw
            where
                shares > 1e10
        )
    group by 1
),
share_pricing as (
    select
        dt,
        blockchain,
        symbol,
        token_address,
        price_address,
        assets / cast(shares as double) as share_price
    from susde
    where
        shares > 0
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
    where
        token_address = 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 -- sUSDe
),
balance_ssl_final as (
    select
        dt,
        p.susde_price,
        p.susde_yield,
        b.usde_amount as usde_value,
        b.usde_pay_amount as usde_pay_value,
        b.susde_amount * p.susde_price as susde_value,
        b.u_usde_amount as u_usde_value,
        --least(b.usde_amount, b.susde_amount * p.susde_price + b.u_usde_amount) + b.u_usde_amount as payout_base_value,
        b.susde_amount * p.susde_price * coalesce(p.susde_yield, 0) as daily_payout_value
    from balance_ssl_pivot_clean b
    left join susde_price_usd p
        using (dt)
),
balance_ssl_yield_cum as (
    select
        *,
        sum(coalesce(daily_payout_value, 0)) over (
            order by dt asc
        ) as payout_value
    from balance_ssl_final
)
select
    *
from balance_ssl_yield_cum
order by dt desc
    /*
     --select * from ethena_labs_ethereum.stakedusdev2_evt_withdraw
     --where sender = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- SLL
     0x019839e6e4f828be736513c11c3ba07c07e3018c54cfb5730f5bcf8ab422cb09 -- $204.3M sUSDe burnt, $202.2M USDe to Silo, 2025-02-21 15:51
     0xb2bb097055b5ae119828e500c5eb7c884eaa579f593bf38c24914565284a33de -- $2.9M sUSDe burnt, $2.9M USDe to Silo, 2025-02-14 15:13
     
     --select * from ethena_labs_ethereum.usde_evt_transfer
     --where "to" = 0x1601843c5E9bC251A3272907010AFa41Fa18347E -- SLL
     --  and "from" = 0x7FC7c91D556B400AFa565013E3F32055a0713425 -- Silo
     0x04a542c06c78efdd8bd54e7307dd1ba879444c29d3903bf4daacb7b9dfb1a114 -- $202.2M USDe to SLL , 2025-02-28 16:01
     0x02c4d8d085bf96ccefb28e6cd525eaa30cd4945eb2046e0dd45dd54c9eb54a01 -- $2.9M USDe to SLL , 2025-02-21 15:19
     */