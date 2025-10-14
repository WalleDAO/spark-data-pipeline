with seq as (
    select
        dt
    from unnest(
            sequence(
                date '2023-11-14',
                current_date,
                interval '1' day
            )
        ) as t(dt)
),
daily_staking_data as (
    select
        date(evt_block_time) as dt,
        coalesce(sum(assets / 1e18), 0) as daily_deposits
    from ethena_labs_ethereum.StakedUSDeV2_evt_Deposit
    where
        date(evt_block_time) >= date('2023-11-14')
    group by 1
    union all
    select
        date(evt_block_time) as dt,
        - coalesce(sum(assets / 1e18), 0) as daily_deposits
    from ethena_labs_ethereum.StakedUSDeV2_evt_Withdraw
    where
        date(evt_block_time) >= date('2023-11-14')
    group by 1
),
daily_net_staking as (
    select
        dt,
        sum(daily_deposits) as net_daily_staking
    from daily_staking_data
    group by 1
),
-- Calculate daily protocol returns from USDe transfers
daily_data as (
    select
        date(evt_block_time) as dt,
        sum(
            case
                when "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439
                    then value / 1e18
                else 0
            end
        ) as daily_protocol_return
    from ethena_labs_ethereum.usde_evt_transfer
    where
        date(evt_block_time) >= date('2023-11-14')
        and "to" = 0xf2fa332bd83149c66b09b45670bce64746c6b439 -- StakingRewardsDistributor
    group by 1
),
-- Calculate cumulative staked USDe balance over time
balances_cum as (
    select
        s.dt,
        sum(coalesce(d.net_daily_staking, 0)) over (
            order by s.dt
        ) as total_staked_usde
    from seq s
    left join daily_net_staking d
        on s.dt = d.dt
),
-- Calculate sUSDe APY using 7-day rolling average with weekly compounding
susde_apy as (
    select
        dt,
        POWER(1 + weekly_avg_daily_rate * 7, 52) - 1 AS susde_apy,
        weekly_avg_daily_rate * 365 AS susde_apr
    from (
            select
                dt,
                avg(coalesce(daily_protocol_return, 0)) over (
                    order by dt rows between 6 preceding
                        and current row
                ) / nullif(
                    avg(total_staked_usde) over (
                        order by dt rows between 6 preceding
                            and current row
                    ),
                    0
                ) as weekly_avg_daily_rate
            from balances_cum b
            left join daily_data d
                using (dt)
        )
),
main_data as (
    select
        dt,
        'ethereum' as blockchain,
        'ethena' as protocol_name,
        'USDe' as token_symbol,
        i.reward_code,
        i.reward_per,
        a.susde_apy,
        a.susde_apr,
        usde_value + susde_value + u_usde_value -(
            sum(
                case
                    when dt >= date '2025-09-16'
                    and dt <= date '2025-09-24'
                        then coalesce(usde_withdrawal_value, 0)
                    else 0
                end
            ) over (
                order by dt
            ) + usde_value + susde_value + u_usde_value
        ) / 2 as amount,
        (
            usde_value + susde_value + u_usde_value -(
                sum(
                    case
                        when dt >= date '2025-09-16'
                        and dt <= date '2025-09-24'
                            then coalesce(usde_withdrawal_value, 0)
                        else 0
                    end
                ) over (
                    order by dt
                ) + usde_value + susde_value + u_usde_value
            ) / 2
        ) / 365 * i.reward_per as daily_BR_cost,
        usde_value,
        usde_withdrawal_value,
        susde_value,
        u_usde_value,
        daily_usde_pay_value,
        daily_payout_value,
        usde_value + susde_value + u_usde_value as total_holdings,
        sum(
            case
                when dt >= date '2025-09-16'
                and dt <= date '2025-09-24'
                    then coalesce(usde_withdrawal_value, 0)
                else 0
            end
        ) over (
            order by dt
        ) as spark_cumulative_withdrawal,
        (
            sum(
                case
                    when dt >= date '2025-09-16'
                    and dt <= date '2025-09-24'
                        then coalesce(usde_withdrawal_value, 0)
                    else 0
                end
            ) over (
                order by dt
            ) + usde_value + susde_value + u_usde_value
        ) / 2 as grove_holdings,
        usde_value + susde_value + u_usde_value -(
            sum(
                case
                    when dt >= date '2025-09-16'
                    and dt <= date '2025-09-24'
                        then coalesce(usde_withdrawal_value, 0)
                    else 0
                end
            ) over (
                order by dt
            ) + usde_value + susde_value + u_usde_value
        ) / 2 as spark_holdings,
        (
            usde_value + susde_value + u_usde_value -(
                sum(
                    case
                        when dt >= date '2025-09-16'
                        and dt <= date '2025-09-24'
                            then coalesce(usde_withdrawal_value, 0)
                        else 0
                    end
                ) over (
                    order by dt
                ) + usde_value + susde_value + u_usde_value
            ) / 2
        ) /(usde_value + susde_value + u_usde_value) as spark_share
    from seq s
    cross join query_5353955 i
    left join query_5163486 b
        using (dt)
    left join susde_apy a
        using (dt)
    where
        dt >= date '2024-10-23'
        and i.reward_code = 'BR'
        and dt between i.start_dt
        and i.end_dt
),
payout_dates as (
    select
        dt,
        daily_usde_pay_value,
        spark_share,
        case
            when daily_usde_pay_value > 0
                then dt
        end as payout_date,
        lag(
            case
                when daily_usde_pay_value > 0
                    then dt
            end
        ) ignore nulls over (
            order by dt
        ) as prev_payout_date
    from main_data
),
spark_share_avg as (
    select
        p1.dt as payout_date,
        avg(if(p2.spark_share > 0, p2.spark_share, 0)) as avg_spark_share
    from payout_dates p1
    join payout_dates p2
        on p2.dt > coalesce(p1.prev_payout_date, date '1900-01-01')
        and p2.dt <= p1.dt
    where
        p1.daily_usde_pay_value > 0
    group by p1.dt
)
select
    m.dt,
    m.blockchain,
    m.protocol_name,
    m.token_symbol,
    m.reward_code as borrow_cost_code,
    m.reward_per as borrow_cost_apr,
    m.susde_apy,
    m.susde_apr,
    case
        when dt >= date '2025-09-24'
            then 0
        else if(m.amount > 0, m.amount, 0)
    end as amount,
    case
        when dt >= date '2025-09-26'
            then 0
        when dt >= date '2025-09-16'
            then daily_usde_pay_value * coalesce(s.avg_spark_share, 0)
        else daily_usde_pay_value / 2
    end + case
        when dt >= date '2025-09-24'
            then 0
        else if(
            spark_share > 0,
            daily_payout_value * spark_share,
            0
        )
    end as daily_actual_revenue,
    case
        when dt >= date '2025-09-24'
            then 0
        else if(m.daily_BR_cost > 0, daily_BR_cost, 0)
    end as daily_BR_cost,
    m.usde_value,
    m.usde_withdrawal_value,
    m.susde_value,
    m.u_usde_value,
    m.daily_usde_pay_value,
    m.daily_payout_value,
    m.total_holdings,
    m.spark_cumulative_withdrawal,
    case when dt >= date '2025-09-24' then total_holdings else  m.grove_holdings end as grove_holdings ,
    if(m.spark_holdings > 0, m.spark_holdings, 0) as spark_holdings,
    if(m.spark_share > 0, m.spark_share, 0) as spark_share,
    s.avg_spark_share as daily_usde_pay_value_spark_share
from main_data m
left join spark_share_avg s
    on m.dt = s.payout_date
where
    m.dt >= date '2025-01-13'
order by dt desc