-- @dev: perhaps better to include in query_5531960

with
    -- Token targets
    token_targets (blockchain, token_symbol, token_addr, decimals, start_date) as (
        values
            ('ethereum', 'USDS', 0xdC035D45d973E3EC169d2276DDab16f1e407384F, 18, date '2024-09-02')
    ),
    -- Protocol targets
    spark_targets (code, reward_code, interest_code, ref_code, blockchain, protocol_name, protocol_addr, start_date) as (
        values
        -- Spark Proxy (Treasury)
            (3, 'AR', 'BR', 126, 'ethereum', 'Treasury', 0x3300f198988e4C9C63F75dF86De36421f06af8c4, date '2023-05-26')
    ),
    -- Referral codes
    ref_codes (ref_code, integrator_name) as (
        select * from query_5354195 -- Spark - Accessibility Rewards - Referrals
    ),
    protocol_balances as (
        select
            tr.blockchain,
            tt.token_addr,
            tr.block_date as dt,
            tr."to" as protocol_addr,
            tr.amount_raw / power(10, tt.decimals) as amount
        from tokens.transfers tr
        join token_targets tt
            on tr.blockchain = tt.blockchain
            and tr.contract_address = tt.token_addr
        join spark_targets st
            on tr.blockchain = st.blockchain
            and tr."to" = st.protocol_addr
        where date(tr.block_date) >= tt.start_date
          and tt.token_symbol in ('USDS')
          and st.code in (3)
        union all
        select
            tr.blockchain,
            tt.token_addr,
            tr.block_date as dt,
            tr."from" as protocol_addr,
            -tr.amount_raw / power(10, tt.decimals) as amount
        from tokens.transfers tr
        join token_targets tt
            on tr.blockchain = tt.blockchain
            and tr.contract_address = tt.token_addr
        join spark_targets st
            on tr.blockchain = st.blockchain
            and tr."from" = st.protocol_addr
        where date(tr.block_date) >= tt.start_date
          and tt.token_symbol in ('USDS')
          and st.code in (3)
    ),
    totals_check as (
        select blockchain, token_addr, protocol_addr, sum(amount) as amount from protocol_balances group by 1,2,3
    ),
    protocol_balances_sum as (
        select
            blockchain,
            protocol_addr,
            token_addr,
            dt,
            sum(amount) as amount
        from protocol_balances
        group by 1,2,3,4
    ),
    seq as (
        select
            b.blockchain,
            b.protocol_addr,
            b.token_addr,
            s.dt
        from (select blockchain, protocol_addr, token_addr, min(dt) as start_dt from protocol_balances_sum group by 1,2,3) b
        cross join unnest(sequence(b.start_dt, current_date, interval '1' day)) as s(dt)
    ),
    -- get cumulative balance
    protocol_balances_cum as (
        select
            blockchain,
            protocol_addr,
            tt.token_symbol,
            dt,
            st.reward_code,
            st.protocol_name,
            st.ref_code,
            sum(coalesce(b.amount, 0)) over (partition by blockchain, protocol_addr, token_addr order by dt asc) as amount
        from seq s
        join token_targets tt using (blockchain, token_addr)
        join spark_targets st using (blockchain, protocol_addr)
        left join protocol_balances_sum b using (blockchain, protocol_addr, token_addr, dt)
    ),
    protocol_balances_proj as (
        select
            b.dt,
            b.blockchain,
            b.protocol_name,
            b.reward_code,
            p.reward_per,
            b.ref_code,
            b.amount,
            avg((b.amount / 365) * p.reward_per) over (
                partition by b.blockchain, b.ref_code
                order by b.dt
                rows between 6 preceding and current row
            ) as tw_reward_7d_avg
        from protocol_balances_cum b
        join query_5353955 p -- Spark - Accessibility Rewards - Rates
          on b.reward_code = p.reward_code
          and b.dt between p.start_dt and p.end_dt
    ),
    protocol_balances_monthly as (
        select
            date_trunc('month', dt) as dt,
            blockchain,
            ref_code,
            protocol_name,
            integrator_name,
            reward_code,
            reward_per,
            sum(amount / 365) as tw_amount,
            sum((amount / 365) * reward_per) as tw_reward,
            max_by(tw_reward_7d_avg, dt) as tw_reward_7d_avg,
            max_by(tw_reward_7d_avg, dt) * 365 as tw_reward_proj_1y
        from protocol_balances_proj
        join ref_codes using (ref_code)
        group by 1,2,3,4,5,6,7  
    )

select *
from protocol_balances_monthly
order by dt desc, protocol_name asc, blockchain asc
