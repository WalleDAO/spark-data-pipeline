with
    -- Token targets: sUSDS and sUSDC in all chains
    token_targets (blockchain, symbol, contract_address, start_date) as (
        values
            -- sUSDS
            ('ethereum', 'sUSDS', 0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD, date '2024-09-04'),
            ('base', 'sUSDS', 0x5875eEE11Cf8398102FdAd704C9E96607675467a, date '2024-10-10'),
            ('arbitrum', 'sUSDS', 0xdDb46999F8891663a8F2828d25298f70416d7610, date '2025-01-29'),
            ('optimism', 'sUSDS', 0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0, date '2025-04-30'),
            ('unichain', 'sUSDS', 0xA06b10Db9F390990364A3984C04FaDf1c13691b5, date '2025-04-30'),
            -- sUSDC
            ('ethereum', 'sUSDC', 0xBc65ad17c5C0a2A4D159fa5a503f4992c7B545FE, date '2025-03-03'),
            ('base', 'sUSDC', 0x3128a0F7f0ea68E7B7c9B00AFa7E41045828e858, date '2025-03-03'),
            ('arbitrum', 'sUSDC', 0x940098b108fB7D0a7E374f6eDED7760787464609, date '2025-03-03'),
            ('optimism', 'sUSDC', 0xCF9326e24EBfFBEF22ce1050007A43A3c0B6DB55, date '2025-05-26'),
            ('unichain', 'sUSDC', 0x14d9143BEcC348920b68D123687045db49a016C6, date '2025-05-14')
    ),

    all_token_transfers as (
        -- Ethereum
        select
            'ethereum' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from erc20_ethereum.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'ethereum'
        
        union all
        
        select
            'ethereum' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from erc20_ethereum.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'ethereum'
        
        union all
        
        -- Base
        select
            'base' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from erc20_base.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'base'
        
        union all
        
        select
            'base' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from erc20_base.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'base'
        
        union all
        
        -- Arbitrum
        select
            'arbitrum' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from erc20_arbitrum.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'arbitrum'
        
        union all
        
        select
            'arbitrum' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from erc20_arbitrum.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'arbitrum'
        
        union all
        
        -- Optimism
        select
            'optimism' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from erc20_optimism.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'optimism'
        
        union all
        
        select
            'optimism' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from erc20_optimism.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'optimism'
        
        union all
        
        -- Unichain
        select
            'unichain' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."to" as user_addr,
            tr."value" / 1e18 as amount
        from erc20_unichain.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."to" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'unichain'
        
        union all
        
        select
            'unichain' as blockchain,
            ta.contract_address,
            tr.evt_block_date as dt,
            tr.evt_block_time as ts,
            tr.evt_block_number,
            tr.evt_tx_hash,
            tr.evt_index,
            tr."from" as user_addr,
            -tr."value" / 1e18 as amount
        from erc20_unichain.evt_Transfer tr
        join token_targets ta on tr.contract_address = ta.contract_address
        where date(tr.evt_block_date) >= ta.start_date
          and tr."from" != 0x0000000000000000000000000000000000000000
          and ta.blockchain = 'unichain'
    )
    
select
    blockchain,
    contract_address,
    dt,
    ts,
    evt_block_number,
    evt_tx_hash,
    evt_index,
    user_addr,
    sum(amount) as amount_change
from all_token_transfers
group by 1,2,3,4,5,6,7,8
order by blockchain, contract_address, dt desc, ts desc
