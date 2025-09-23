with -- Token targets
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
-- Protocol targets
spark_targets (reward_code,ref_code,blockchain,protocol_name,protocol_addr,start_date) as (
    values 
        ( 'XR', 185,'ethereum','Euler',0x1e548CfcE5FCF17247E024eF06d32A01841fF404,date '2024-10-17'),
        ( 'XR', 185,'ethereum','Euler',0x1cA03621265D9092dC0587e1b50aB529f744aacB,date '2024-12-18'),
        ( 'XR', 185,'unichain','Euler',0x7650D7ae1981f2189d352b0EC743b9099D24086F,date '2025-06-02'),
        ( 'XR', 185,'arbitrum','Euler',0xa7a9B773f139010f284E825a74060648d91dE37a,date '2025-07-07'),
        ( 'XR', 185,'arbitrum','Euler',0x8E8FA7c73BeF97205552456A7D41E14cAf57404D,date '2025-08-31'),
        ( 'XR', 185,'arbitrum','Euler',0x0EE8D628411F446BFbbe08BDeF53E42414C8fBC4,date '2025-07-07'),
        ( 'XR', 185,'arbitrum','Euler',0x7007415237e9b1c729404A3363293e0a44EAF585,date '2025-08-31'),
        ( 'XR', 185,'base','Euler',0x65cFEF3Efbc5586f0c05299343b8BeFb3fF5d81a,date '2025-02-07'),
        ( 'XR', ,'ethereum','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-02-16'),
        ( 'XR', ,'arbitrum','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-06-10'),
        ( 'XR', ,'base','Fluid',0x52Aa899454998Be5b000Ad077a46Bbe360F4e497,date '2024-07-21')
)