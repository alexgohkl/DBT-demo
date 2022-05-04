{{ 
    config(
        materialized = 'table',
        tags = ['datamarts']
    ) 
}}

with 

STG_JHU_COVID_19 as (
    select * from {{ ref('STG_JHU_COVID_19')}}
),

-- Extract required info for AUS COVID cases
STG_AUS_COVID_MART as (
    select
        STG_JHU_COVID_19.PROVINCE_STATE_CODE as STATE,
        STG_JHU_COVID_19.REPORTED_DATE as REPORTED_DATE,
        -- ZEROIFNULL(STG_JHU_COVID_19.NEW_CASES) as NEW_CASES
        {{ force_non_negative('STG_JHU_COVID_19.NEW_CASES') }} as NEW_CASES
    from
        STG_JHU_COVID_19 as STG_JHU_COVID_19
    where
        STG_JHU_COVID_19.COUNTRY = 'Australia'
        and STG_JHU_COVID_19.CASE_TYPE = 'Confirmed'
        and STG_JHU_COVID_19.LAST_REPORTED_FLAG = False
        and STG_JHU_COVID_19.PROVINCE_STATE_CODE is not NULL
),

-- Pivot the table to timeseries format+tag
AUS_COVID_MART as (
    select
        REPORTED_DATE as REPORTED_DATE,
        ZEROIFNULL("'ACT'") as ACT_NEW_CASES,
        ZEROIFNULL("'NSW'") as NSW_NEW_CASES,
        ZEROIFNULL("'NT'") as NT_NEW_CASES,
        ZEROIFNULL("'QLD'") as QLD_NEW_CASES,
        ZEROIFNULL("'SA'") as SA_NEW_CASES,
        ZEROIFNULL("'TAS'") as TAS_NEW_CASES,
        ZEROIFNULL("'WA'") as WA_NEW_CASES,
        ZEROIFNULL("'VIC'") as VIC_NEW_CASES
    from
        STG_AUS_COVID_MART as STG_AUS_COVID_MART
        pivot(SUM(STG_AUS_COVID_MART.NEW_CASES) for STATE in ('ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'WA', 'VIC'))  
    order by
        STG_AUS_COVID_MART.REPORTED_DATE asc
)

select * from AUS_COVID_MART



