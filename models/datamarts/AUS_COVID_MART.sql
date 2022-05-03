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

AUS_COVID_MART as (
    select
        *
    from
        STG_AUS_COVID_MART as STG_AUS_COVID_MART
        pivot(SUM(NEW_CASES) for STATE in ('ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'WA', 'VIC'))
    order by
        REPORTED_DATE asc
)

select * from AUS_COVID_MART



