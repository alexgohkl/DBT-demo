{{ 
    config(
        materialized = 'table',
        tags = ['stage', 'github', 'daily']
    ) 
}}

with 

JHU_COVID_19 as (
    select * from {{ source('DEV', 'JHU_COVID_19')}}
),

-- data with case_type active and recovered is not reliable, therefore filtered
STG_JHU_COVID_19 as (
    select
        JHU_COVID_19.COUNTRY_REGION as COUNTRY,
        JHU_COVID_19.ISO3166_1 as COUNTRY_CODE,
        JHU_COVID_19.PROVINCE_STATE as PROVINCE_STATE,
        JHU_COVID_19.ISO3166_2 as PROVINCE_STATE_CODE,
        JHU_COVID_19.DATE as REPORTED_DATE,
        JHU_COVID_19.CASE_TYPE as CASE_TYPE,
        JHU_COVID_19.CASES as ACCUMULATED_CASES,
        JHU_COVID_19.DIFFERENCE as NEW_CASES,
        JHU_COVID_19.LAST_UPDATED_DATE as LAST_UPDATED_DATE,
        JHU_COVID_19.LAST_REPORTED_FLAG as LAST_REPORTED_FLAG
    from
        JHU_COVID_19 as JHU_COVID_19
    where
        CASE_TYPE IN ('Confirmed', 'Deaths')
)

select * from STG_JHU_COVID_19

