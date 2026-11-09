
{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

with source_periods as (

    select period_start_date
    from {{ ref('stg_comtrade_world_total') }}

    union all

    select period_start_date
    from {{ ref('stg_comtrade_partner_detail') }}

),

unique_periods as (

    select distinct
        period_start_date

    from source_periods

),

period_dimension as (

    select
        cast(
            format_date('%Y%m', period_start_date)
            as int64
        ) as period_key,

        period_start_date,

        extract(year from period_start_date)
            as year_number,

        extract(quarter from period_start_date)
            as quarter_number,

        extract(month from period_start_date)
            as month_number,

        format_date('%Y-%m', period_start_date)
            as year_month

    from unique_periods

)

select *
from period_dimension
