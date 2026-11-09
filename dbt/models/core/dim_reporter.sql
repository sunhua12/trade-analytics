
{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

with reporter_codes as (

    select reporter_code
    from {{ ref('stg_comtrade_world_total') }}

    union all

    select reporter_code
    from {{ ref('stg_comtrade_partner_detail') }}

),

reporter_dimension as (

    select distinct
        reporter_code,

        cast(reporter_code as string)
            as reporter_code_text

    from reporter_codes

)

select *
from reporter_dimension
