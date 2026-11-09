
{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

with partner_codes as (

    select partner_code
    from {{ ref('stg_comtrade_world_total') }}

    union all

    select partner_code
    from {{ ref('stg_comtrade_partner_detail') }}

),

partner_dimension as (

    select distinct
        partner_code,

        cast(partner_code as string)
            as partner_code_text,

        case
            when partner_code = 0
                then 'WORLD_TOTAL'
            else 'PARTNER_CODE'
        end as partner_type

    from partner_codes

)

select *
from partner_dimension
