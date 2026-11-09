
{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

with commodity_codes as (

    select
        cmd_code,
        classification_code,
        hs_version

    from {{ ref('stg_comtrade_world_total') }}

    union all

    select
        cmd_code,
        classification_code,
        hs_version

    from {{ ref('stg_comtrade_partner_detail') }}

),

unique_commodities as (

    select distinct
        cmd_code,
        classification_code,
        hs_version

    from commodity_codes

),

commodity_dimension as (

    select
        {{
            comtrade_surrogate_key([
                'cmd_code',
                'classification_code',
                'hs_version'
            ])
        }} as commodity_key,

        cmd_code,
        classification_code,
        hs_version,

        length(cmd_code) as commodity_code_length

    from unique_commodities

)

select *
from commodity_dimension
