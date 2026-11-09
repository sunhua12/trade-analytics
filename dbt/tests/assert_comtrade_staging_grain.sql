
with staging_records as (

    select
        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification

    from {{ ref('stg_comtrade_world_total') }}

    union all

    select
        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification

    from {{ ref('stg_comtrade_partner_detail') }}

),

duplicate_business_grain as (

    select
        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        count(*) as duplicate_count

    from staging_records

    group by
        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification

    having count(*) > 1

)

select *
from duplicate_business_grain
