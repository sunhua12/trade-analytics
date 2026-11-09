
{{
    config(
        materialized='view',
        tags=['staging', 'world_total']
    )
}}

with source_records as (

    select
        period_start_date,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        primary_value,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        net_weight,
        quantity,
        source_record_json,
        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from {{ source('comtrade_raw', 'comtrade_records') }}

),

world_total as (

    select
        period_start_date,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        primary_value,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        net_weight,
        quantity,
        source_record_json,
        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from source_records

    where query_type = 'world_total'
      and partner_code = 0

)

select *
from world_total
