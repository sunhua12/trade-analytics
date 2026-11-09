
{{
    config(
        materialized='table',
        partition_by={
            'field': 'period_start_date',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by=[
            'reporter_code',
            'cmd_code',
            'query_type'
        ],
        tags=['core', 'fact']
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
        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from {{ ref('stg_comtrade_world_total') }}

    union all

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
        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from {{ ref('stg_comtrade_partner_detail') }}

),

fact_records as (

    select

        {{
            comtrade_surrogate_key([
                'period',
                'reporter_code',
                'partner_code',
                'cmd_code',
                'query_type',
                'flow_code',
                'classification_code',
                'hs_version',
                'freq_code',
                'partner2_code',
                'customs_code',
                'mot_code',
                'is_original_classification'
            ])
        }} as trade_observation_id,

        cast(
            format_date('%Y%m', period_start_date)
            as int64
        ) as period_key,

        {{
            comtrade_surrogate_key([
                'cmd_code',
                'classification_code',
                'hs_version'
            ])
        }} as commodity_key,

        period_start_date,
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

        primary_value as import_value,

        net_weight,
        quantity,

        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from source_records

)

select *
from fact_records
