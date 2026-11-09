
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
            'cmd_code'
        ],
        tags=['marts', 'concentration']
    )
}}

with scope_quality as (

    select *
    from {{ ref('int_comtrade_scope_quality') }}

),

enriched as (

    select
        q.period_start_date,

        cast(
            format_date('%Y%m', q.period_start_date)
            as int64
        ) as period_key,

        q.period,
        q.reporter_code,
        q.cmd_code,
        q.flow_code,
        q.classification_code,
        q.hs_version,
        q.freq_code,
        q.partner2_code,
        q.customs_code,
        q.mot_code,
        q.is_original_classification,

        q.world_record_count,
        q.partner_record_count,
        q.observed_partner_count,
        q.negative_value_count,

        q.world_total_value,
        q.partner_total_value,
        q.reconciliation_gap,
        q.partner_coverage_ratio,
        q.reconciliation_status,
        q.is_value_reconciled,

        q.hhi_reported_partners,
        q.effective_partner_count,
        q.hhi_status,

        q.has_usable_world_comparison,
        q.has_observed_concentration,
        q.source_assurance_level

    from scope_quality as q

)

select
    *,

    case
        when world_record_count = 0
         and partner_record_count > 0
            then 'MISSING_WORLD_TOTAL'

        when partner_record_count = 0
            then 'MISSING_PARTNER_DETAIL'

        when negative_value_count > 0
            then 'INVALID_SOURCE_VALUES'

        when hhi_reported_partners is null
            then 'NOT_CALCULABLE'

        when reconciliation_status = 'MATCHED_VALUE'
            then 'PROVISIONAL_VALUE_MATCH'

        when reconciliation_status = 'PARTNER_EXCEEDS_WORLD'
            then 'PARTNER_EXCEEDS_WORLD'

        else 'PROVISIONAL_UNRECONCILED'
    end as concentration_quality_status

from enriched
