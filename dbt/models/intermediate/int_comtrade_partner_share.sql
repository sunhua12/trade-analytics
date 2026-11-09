
{{
    config(
        materialized='view',
        tags=['intermediate', 'partner_share']
    )
}}

with partner_records as (

    select *
    from {{ ref('stg_comtrade_partner_detail') }}

),

scope_totals as (

    select *
    from {{ ref('int_comtrade_scope_reconciliation') }}

),

enriched as (

    select
        p.period_start_date,
        p.period,
        p.reporter_code,
        p.partner_code,
        p.cmd_code,
        p.flow_code,
        p.classification_code,
        p.hs_version,
        p.freq_code,
        p.partner2_code,
        p.customs_code,
        p.mot_code,
        p.is_original_classification,

        p.primary_value as partner_import_value,

        s.world_total_value,
        s.partner_total_value,
        s.observed_partner_count,
        s.negative_value_count,
        s.partner_coverage_ratio,
        s.reconciliation_status,
        s.source_assurance_level,

        p.batch_id,
        p.source_query_key,
        p.revision_id,
        p.ingested_at

    from partner_records as p

    left join scope_totals as s
      on p.period_start_date = s.period_start_date
     and p.period = s.period
     and p.reporter_code = s.reporter_code
     and p.cmd_code = s.cmd_code
     and p.flow_code = s.flow_code
     and p.classification_code = s.classification_code
     and p.hs_version is not distinct from s.hs_version
     and p.freq_code = s.freq_code
     and p.partner2_code = s.partner2_code
     and p.customs_code = s.customs_code
     and p.mot_code = s.mot_code
     and p.is_original_classification =
         s.is_original_classification

),


calculated as (

    select
        *,

        case
            when partner_import_value >= 0
             and negative_value_count = 0
             and world_total_value > 0
             and partner_total_value >= 0
             and reconciliation_status not in (
                 'INVALID_SOURCE_VALUES',
                 'PARTNER_EXCEEDS_WORLD'
             )
            then safe_divide(
                partner_import_value,
                world_total_value
            )
            else null
        end as share_of_world,

        case
            when partner_import_value >= 0
             and negative_value_count = 0
             and partner_total_value > 0
            then safe_divide(
                partner_import_value,
                partner_total_value
            )
            else null
        end as share_of_reported_partners

    from enriched

)


select *
from calculated
