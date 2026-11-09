
{{
    config(
        materialized='view',
        tags=['intermediate', 'scope_quality']
    )
}}

with reconciliation as (

    select *
    from {{ ref('int_comtrade_scope_reconciliation') }}

),

concentration as (

    select *
    from {{ ref('int_comtrade_concentration') }}

),

joined as (

    select
        r.period_start_date,
        r.period,
        r.reporter_code,
        r.cmd_code,
        r.flow_code,
        r.classification_code,
        r.hs_version,
        r.freq_code,
        r.partner2_code,
        r.customs_code,
        r.mot_code,
        r.is_original_classification,

        r.world_record_count,
        r.partner_record_count,
        r.observed_partner_count,
        r.negative_value_count,

        r.world_total_value,
        r.partner_total_value,
        r.reconciliation_gap,
        r.partner_coverage_ratio,
        r.reconciliation_status,
        r.is_value_reconciled,
        r.source_assurance_level,

        c.hhi_reported_partners,
        c.effective_partner_count,
        coalesce(
            c.hhi_status,
            'NOT_CALCULABLE'
        ) as hhi_status

    from reconciliation as r

    left join concentration as c
      on r.period_start_date = c.period_start_date
     and r.period = c.period
     and r.reporter_code = c.reporter_code
     and r.cmd_code = c.cmd_code
     and r.flow_code = c.flow_code
     and r.classification_code = c.classification_code
     and r.hs_version is not distinct from c.hs_version
     and r.freq_code = c.freq_code
     and r.partner2_code = c.partner2_code
     and r.customs_code = c.customs_code
     and r.mot_code = c.mot_code
     and r.is_original_classification =
         c.is_original_classification

)

select
    *,

    (
        world_record_count = 1
        and partner_record_count > 0
        and negative_value_count = 0
        and world_total_value > 0
        and partner_total_value > 0
        and reconciliation_status in (
            'MATCHED_VALUE',
            'UNRECONCILED'
        )
    ) as has_usable_world_comparison,

    (
        hhi_reported_partners is not null
        and hhi_status = 'PROVISIONAL_BOUNDED_PREVIEW'
    ) as has_observed_concentration

from joined
