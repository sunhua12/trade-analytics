
{{
    config(
        materialized='view',
        tags=['intermediate', 'concentration']
    )
}}

with partner_shares as (

    select *
    from {{ ref('int_comtrade_partner_share') }}

),

scope_concentration as (

    select
        period_start_date,
        period,
        reporter_code,
        cmd_code,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,

        count(*) as partner_record_count,
        count(distinct partner_code) as observed_partner_count,

        max(partner_total_value) as partner_total_value,
        max(world_total_value) as world_total_value,
        max(partner_coverage_ratio) as partner_coverage_ratio,
        max(reconciliation_status) as reconciliation_status,
        max(source_assurance_level) as source_assurance_level,

        sum(
            safe_multiply(
                share_of_reported_partners,
                share_of_reported_partners
            )
        ) as sum_squared_partner_share

    from partner_shares

    group by
        period_start_date,
        period,
        reporter_code,
        cmd_code,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification

),

calculated as (

    select
        *,

        case
            when partner_total_value > 0
             and reconciliation_status != 'INVALID_SOURCE_VALUES'
            then safe_multiply(
                sum_squared_partner_share,
                cast(10000 as bignumeric)
            )
            else null
        end as hhi_reported_partners

    from scope_concentration

)

select
    *,

    safe_divide(
        cast(10000 as bignumeric),
        hhi_reported_partners
    ) as effective_partner_count,

    case
        when hhi_reported_partners is null
            then 'NOT_CALCULABLE'
        else 'PROVISIONAL_BOUNDED_PREVIEW'
    end as hhi_status

from calculated
