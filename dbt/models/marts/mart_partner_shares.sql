
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
            'partner_code'
        ],
        tags=['marts', 'partner_shares']
    )
}}

with partner_shares as (

    select *
    from {{ ref('int_comtrade_partner_share') }}

),

enriched as (

    select
        p.period_start_date,
        d.period_key,
        d.year_number,
        d.month_number,

        p.period,
        p.reporter_code,
        p.partner_code,
        partner.partner_type,

        commodity.commodity_key,
        p.cmd_code,
        p.classification_code,
        p.hs_version,
        p.flow_code,
        p.freq_code,
        p.partner2_code,
        p.customs_code,
        p.mot_code,
        p.is_original_classification,

        p.partner_import_value,
        p.world_total_value,
        p.partner_total_value,
        p.observed_partner_count,
        p.partner_coverage_ratio,

        p.share_of_world,
        p.share_of_reported_partners,

        p.reconciliation_status,
        p.source_assurance_level,

        p.batch_id,
        p.source_query_key,
        p.revision_id,
        p.ingested_at

    from partner_shares as p

    inner join {{ ref('dim_period') }} as d
        on p.period_start_date = d.period_start_date

    inner join {{ ref('dim_partner') }} as partner
        on p.partner_code = partner.partner_code

    inner join {{ ref('dim_commodity') }} as commodity
        on p.cmd_code = commodity.cmd_code
       and p.classification_code =
           commodity.classification_code
       and p.hs_version is not distinct from
           commodity.hs_version

),

ranked as (

    select
        *,

        case
            when share_of_reported_partners is not null
            then dense_rank() over (
                partition by
                    period_start_date,
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
                order by partner_import_value desc
            )
            else null
        end as partner_rank

    from enriched

)

select
    *,

    case
        when reconciliation_status = 'PARTNER_ONLY'
            then 'NO_WORLD_TOTAL'

        when reconciliation_status = 'INVALID_SOURCE_VALUES'
            then 'INVALID_SOURCE_VALUES'

        when reconciliation_status = 'PARTNER_EXCEEDS_WORLD'
            then 'PARTNER_EXCEEDS_WORLD'

        when world_total_value = 0
            then 'ZERO_WORLD_TOTAL'

        when share_of_world is not null
            then 'PROVISIONAL_COMPARISON'

        else 'NOT_COMPARABLE'
    end as share_quality_status

from ranked
