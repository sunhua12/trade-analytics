
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
            'classification_code',
            'cmd_code'
        ],
        tags=['marts', 'product_rankings']
    )
}}

with world_totals as (

    select
        f.trade_observation_id,
        f.period_key,
        f.period_start_date,
        f.period,
        f.reporter_code,

        f.commodity_key,
        f.cmd_code,
        c.commodity_code_length,

        f.classification_code,
        f.hs_version,
        f.flow_code,
        f.freq_code,
        f.partner2_code,
        f.customs_code,
        f.mot_code,
        f.is_original_classification,

        f.import_value,
        f.batch_id,
        f.source_query_key,
        f.revision_id,
        f.ingested_at

    from {{ ref('fct_trade_imports') }} as f

    inner join {{ ref('dim_commodity') }} as c
      on f.commodity_key = c.commodity_key

    where f.query_type = 'world_total'
      and f.partner_code = 0

),

ranked as (

    select
        *,

        count(*) over (
            partition by
                period_start_date,
                reporter_code,
                flow_code,
                classification_code,
                hs_version,
                freq_code,
                partner2_code,
                customs_code,
                mot_code,
                is_original_classification,
                commodity_code_length
        ) as observed_commodity_count,

        case
            when import_value >= 0
            then dense_rank() over (
                partition by
                    period_start_date,
                    reporter_code,
                    flow_code,
                    classification_code,
                    hs_version,
                    freq_code,
                    partner2_code,
                    customs_code,
                    mot_code,
                    is_original_classification,
                    commodity_code_length
                order by import_value desc
            )
            else null
        end as commodity_rank

    from world_totals

)

select
    *,

    case
        when import_value < 0
            then 'INVALID_NEGATIVE_VALUE'

        when observed_commodity_count = 1
            then 'SINGLE_OBSERVED_COMMODITY'

        else 'OBSERVED_COMMODITIES_ONLY'
    end as ranking_status,

    'BOUNDED_PREVIEW' as source_assurance_level

from ranked
