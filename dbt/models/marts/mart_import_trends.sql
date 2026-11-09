
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
        tags=['marts', 'import_trends']
    )
}}

with world_totals as (

    select
        trade_observation_id,
        period_key,
        period_start_date,
        period,
        reporter_code,
        commodity_key,
        cmd_code,
        classification_code,
        hs_version,
        flow_code,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        import_value,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at

    from {{ ref('fct_trade_imports') }}

    where query_type = 'world_total'
      and partner_code = 0

),

compared as (

    select
        curr.*,

        prior_month.trade_observation_id
            as previous_month_observation_id,

        prior_month.import_value
            as previous_month_value,

        prior_year.trade_observation_id
            as previous_year_observation_id,

        prior_year.import_value
            as previous_year_value

    from world_totals as curr

    left join world_totals as prior_month
      on prior_month.period_start_date =
         date_sub(
             curr.period_start_date,
             interval 1 month
         )
     and prior_month.reporter_code = curr.reporter_code
     and prior_month.cmd_code = curr.cmd_code
     and prior_month.classification_code =
         curr.classification_code
     and prior_month.hs_version is not distinct from
         curr.hs_version
     and prior_month.flow_code = curr.flow_code
     and prior_month.freq_code = curr.freq_code
     and prior_month.partner2_code = curr.partner2_code
     and prior_month.customs_code = curr.customs_code
     and prior_month.mot_code = curr.mot_code
     and prior_month.is_original_classification =
         curr.is_original_classification

    left join world_totals as prior_year
      on prior_year.period_start_date =
         date_sub(
             curr.period_start_date,
             interval 1 year
         )
     and prior_year.reporter_code = curr.reporter_code
     and prior_year.cmd_code = curr.cmd_code
     and prior_year.classification_code =
         curr.classification_code
     and prior_year.hs_version is not distinct from
         curr.hs_version
     and prior_year.flow_code = curr.flow_code
     and prior_year.freq_code = curr.freq_code
     and prior_year.partner2_code = curr.partner2_code
     and prior_year.customs_code = curr.customs_code
     and prior_year.mot_code = curr.mot_code
     and prior_year.is_original_classification =
         curr.is_original_classification

),

metrics as (

    select
        *,

        case
            when previous_month_observation_id is null
                then 'NO_PRIOR_MONTH'

            when import_value < 0
              or previous_month_value < 0
                then 'NEGATIVE_VALUE'

            when previous_month_value = 0
                then 'ZERO_BASELINE'

            else 'COMPARABLE'
        end as mom_status,

        case
            when previous_year_observation_id is null
                then 'NO_PRIOR_YEAR'

            when import_value < 0
              or previous_year_value < 0
                then 'NEGATIVE_VALUE'

            when previous_year_value = 0
                then 'ZERO_BASELINE'

            else 'COMPARABLE'
        end as yoy_status

    from compared

)

select
    trade_observation_id,
    period_key,
    period_start_date,
    period,
    reporter_code,
    commodity_key,
    cmd_code,
    classification_code,
    hs_version,
    flow_code,
    freq_code,
    partner2_code,
    customs_code,
    mot_code,
    is_original_classification,

    import_value,
    previous_month_value,
    previous_year_value,

    case
        when mom_status = 'COMPARABLE'
            then import_value - previous_month_value
        else null
    end as mom_absolute_change,

    case
        when mom_status = 'COMPARABLE'
            then safe_divide(
                import_value - previous_month_value,
                previous_month_value
            )
        else null
    end as mom_growth_rate,

    case
        when yoy_status = 'COMPARABLE'
            then import_value - previous_year_value
        else null
    end as yoy_absolute_change,

    case
        when yoy_status = 'COMPARABLE'
            then safe_divide(
                import_value - previous_year_value,
                previous_year_value
            )
        else null
    end as yoy_growth_rate,

    mom_status,
    yoy_status,

    batch_id,
    source_query_key,
    revision_id,
    ingested_at,

    'BOUNDED_PREVIEW' as source_assurance_level

from metrics
