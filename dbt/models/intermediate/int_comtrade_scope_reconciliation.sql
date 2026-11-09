
{{
    config(
        materialized='view',
        tags=['intermediate', 'reconciliation']
    )
}}

with source_records as (

    select
        period_start_date,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        primary_value,
        'world_total' as record_type

    from {{ ref('stg_comtrade_world_total') }}

    union all

    select
        period_start_date,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        primary_value,
        'partner_detail' as record_type

    from {{ ref('stg_comtrade_partner_detail') }}

),

scope_totals as (

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

        countif(record_type = 'world_total')
            as world_record_count,

        countif(record_type = 'partner_detail')
            as partner_record_count,

        count(distinct if(
            record_type = 'partner_detail',
            partner_code,
            null
        )) as observed_partner_count,

        countif(primary_value < 0)
            as negative_value_count,

        if(
            countif(record_type = 'world_total') = 0,
            null,
            sum(
                if(
                    record_type = 'world_total',
                    primary_value,
                    cast(0 as bignumeric)
                )
            )
        ) as world_total_value,

        if(
            countif(record_type = 'partner_detail') = 0,
            null,
            sum(
                if(
                    record_type = 'partner_detail',
                    primary_value,
                    cast(0 as bignumeric)
                )
            )
        ) as partner_total_value

    from source_records

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

reconciled as (

    select
        *,

        case
            when world_total_value is not null
             and partner_total_value is not null
            then world_total_value - partner_total_value
            else null
        end as reconciliation_gap,

        case
            when world_total_value > 0
             and partner_total_value >= 0
             and negative_value_count = 0
            then safe_divide(
                partner_total_value,
                world_total_value
            )
            else null
        end as partner_coverage_ratio,

        case
            when world_record_count = 0
                then 'PARTNER_ONLY'

            when partner_record_count = 0
                then 'WORLD_ONLY'

            when world_record_count != 1
              or negative_value_count > 0
                then 'INVALID_SOURCE_VALUES'

            when world_total_value = 0
                then 'ZERO_WORLD_VALUE'

            when partner_total_value = 0
                then 'ZERO_PARTNER_VALUE'

            when partner_total_value = world_total_value
                then 'MATCHED_VALUE'

            when partner_total_value > world_total_value
                then 'PARTNER_EXCEEDS_WORLD'

            else 'UNRECONCILED'
        end as reconciliation_status

    from scope_totals

)

select
    *,

    case
        when reconciliation_status = 'MATCHED_VALUE'
            then true

        when reconciliation_status in (
            'UNRECONCILED',
            'PARTNER_EXCEEDS_WORLD'
        )
            then false

        else null
    end as is_value_reconciled,

    'BOUNDED_PREVIEW' as source_assurance_level

from reconciled
