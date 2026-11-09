
with staging_records as (

    select *
    from {{ ref('stg_comtrade_world_total') }}

    union all

    select *
    from {{ ref('stg_comtrade_partner_detail') }}

),

expected_records as (

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

        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        primary_value as import_value,
        batch_id,
        source_query_key,
        source_row_index,
        revision_id

    from staging_records

),

fact_records as (

    select
        trade_observation_id,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        query_type,
        import_value,
        batch_id,
        source_query_key,
        source_row_index,
        revision_id

    from {{ ref('fct_trade_imports') }}

),

compared as (

    select
        coalesce(
            s.trade_observation_id,
            f.trade_observation_id
        ) as trade_observation_id,

        case
            when s.trade_observation_id is null
                then 'MISSING_IN_STAGING'

            when f.trade_observation_id is null
                then 'MISSING_IN_FACT'

            when s.import_value is distinct from f.import_value
                then 'VALUE_MISMATCH'

            when s.batch_id is distinct from f.batch_id
              or s.source_query_key is distinct from f.source_query_key
              or s.source_row_index is distinct from f.source_row_index
              or s.revision_id is distinct from f.revision_id
                then 'LINEAGE_MISMATCH'

            else null
        end as failure_reason

    from expected_records as s

    full outer join fact_records as f
      on s.trade_observation_id = f.trade_observation_id

)

select *
from compared
where failure_reason is not null
