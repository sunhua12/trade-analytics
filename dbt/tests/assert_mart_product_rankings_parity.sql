
with source_records as (

    select
        trade_observation_id,
        period_start_date,
        reporter_code,
        commodity_key,
        import_value

    from {{ ref('fct_trade_imports') }}

    where query_type = 'world_total'
      and partner_code = 0

),

mart_records as (

    select
        trade_observation_id,
        period_start_date,
        reporter_code,
        commodity_key,
        import_value,
        commodity_rank,
        observed_commodity_count

    from {{ ref('mart_product_rankings') }}

),

compared as (

    select
        coalesce(
            s.trade_observation_id,
            m.trade_observation_id
        ) as trade_observation_id,

        case
            when s.trade_observation_id is null
                then 'EXTRA_MART_RECORD'

            when m.trade_observation_id is null
                then 'MISSING_MART_RECORD'

            when s.import_value is distinct from m.import_value
                then 'VALUE_MISMATCH'

            when s.period_start_date is distinct from
                 m.period_start_date
              or s.reporter_code is distinct from m.reporter_code
              or s.commodity_key is distinct from m.commodity_key
                then 'DIMENSION_MISMATCH'

            when m.observed_commodity_count < 1
                then 'INVALID_OBSERVED_COUNT'

            when s.import_value >= 0
             and (
                m.commodity_rank is null
                or m.commodity_rank < 1
             )
                then 'INVALID_RANK'

            else null
        end as failure_reason

    from source_records as s

    full outer join mart_records as m
      on s.trade_observation_id = m.trade_observation_id

)

select *
from compared
where failure_reason is not null
