
with staging_records as (

    select
        query_type,
        primary_value as import_value

    from {{ ref('stg_comtrade_world_total') }}

    union all

    select
        query_type,
        primary_value as import_value

    from {{ ref('stg_comtrade_partner_detail') }}

),

source_summary as (

    select
        query_type,
        count(*) as record_count,
        sum(import_value) as total_value

    from staging_records

    group by query_type

),

fact_summary as (

    select
        query_type,
        count(*) as record_count,
        sum(import_value) as total_value

    from {{ ref('fct_trade_imports') }}

    group by query_type

)

select
    coalesce(s.query_type, f.query_type) as query_type,

    s.record_count as source_record_count,
    f.record_count as fact_record_count,

    s.total_value as source_total_value,
    f.total_value as fact_total_value

from source_summary as s

full outer join fact_summary as f
    on s.query_type = f.query_type

where
    s.record_count is distinct from f.record_count
    or s.total_value is distinct from f.total_value
