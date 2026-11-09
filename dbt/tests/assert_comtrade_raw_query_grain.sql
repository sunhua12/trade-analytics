
with raw_records as (

    select
        batch_id,
        source_query_key,
        source_row_index,
        period_start_date,
        period,
        query_type,
        partner_code

    from {{ source('comtrade_raw', 'comtrade_records') }}

),

invalid_records as (

    select
        'invalid_query_grain' as failure_reason,
        batch_id,
        source_query_key,
        source_row_index

    from raw_records

    where (
        query_type = 'world_total'
        and partner_code != 0
    )
    or (
        query_type = 'partner_detail'
        and partner_code = 0
    )
    or (
        period_start_date !=
        safe.parse_date('%Y%m', period)
    )
    or (
        safe.parse_date('%Y%m', period) is null
    )
    or source_row_index < 0

),

duplicate_row_index as (

    select
        'duplicate_source_row_index' as failure_reason,
        batch_id,
        source_query_key,
        source_row_index

    from raw_records

    group by
        batch_id,
        source_query_key,
        source_row_index

    having count(*) > 1

)

select *
from invalid_records

union all

select *
from duplicate_row_index
