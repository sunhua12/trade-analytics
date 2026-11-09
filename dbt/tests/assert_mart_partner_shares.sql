
with source_summary as (

    select
        source_query_key,
        partner_code,
        count(*) as record_count,
        sum(partner_import_value) as total_value

    from {{ ref('int_comtrade_partner_share') }}

    group by
        source_query_key,
        partner_code

),

mart_summary as (

    select
        source_query_key,
        partner_code,
        count(*) as record_count,
        sum(partner_import_value) as total_value

    from {{ ref('mart_partner_shares') }}

    group by
        source_query_key,
        partner_code

),

parity_failures as (

    select
        'SOURCE_MART_PARITY' as failure_reason,

        coalesce(
            s.source_query_key,
            m.source_query_key
        ) as scope_key,

        coalesce(
            s.partner_code,
            m.partner_code
        ) as partner_code

    from source_summary as s

    full outer join mart_summary as m
      on s.source_query_key = m.source_query_key
     and s.partner_code = m.partner_code

    where s.record_count is distinct from m.record_count
       or s.total_value is distinct from m.total_value

),

invalid_shares as (

    select
        'INVALID_SHARE_RANGE' as failure_reason,
        source_query_key as scope_key,
        partner_code

    from {{ ref('mart_partner_shares') }}

    where (
        share_of_world is not null
        and (
            share_of_world < 0
            or share_of_world > 1
        )
    )
    or (
        share_of_reported_partners is not null
        and (
            share_of_reported_partners < 0
            or share_of_reported_partners > 1
        )
    )

),

invalid_share_sums as (

    select
        'INVALID_REPORTED_SHARE_SUM' as failure_reason,
        source_query_key as scope_key,
        cast(null as int64) as partner_code

    from {{ ref('mart_partner_shares') }}

    group by source_query_key

    having
        countif(
            share_of_reported_partners is null
        ) = 0

        and min(partner_total_value) > 0

        and abs(
            sum(share_of_reported_partners)
            - cast(1 as bignumeric)
        ) > cast(0.000000000001 as bignumeric)

)

select *
from parity_failures

union all

select *
from invalid_shares

union all

select *
from invalid_share_sums
