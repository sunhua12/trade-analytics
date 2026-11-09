
with records as (

    select *
    from {{ ref('int_comtrade_scope_reconciliation') }}

)

select
    period,
    reporter_code,
    cmd_code,
    reconciliation_status,
    world_total_value,
    partner_total_value,
    reconciliation_gap,
    partner_coverage_ratio

from records

where
    (
        world_total_value is not null
        and partner_total_value is not null
        and (
            reconciliation_gap is null
            or reconciliation_gap !=
                world_total_value - partner_total_value
        )
    )

    or (
        (world_total_value is null or partner_total_value is null)
        and reconciliation_gap is not null
    )

    or (
        reconciliation_status = 'MATCHED_VALUE'
        and (
            reconciliation_gap is null
            or reconciliation_gap != 0
        )
    )

    or (
        world_total_value = 0
        and partner_coverage_ratio is not null
    )

    or (
        reconciliation_status = 'WORLD_ONLY'
        and partner_record_count != 0
    )

    or (
        reconciliation_status = 'PARTNER_ONLY'
        and world_record_count != 0
    )
