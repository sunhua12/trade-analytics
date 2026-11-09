
with records as (

    select
        period,
        reporter_code,
        cmd_code,
        world_record_count,
        partner_record_count,
        partner_total_value,
        hhi_reported_partners,
        effective_partner_count,
        hhi_status,
        concentration_quality_status

    from {{ ref('mart_trade_concentration') }}

)

select *
from records

where
    (
        hhi_reported_partners is not null
        and (
            hhi_reported_partners <= 0
            or hhi_reported_partners > 10000
        )
    )

    or (
        partner_record_count = 0
        and hhi_reported_partners is not null
    )

    or (
        hhi_reported_partners is not null
        and (
            partner_total_value is null
            or partner_total_value <= 0
        )
    )

    or (
        hhi_status = 'NOT_CALCULABLE'
        and hhi_reported_partners is not null
    )

    or (
        hhi_status = 'PROVISIONAL_BOUNDED_PREVIEW'
        and hhi_reported_partners is null
    )

    or (
        hhi_reported_partners is null
        and effective_partner_count is not null
    )

    or (
        concentration_quality_status =
            'MISSING_PARTNER_DETAIL'
        and partner_record_count != 0
    )

    or (
        concentration_quality_status =
            'MISSING_WORLD_TOTAL'
        and world_record_count != 0
    )
