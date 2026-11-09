
with records as (

    select *
    from {{ ref('int_comtrade_concentration') }}

)

select
    period,
    reporter_code,
    cmd_code,
    observed_partner_count,
    partner_total_value,
    hhi_reported_partners,
    effective_partner_count,
    hhi_status

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
        hhi_reported_partners is not null
        and effective_partner_count is null
    )
