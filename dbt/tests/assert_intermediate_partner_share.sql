
with records as (

    select *
    from {{ ref('int_comtrade_partner_share') }}

)

select
    period,
    reporter_code,
    partner_code,
    cmd_code,
    partner_import_value,
    world_total_value,
    partner_total_value,
    share_of_world,
    share_of_reported_partners,
    reconciliation_status

from records

where
    (
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

    or (
        share_of_world is not null
        and (
            world_total_value is null
            or world_total_value <= 0
        )
    )

    or (
        share_of_reported_partners is not null
        and (
            partner_total_value is null
            or partner_total_value <= 0
        )
    )
