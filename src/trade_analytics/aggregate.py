from _typeshed import SupportsAdd
from decimal import Decimal
from typing import Any


def aggregate_trade_data(trade_data: dict) -> dict:
    partner_records = trade_data["partners"]
    world_record = trade_data["world"][0]

    world_value = calc_world_value(world_record)
    partner_summary = calc_partner_summary(partner_records, world_value)
    partner_total = calc_partner_total(partner_records)

    return {
        "period": world_record["period"],
        "reporter_code": world_record["reporter_code"],
        "flow_code": world_record["flow_code"],
        "cmd_code": world_record["cmd_code"],
        "hs_version": world_record["hs_version"],
        "world_value": world_value,
        "partner_count": len(partner_records),
        "partner_total": partner_total,
        "partners": partner_summary,
    }


def calc_partner_total(partner_records) -> Any | Decimal:
    # Partner 金額合計僅作為統計資訊，不取代 World 總額
    partner_total = sum(
        (record["primary_value"] for record in partner_records),
        Decimal("0"),
    )
    return partner_total


def calc_world_value(world_record) -> Any:
    world_value = world_record["primary_value"]
    if world_value < 0:
        raise ValueError("World value must not be negative")
    return world_value


def calc_partner_summary(partner_records, world_value) -> list[Any]:
    partner_summary = []
    sorted_partners = sorted(partner_records, key=lambda record: record["primary_value"], reverse=True, )
    for rank, record in enumerate(sorted_partners, start=1):
        partner_value = record["primary_value"]
        market_share = calc_market_share(partner_value, world_value)

        partner_summary.append({
            "rank": rank,
            "partner_code": record["partner_code"],
            "primary_value": partner_value,
            "market_share_pct": market_share,
        })
    return partner_summary


def calc_market_share(partner_value, world_value) -> Any:
    if world_value == 0:
        market_share = None
    else:
        market_share = (partner_value / world_value) * Decimal("100")
    return market_share
