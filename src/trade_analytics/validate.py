import logging

from typing import Any


logger = logging.getLogger(__name__)


def validate_trade_data(trade_data: dict, params: dict) -> None:
    validate_data_structure(trade_data)
    validate_world_data(params, trade_data)
    validate_partner_data(params, trade_data)
    logger.debug("業務資料驗證通過：period=%s reporter=%s cmd=%s",
                 params["period"], params["reporterCode"], params["cmdCode"])


def validate_partner_data(params: dict[Any, Any], trade_data: dict[Any, Any]) -> None:
    partner_records = trade_data["partners"]

    # 驗證 Partner 資料結構
    if not isinstance(partner_records, list):
        raise ValueError("partners 必須是 list")

    # 驗證 Partner 筆數
    if not partner_records:
        raise ValueError("Partner 資料不可為空")

    # 驗證 Partner 資料及業務鍵唯一性
    seen_keys = set()

    for record in partner_records:
        validate_record(record, params)

        if record["partnerCode"] == 0:
            raise ValueError("Partner 資料不可包含 World")

        business_key = (
            record["period"],
            record["reporterCode"],
            record["partnerCode"],
            record["flowCode"],
            record["cmdCode"],
        )

        if business_key in seen_keys:
            raise ValueError(f"發現重複的 Partner 業務鍵：{business_key}")

        seen_keys.add(business_key)


def validate_world_data(params: dict[Any, Any], trade_data: dict[Any, Any]) -> None:
    world_records = trade_data["world"]

    # 驗證 World 資料結構
    if not isinstance(world_records, list):
        raise ValueError("world 必須是 list")

    # 驗證 World 筆數
    if len(world_records) != 1:
        raise ValueError("World 資料必須剛好有一筆")

    # 驗證 World 資料
    for record in world_records:
        validate_record(record, params)

        if record["partnerCode"] != 0:
            raise ValueError("World 的 partnerCode 必須為 0")


def validate_data_structure(trade_data: dict[Any, Any]) -> None:
    if not isinstance(trade_data, dict):
        raise ValueError("trade_data 必須是 dict")

    if set(trade_data) != {"world", "partners"}:
        raise ValueError("trade_data 必須包含 world 和 partners 兩個欄位")


def validate_record(record: dict, params: dict) -> None:
    # 驗證 API 回傳資料與查詢條件一致
    expected_values = {"period": params["period"], "reporterCode": int(params["reporterCode"]), "flowCode": params["flowCode"], "cmdCode": params["cmdCode"], "freqCode": "M", "classificationCode": "H6", "partner2Code": 0, "customsCode": "C00", "motCode": 0, "isOriginalClassification": True}

    for field, expected in expected_values.items():
        actual = record[field]

        if type(actual) is not type(expected) or actual != expected:
            raise ValueError(f"{field} 不符合預期：預期 {expected!r}，實際 {actual!r}")
