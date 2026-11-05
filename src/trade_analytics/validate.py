from decimal import Decimal, InvalidOperation
from typing import Any

REQUIRED_FIELDS = {
    "period",
    "reporterCode",
    "partnerCode",
    "flowCode",
    "cmdCode",
    "primaryValue",
    "classificationCode",
    "freqCode",
    "partner2Code",
    "customsCode",
    "motCode",
    "isOriginalClassification",
}


def validate_trade_data(trade_data: dict, params: dict, ) -> None:
    validate_data_structure(trade_data)
    validate_world_data(params, trade_data)
    validate_partner_data(params, trade_data)


def validate_partner_data(params: dict[Any, Any], trade_data: dict[Any, Any]):
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


def validate_world_data(params: dict[Any, Any], trade_data: dict[Any, Any]):
    world_records = trade_data["world"]

    # 驗證 World 結構
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


def validate_data_structure(trade_data: dict[Any, Any]):
    if not isinstance(trade_data, dict):
        raise ValueError("trade_data 必須是 dict")

    if set(trade_data) != {"world", "partners"}:
        raise ValueError("trade_data 必須包含 world 和 partners 兩個欄位")


def validate_record(record: dict, params: dict, ) -> None:
    # 檢查資料型別
    if not isinstance(record, dict):
        raise ValueError("每筆資料必須是 dict")

    # 檢查必要欄位
    missing_fields = REQUIRED_FIELDS - record.keys()

    if missing_fields:
        raise ValueError(f"缺少必要欄位：{sorted(missing_fields)}")

    # 檢查查詢條件是否一致
    expected_values = {
        "period": params["period"],
        "reporterCode": int(params["reporterCode"]),
        "flowCode": params["flowCode"],
        "cmdCode": params["cmdCode"],
        "freqCode": "M",
        "classificationCode": "H6",
        "partner2Code": 0,
        "customsCode": "C00",
        "motCode": 0,
        "isOriginalClassification": True,
    }

    for field, expected in expected_values.items():
        actual = record[field]

        if type(actual) is not type(expected) or actual != expected:
            raise ValueError(f"{field} 不符合預期：預期 {expected!r}，實際 {actual!r}")

    # 驗證 Partner Code
    partner_code = record["partnerCode"]

    if type(partner_code) is not int or partner_code < 0:
        raise ValueError("partnerCode 必須是非負整數")

    # 驗證進口金額
    validate_primary_value(record["primaryValue"])


def validate_primary_value(value: object) -> None:
    if isinstance(value, bool):
        raise ValueError("primaryValue 不可為 bool")

    if not isinstance(value, (int, float, Decimal)):
        raise ValueError("primaryValue 必須是數字")

    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("primaryValue 無法轉換為 Decimal") from exc

    if not amount.is_finite():
        raise ValueError("primaryValue 不可為 NaN 或 Infinity")

    if amount < 0:
        raise ValueError("primaryValue 不可為負數")
