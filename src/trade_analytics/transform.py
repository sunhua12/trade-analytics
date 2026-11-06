from decimal import Decimal

# 將 UN Comtrade API 的原始欄位名稱對應到標準化欄位名稱
FIELD_MAPPING = {
    "period": "period",
    "reporterCode": "reporter_code",
    "partnerCode": "partner_code",
    "flowCode": "flow_code",
    "cmdCode": "cmd_code",
    "classificationCode": "hs_version",
    "primaryValue": "primary_value",
}


def transform_record(record: dict) -> dict:
    # 根據 FIELD_MAPPING 建立新字典，接著將原始欄位的值，改成對應的標準化欄位名稱
    transformed = {target_field: record[source_field] for source_field, target_field in FIELD_MAPPING.items()}

    # 將金額統一轉換為 Decimal，確保後續計算的數值精度
    transformed["primary_value"] = Decimal(str(transformed["primary_value"]))

    return transformed


def transform_trade_data(trade_data: dict) -> dict:
    world_records = [transform_record(record) for record in trade_data["world"]]
    partner_records = [transform_record(record) for record in trade_data["partners"]]
    return {"world": world_records, "partners": partner_records, }
