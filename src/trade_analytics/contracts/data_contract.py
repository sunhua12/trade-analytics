
import json
from decimal import Decimal
from typing import Any

from trade_analytics.ingestion.batch import BatchResult
from trade_analytics.warehouse.bigquery_schema import LANDING_SCHEMA
from trade_analytics.ingestion.query import QueryType


BIGNUMERIC_MAX = Decimal("5.7896044618658097711785492504343953926634992332820282019728792003956564819967E+38")


def decimal_to_text(value: Any, field_name: str, nullable: bool = False) -> str | None:
    """將數值轉成十進位文字，拒絕無法精確儲存於 BIGNUMERIC 的值。"""
    if value is None:
        if nullable:
            return None
        raise ValueError(f"{field_name} 不可為 None")

    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise TypeError(f"{field_name} 必須是 int 或 Decimal")

    decimal_value = Decimal(value)

    if not decimal_value.is_finite():
        raise ValueError(f"{field_name} 不可為 NaN 或 Infinity")

    if decimal_value.is_zero():
        return "0"

    if abs(decimal_value) > BIGNUMERIC_MAX:
        raise ValueError(f"{field_name} 超出 BigQuery BIGNUMERIC 範圍")

    check_exponent(decimal_value, field_name)

    return format(decimal_value, "f")


def check_exponent(decimal_value: Decimal, field_name: str):
    decimal_tuple = decimal_value.as_tuple()
    digits = list(decimal_tuple.digits)
    exponent = decimal_tuple.exponent

    while exponent < 0 and digits and digits[-1] == 0:
        digits.pop()
        exponent += 1

    if exponent < -38:
        raise ValueError(f"{field_name} 超過 BIGNUMERIC 的 38 位小數限制")


def serialize_source_record(record: dict[str, Any]) -> str:
    """序列化來源紀錄，將 Decimal 保留為 JSON 字串。"""
    def serialize_value(value: Any) -> str:
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"不支援的 JSON 型別：{type(value).__name__}")

    return json.dumps(record, default=serialize_value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def validate_query_scope(record: dict[str, Any], result: BatchResult) -> None:
    """確認來源紀錄符合當前 Query 的查詢範圍。"""
    query = result.query

    if record["period"] != query.period:
        raise ValueError("Record period 與 Query 不一致")

    if record["reporterCode"] != query.reporter_code:
        raise ValueError("Record reporterCode 與 Query 不一致")

    if record["cmdCode"] != query.cmd_code:
        raise ValueError("Record cmdCode 與 Query 不一致")

    if query.query_type == QueryType.WORLD_TOTAL and record["partnerCode"] != 0:
        raise ValueError("World Total 的 partnerCode 必須為 0")

    if query.query_type == QueryType.PARTNER_DETAIL and record["partnerCode"] == 0:
        raise ValueError("Partner Detail 不應包含 World Total")


def validate_landing_record(record: dict[str, Any]) -> None:
    """確認 Landing Record 的欄位與必要值符合 BigQuery Schema。"""
    schema_fields = {field.name: field for field in LANDING_SCHEMA}

    missing = set(schema_fields) - set(record)
    unexpected = set(record) - set(schema_fields)

    if missing:
        raise ValueError(f"Landing Record 缺少欄位：{sorted(missing)}")

    if unexpected:
        raise ValueError(f"Landing Record 包含未定義欄位：{sorted(unexpected)}")

    for name, field in schema_fields.items():
        if field.mode == "REQUIRED" and record[name] is None:
            raise ValueError(f"Landing 必要欄位不可為 None：{name}")


def to_landing_record(record: dict[str, Any], result: BatchResult, batch_id: str, source_query_key: str, row_index: int) -> dict[str, Any]:
    """將單筆來源紀錄轉換為 BigQuery Landing Record。"""
    if not batch_id or not source_query_key:
        raise ValueError("batch_id 與 source_query_key 不可為空")

    if row_index < 0:
        raise ValueError("source_row_index 不可為負數")

    validate_query_scope(record, result)

    landing_record = {
        "batch_id": batch_id,
        "source_query_key": source_query_key,
        "query_type": result.query.query_type.value,
        "source_row_index": row_index,
        "period": record["period"],
        "reporter_code": record["reporterCode"],
        "partner_code": record["partnerCode"],
        "cmd_code": record["cmdCode"],
        "primary_value_text": decimal_to_text(record["primaryValue"], "primaryValue"),
        "flow_code": record["flowCode"],
        "classification_code": record["classificationCode"],
        "hs_version": None,
        "freq_code": record["freqCode"],
        "partner2_code": record["partner2Code"],
        "customs_code": record["customsCode"],
        "mot_code": record["motCode"],
        "is_original_classification": record["isOriginalClassification"],
        "net_weight_text": decimal_to_text(record.get("netWgt"), "netWgt", nullable=True),
        "quantity_text": decimal_to_text(record.get("qty"), "qty", nullable=True),
        "source_record_json": serialize_source_record(record),
    }

    validate_landing_record(landing_record)
    return landing_record


def to_landing_records(result: BatchResult, batch_id: str, source_query_key: str) -> list[dict[str, Any]]:
    """將整組 BatchResult 轉換為 Landing Records。"""
    return [
        to_landing_record(record, result, batch_id, source_query_key, index)
        for index, record in enumerate(result.records)
    ]
