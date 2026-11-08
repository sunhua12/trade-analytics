
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from trade_analytics.contracts import data_contract, query_identity
from trade_analytics.ingestion import query
from trade_analytics.processing import landing_payload, ndjson_serializer


DECIMAL_PATTERN = re.compile(r"-?\d+(?:\.\d+)?\Z")


@dataclass(frozen=True)
class RawLoadScope:
    """描述單次 Raw Load 的來源範圍與識別資訊。"""

    batch_id: str
    source_query_key: str
    query_type: str
    period: str
    period_start_date: date
    reporter_code: int
    cmd_code: str
    flow_code: str
    record_count: int


def parse_period_start_date(period: str) -> date:
    """將合法的 YYYYMM 轉換為月份第一日。"""
    if not isinstance(period, str) or not re.fullmatch(r"\d{6}", period):
        raise ValueError(f"無效的 Period：{period}")

    year, month = int(period[:4]), int(period[4:])

    try:
        return date(year, month, 1)
    except ValueError as exc:
        raise ValueError(f"無效的 Period：{period}") from exc


def validate_decimal_text(value: str | None, name: str, nullable: bool = False) -> None:
    """驗證十進位文字可安全轉換為 BigQuery BIGNUMERIC。"""
    if value is None:
        if nullable:
            return
        raise ValueError(f"{name} 不可為 None")

    if not isinstance(value, str) or not DECIMAL_PATTERN.fullmatch(value):
        raise ValueError(f"{name} 不是合法的十進位字串")

    try:
        data_contract.decimal_to_text(Decimal(value), name)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} 超出 BIGNUMERIC 範圍或精度限制") from exc


def validate_source_record(record: dict, index: int) -> dict:
    """驗證來源 JSON 與 Landing 欄位映射一致。"""
    try:
        source = json.loads(record["source_record_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"來源 JSON 無效：row={index}") from exc

    if not isinstance(source, dict):
        raise ValueError(f"來源 JSON 必須為 Object：row={index}")

    field_mapping = {
        "period": "period",
        "reporterCode": "reporter_code",
        "partnerCode": "partner_code",
        "cmdCode": "cmd_code",
        "flowCode": "flow_code",
        "classificationCode": "classification_code",
        "freqCode": "freq_code",
        "partner2Code": "partner2_code",
        "customsCode": "customs_code",
        "motCode": "mot_code",
        "isOriginalClassification": "is_original_classification",
    }

    for source_name, landing_name in field_mapping.items():
        if source.get(source_name) != record[landing_name]:
            raise ValueError(
                f"來源與 Landing 欄位不一致：row={index} field={source_name}"
            )

    numeric_mapping = {
        "primaryValue": "primary_value_text",
        "netWgt": "net_weight_text",
        "qty": "quantity_text",
    }

    for source_name, landing_name in numeric_mapping.items():
        source_value = source.get(source_name)
        landing_value = record[landing_name]

        if source_value is None and landing_value is None:
            continue

        if source_value is None or landing_value is None:
            raise ValueError(
                f"來源與 Landing 數值不一致：row={index} field={source_name}"
            )

        try:
            matches = Decimal(str(source_value)) == Decimal(landing_value)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(
                f"來源數值格式錯誤：row={index} field={source_name}"
            ) from exc

        if not matches:
            raise ValueError(
                f"來源與 Landing 數值不一致：row={index} field={source_name}"
            )

    return source


def validate_raw_record(record: dict, scope: RawLoadScope, index: int) -> dict:
    """驗證單筆 Landing 資料的 Raw Scope、數值與來源一致性。"""
    expected_values = {
        "batch_id": scope.batch_id,
        "source_query_key": scope.source_query_key,
        "query_type": scope.query_type,
        "period": scope.period,
        "reporter_code": scope.reporter_code,
        "cmd_code": scope.cmd_code,
        "flow_code": scope.flow_code,
        "freq_code": "M",
        "partner2_code": 0,
        "customs_code": "C00",
        "mot_code": 0,
        "is_original_classification": True,
        "source_row_index": index,
    }

    for name, expected in expected_values.items():
        if record[name] != expected:
            raise ValueError(f"Raw Scope 不一致：row={index} field={name}")

    if scope.query_type == query.QueryType.WORLD_TOTAL.value:
        if record["partner_code"] != 0:
            raise ValueError(f"World Total Partner Code 必須為 0：row={index}")
    elif record["partner_code"] == 0:
        raise ValueError(f"Partner Detail 不可包含 World Total：row={index}")

    if record["hs_version"] is not None:
        if record["hs_version"] != record["classification_code"]:
            raise ValueError(f"HS Version 與來源 Classification 不一致：row={index}")

    validate_decimal_text(record["primary_value_text"], "primary_value_text")
    validate_decimal_text(record["net_weight_text"], "net_weight_text", nullable=True)
    validate_decimal_text(record["quantity_text"], "quantity_text", nullable=True)

    return validate_source_record(record, index)


def validate_raw_load_scope(
    payload: landing_payload.LandingPayload,
    comtrade_query: query.ComtradeQuery,
) -> RawLoadScope:
    """驗證整批 Landing 資料並建立可供 Raw Loader 使用的 Scope。"""
    ndjson_serializer.validate_landing_schema(payload)

    if payload.record_count == 0:
        raise ValueError("Raw Initial Load 不接受空批次")

    expected_key = query_identity.build_source_query_key(comtrade_query)

    if payload.source_query_key != expected_key:
        raise ValueError("Source Query Key 與查詢定義不一致")

    if payload.query_type != comtrade_query.query_type.value:
        raise ValueError("Query Type 與查詢定義不一致")

    scope = RawLoadScope(
        batch_id=payload.batch_id,
        source_query_key=expected_key,
        query_type=comtrade_query.query_type.value,
        period=comtrade_query.period,
        period_start_date=parse_period_start_date(comtrade_query.period),
        reporter_code=comtrade_query.reporter_code,
        cmd_code=comtrade_query.cmd_code,
        flow_code="M",
        record_count=payload.record_count,
    )

    seen_keys = set()

    for index, record in enumerate(payload.records):
        source = validate_raw_record(record, scope, index)

        try:
            business_key = query_identity.build_record_business_key(
                source, comtrade_query
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"無法建立 Record Business Key：row={index}"
            ) from exc

        if business_key in seen_keys:
            raise ValueError(f"Raw Record Business Key 重複：row={index}")

        seen_keys.add(business_key)

    return scope
