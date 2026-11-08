import hashlib
import json
import logging
import dataclasses

from trade_analytics.processing import landing_payload
from trade_analytics.utils import manifest
from trade_analytics.warehouse import bigquery_schema

logger = logging.getLogger(__name__)

FIELD_TYPES = {
    "STRING": str,
    "INTEGER": int,
    "INT64": int,
    "BOOLEAN": bool,
    "BOOL": bool,
}


@dataclasses.dataclass(frozen=True)
class SerializedLandingPayload:
    batch_id: str
    source_query_key: str
    query_type: str
    ndjson_bytes: bytes
    record_count: int
    size_bytes: int
    sha256: str


def serialize_landing_payload(payload: landing_payload.LandingPayload) -> SerializedLandingPayload:
    """驗證 Landing Payload，產生穩定的 NDJSON Bytes 及完整性 Metadata。"""
    validate_landing_schema(payload)

    ndjson_bytes = manifest.serialize_records(payload.records)

    result = SerializedLandingPayload(
        batch_id=payload.batch_id,
        source_query_key=payload.source_query_key,
        query_type=payload.query_type,
        ndjson_bytes=ndjson_bytes,
        record_count=payload.record_count,
        size_bytes=len(ndjson_bytes),
        sha256=hashlib.sha256(ndjson_bytes).hexdigest(),
    )

    logger.info("Landing NDJSON 序列化成功：batch_id=%s records=%d bytes=%d sha256=%s",
                result.batch_id, result.record_count, result.size_bytes, result.sha256)

    return result


def validate_landing_schema(payload: landing_payload.LandingPayload) -> None:
    """檢查 Payload 欄位及型別是否符合 BigQuery Landing Schema。"""
    expected_fields = {field.name: field for field in bigquery_schema.LANDING_SCHEMA}

    for index, record in enumerate(payload.records):
        validate_record_fields(record, expected_fields, index)
        validate_field_types(record, expected_fields, index)
        validate_record_integrity(record, payload, index)


def validate_record_fields(record: dict, expected_fields: dict, index: int) -> None:
    """驗證紀錄欄位是否與 BigQuery Landing Schema 一致。"""
    missing = sorted(set(expected_fields) - set(record))
    extra = sorted(set(record) - set(expected_fields))

    if missing or extra:
        raise ValueError(f"Landing Schema 不一致：row={index} missing={missing} extra={extra}")


def validate_field_types(record: dict, expected_fields: dict, index: int) -> None:
    """驗證各欄位的資料型別及 Nullable 限制。"""
    for name, field in expected_fields.items():
        value = record[name]

        if value is None:
            if field.mode == "REQUIRED":
                raise ValueError(f"必要欄位不可為 None：row={index} field={name}")
            continue

        expected_type = FIELD_TYPES.get(field.field_type.upper())
        if expected_type is None:
            raise ValueError(f"不支援的 Landing 欄位型別：{field.field_type}")

        if type(value) is not expected_type:
            raise TypeError(
                f"Landing 欄位型別不符：row={index} field={name} "
                f"expected={expected_type.__name__} actual={type(value).__name__}"
            )


def validate_record_integrity(record: dict, payload: landing_payload.LandingPayload, index: int) -> None:
    """驗證紀錄的 Metadata、Row Index 及來源 JSON 格式。"""
    if (
            record["batch_id"] != payload.batch_id
            or record["source_query_key"] != payload.source_query_key
            or record["query_type"] != payload.query_type
    ):
        raise ValueError(f"Landing Metadata 不一致：row={index}")

    if record["source_row_index"] != index:
        raise ValueError(f"source_row_index 不連續：row={index}")

    try:
        json.loads(record["source_record_json"])
    except json.JSONDecodeError as exc:
        raise ValueError(f"source_record_json 非合法 JSON：row={index}") from exc
