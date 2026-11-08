import hashlib
import re
from dataclasses import dataclass
from typing import Any, Collection

from trade_analytics.contracts import (
    query_identity,
    raw_load_contract,
)
from trade_analytics.ingestion import batch, query, schemas
from trade_analytics.processing import landing_payload
from trade_analytics.utils import manifest

PREVIEW_RECORD_LIMIT = 500

# 名稱、ISO Code、說明文字等展示資訊。
SOURCE_DESCRIPTION_FIELDS = frozenset({
    "reporterDesc",
    "reporterISO",
    "partnerDesc",
    "partnerISO",
    "partner2Desc",
    "partner2ISO",
    "cmdDesc",
    "flowDesc",
    "customsDesc",
    "motDesc",
})

# 與查詢時間及來源分類有關，須進一步驗證值。
SOURCE_REFERENCE_FIELDS = frozenset({
    "typeCode",
    "refYear",
    "refMonth",
    "refPeriodId",
    "classificationSearchCode",
})

# 其他度量與單位資訊。
# 本版 Raw 不建立對應欄位，仍保留於 source_record_json。
SOURCE_MEASURE_FIELDS = frozenset({
    "cifvalue",
    "fobvalue",
    "grossWgt",
    "altQty",
    "qtyUnitCode",
    "qtyUnitAbbr",
    "altQtyUnitCode",
    "altQtyUnitAbbr",
})

# 來源估計值、回報狀態等品質資訊。
SOURCE_QUALITY_FIELDS = frozenset({
    "isQtyEstimated",
    "isAltQtyEstimated",
    "isGrossWgtEstimated",
    "isNetWgtEstimated",
    "legacyEstimationFlag",
    "isReported",
})

# Commodity 彙總層級與供應模式等附加資訊。
# 這些欄位會進行額外型別及值檢查。
SOURCE_AGGREGATION_FIELDS = frozenset({
    "aggrLevel",
    "isAggregate",
    "isLeaf",
    "mosCode",
})

APPROVED_SOURCE_NON_GRAIN_FIELDS = (
        SOURCE_DESCRIPTION_FIELDS
        | SOURCE_REFERENCE_FIELDS
        | SOURCE_MEASURE_FIELDS
        | SOURCE_QUALITY_FIELDS
        | SOURCE_AGGREGATION_FIELDS
)


class SourceCompletenessError(ValueError):
    """API 回應未通過完整性驗證。"""


class SourceGrainError(ValueError):
    """來源資料粒度或欄位契約未通過驗證。"""


@dataclass(frozen=True)
class SourceQualityReport:
    """來源回應與資料粒度的驗證結果。"""

    source_query_key: str
    response_count: int
    accepted_record_count: int
    discarded_world_total_count: int
    record_limit: int
    records_sha256: str
    unmodeled_fields: tuple[str, ...]
    assurance_level: str = "BOUNDED_PREVIEW"


def validate_response_envelope(
        response_payload: dict[str, Any],
        record_limit: int = PREVIEW_RECORD_LIMIT,
) -> None:
    """檢查 Preview API 的基本完整性與截斷門檻。"""
    if not isinstance(response_payload, dict):
        raise SourceCompletenessError(
            "API Response 必須為 Object"
        )

    if type(record_limit) is not int or not 1 <= record_limit <= PREVIEW_RECORD_LIMIT:
        raise ValueError(
            "record_limit 必須介於 1～500"
        )

    if response_payload.get("error"):
        raise SourceCompletenessError(
            f"UN Comtrade API Error：{response_payload['error']}"
        )

    records = response_payload.get("data")
    count = response_payload.get("count")

    if not isinstance(records, list):
        raise SourceCompletenessError(
            "API Response 缺少合法 data"
        )

    if type(count) is not int or count < 0:
        raise SourceCompletenessError(
            "API Response 缺少合法 count"
        )

    if count != len(records):
        raise SourceCompletenessError(
            f"API Response Count 不一致：count={count} "
            f"actual={len(records)}"
        )

    if count >= record_limit:
        raise SourceCompletenessError(
            f"API Response 可能遭截斷："
            f"count={count} limit={record_limit}"
        )


def select_query_records(
        records: list[dict[str, Any]],
        query_type: query.QueryType,
) -> list[dict[str, Any]]:
    """依 Query Type 篩選來源紀錄。"""
    if query_type == query.QueryType.WORLD_TOTAL:
        return [
            record
            for record in records
            if record["partnerCode"] == 0
        ]

    if query_type == query.QueryType.PARTNER_DETAIL:
        return [
            record
            for record in records
            if record["partnerCode"] != 0
        ]

    raise SourceCompletenessError(
        f"不支援的 Query Type：{query_type}"
    )


def validate_response_records(
        result: batch.BatchResult,
        response_payload: dict[str, Any],
) -> tuple[int, int]:
    """確認 BatchResult 與同一次 API Response 的內容一致。"""
    try:
        response = schemas.ComtradeResponse.model_validate(
            response_payload
        )
    except Exception as exc:
        raise SourceCompletenessError(
            "API Response Schema 驗證失敗"
        ) from exc

    records = [
        record.model_dump(by_alias=True)
        for record in response.data
    ]

    selected = select_query_records(
        records,
        result.query.query_type,
    )

    if not selected:
        raise SourceCompletenessError(
            "來源查詢沒有可載入的紀錄"
        )

    if (
            result.query.query_type == query.QueryType.WORLD_TOTAL
            and len(selected) != 1
    ):
        raise SourceCompletenessError(
            "World Total 必須恰好包含一筆紀錄"
        )

    expected_bytes = manifest.serialize_records(selected)
    actual_bytes = manifest.serialize_records(result.records)

    if expected_bytes != actual_bytes:
        raise SourceCompletenessError(
            "BatchResult 與原始 API Response 的篩選結果不一致"
        )

    discarded_count = len(records) - len(selected)

    return len(selected), discarded_count


def validate_source_grain(
        result: batch.BatchResult,
        approved_non_grain_fields: Collection[str] = (),
) -> tuple[str, ...]:
    """拒絕重複 Business Key 與未核准來源欄位。"""
    try:
        query_identity.validate_record_uniqueness(result)
    except (KeyError, TypeError, ValueError) as exc:
        raise SourceGrainError(
            "來源 Record Business Key 驗證失敗"
        ) from exc

    unmodeled = set(
        query_identity.find_unmodeled_fields(result)
    )

    approved = set(approved_non_grain_fields)
    unapproved = unmodeled - approved

    if unapproved:
        raise SourceGrainError(
            f"發現尚未審查的來源欄位："
            f"{sorted(unapproved)}"
        )

    return tuple(sorted(unmodeled))


def validate_source_reference_fields(
        result: batch.BatchResult,
        *,
        require_reference_fields: bool = False,
) -> None:
    """驗證附加時間、分類、彙總及供應模式資訊。

    require_reference_fields=False：
        相容既有精簡 Unit Test Fixtures。

    require_reference_fields=True：
        正式 API Smoke Test 採用較嚴格的來源欄位要求。
    """
    period = result.query.period
    expected_year = int(period[:4])
    expected_month = int(period[4:])
    expected_level = len(result.query.cmd_code)

    required_fields = {
        "typeCode",
        "refYear",
        "refMonth",
        "refPeriodId",
        "classificationSearchCode",
        "aggrLevel",
        "mosCode",
    }

    for index, record in enumerate(result.records):
        if require_reference_fields:
            missing = required_fields - record.keys()

            if missing:
                raise SourceGrainError(
                    f"來源缺少必要參考欄位："
                    f"row={index} fields={sorted(missing)}"
                )

        if "typeCode" in record:
            if record["typeCode"] != "C":
                raise SourceGrainError(
                    f"來源 Type Code 不一致：row={index}"
                )

        if "refYear" in record:
            if type(record["refYear"]) is not int:
                raise SourceGrainError(
                    f"Reference Year 型別不正確：row={index}"
                )

            if record["refYear"] != expected_year:
                raise SourceGrainError(
                    f"Reference Year 與 Period 不一致：row={index}"
                )

        if "refMonth" in record:
            if type(record["refMonth"]) is not int:
                raise SourceGrainError(
                    f"Reference Month 型別不正確：row={index}"
                )

            if record["refMonth"] != expected_month:
                raise SourceGrainError(
                    f"Reference Month 與 Period 不一致：row={index}"
                )

        if "refPeriodId" in record:
            value = record["refPeriodId"]

            if type(value) not in (str, int):
                raise SourceGrainError(
                    f"Reference Period ID 型別不正確：row={index}"
                )

            # 月資料的 Reference Period ID 通常包含 YYYYMM。
            # 允許 YYYYMM 或 YYYYMMDD 格式。
            period_id = str(value)

            if not re.fullmatch(r"\d{6}(\d{2})?", period_id):
                raise SourceGrainError(
                    f"Reference Period ID 格式不正確：row={index}"
                )

            if period_id[:6] != period:
                raise SourceGrainError(
                    f"Reference Period ID 與 Period 不一致：row={index}"
                )

        if "classificationSearchCode" in record:
            if record["classificationSearchCode"] != "HS":
                raise SourceGrainError(
                    f"Classification Search Code 不一致：row={index}"
                )

        if "mosCode" in record:
            if record["mosCode"] not in ("0", 0):
                raise SourceGrainError(
                    f"出現未支援的 Mode of Supply：row={index}"
                )

        if "aggrLevel" in record:
            level = record["aggrLevel"]

            if level is not None:
                if type(level) is not int:
                    raise SourceGrainError(
                        f"Aggregation Level 型別不正確：row={index}"
                    )

                if level != expected_level:
                    raise SourceGrainError(
                        f"Aggregation Level 與 Commodity Code 不一致："
                        f"row={index}"
                    )

        # 這兩個欄位可為 Nullable Boolean。
        # 不假設 isAggregate 必須 False，也不假設 isLeaf 必須 True。
        for name in ("isAggregate", "isLeaf"):
            if name in record:
                value = record[name]

                if value is not None and type(value) is not bool:
                    raise SourceGrainError(
                        f"{name} 型別不正確：row={index}"
                    )


def validate_source_completeness(
        result: batch.BatchResult,
        response_payload: dict[str, Any],
        approved_non_grain_fields: Collection[str] = (),
        record_limit: int = PREVIEW_RECORD_LIMIT,
        *,
        require_reference_fields: bool = False,
) -> SourceQualityReport:
    """整合來源 Response、Grain 與 Raw Contract 驗證。"""
    validate_response_envelope(
        response_payload,
        record_limit,
    )

    accepted_count, discarded_count = validate_response_records(
        result,
        response_payload,
    )

    # 先驗證新增欄位的值，再核准其不進入目前 Raw Grain。
    validate_source_reference_fields(
        result,
        require_reference_fields=require_reference_fields,
    )

    unmodeled_fields = validate_source_grain(
        result,
        approved_non_grain_fields,
    )

    payload = landing_payload.build_landing_payload(
        result,
        batch_id="source-validation",
    )

    raw_load_contract.validate_raw_load_scope(
        payload,
        result.query,
    )

    records_bytes = manifest.serialize_records(result.records)

    return SourceQualityReport(
        source_query_key=query_identity.build_source_query_key(
            result.query
        ),
        response_count=response_payload["count"],
        accepted_record_count=accepted_count,
        discarded_world_total_count=discarded_count,
        record_limit=record_limit,
        records_sha256=hashlib.sha256(
            records_bytes
        ).hexdigest(),
        unmodeled_fields=unmodeled_fields,
    )
