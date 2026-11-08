
import hashlib
import json
from typing import Any

from trade_analytics.ingestion.batch import BatchResult
from trade_analytics.ingestion.query import ComtradeQuery


KEY_VERSION = 1

RECORD_KEY_FIELDS = (
    "period",
    "reporterCode",
    "partnerCode",
    "cmdCode",
    "flowCode",
    "classificationCode",
    "freqCode",
    "partner2Code",
    "customsCode",
    "motCode",
    "isOriginalClassification",
)

NON_KEY_FIELDS = {
    "primaryValue",
    "netWgt",
    "qty",
}


def build_record_business_key(record: dict[str, Any], query: ComtradeQuery) -> str:
    """根據來源維度產生候選 Record Business Key。"""
    dimensions = {field: record[field] for field in RECORD_KEY_FIELDS}

    return calculate_key({
        "key_version": KEY_VERSION,
        "source_query_key": build_source_query_key(query),
        "dimensions": dimensions,
    })

def build_source_query_key(query: ComtradeQuery) -> str:
    """同一組 Query 條件產生相同的 Source Query Key。"""
    return calculate_key(build_query_scope(query))

def calculate_key(value: dict[str, Any]) -> str:
    """使用 SHA-256 產生穩定的識別碼。"""
    data = canonical_json(value).encode("utf-8")
    return hashlib.sha256(data).hexdigest()

def build_query_scope(query: ComtradeQuery) -> dict[str, Any]:
    """根據 API 查詢條件建立 Query Scope。"""
    return {
        "key_version": KEY_VERSION,
        "query_type": query.query_type.value,
        "params": query.to_params(),
    }

def canonical_json(value: dict[str, Any]) -> str:
    """將 Dictionary 轉換為欄位排序固定的 JSON 字串。"""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def validate_record_uniqueness(result: BatchResult) -> None:
    """候選 Business Key 重複時拋出錯誤，不自動去重。"""
    duplicates = find_duplicate_record_keys(result)

    if duplicates:
        first = duplicates[0]
        raise ValueError(
            f"發現重複 Record Business Key："
            f"first_row={first['first_row_index']} "
            f"duplicate_row={first['duplicate_row_index']} "
            f"duplicate_count={len(duplicates)}"
        )

def find_duplicate_record_keys(result: BatchResult) -> list[dict[str, Any]]:
    """找出同一組 Query 中具有相同候選 Business Key 的紀錄。"""
    first_seen: dict[str, int] = {}
    duplicates = []

    for index, record in enumerate(result.records):
        key = build_record_business_key(record, result.query)

        if key in first_seen:
            duplicates.append({
                "record_key": key,
                "first_row_index": first_seen[key],
                "duplicate_row_index": index,
            })
        else:
            first_seen[key] = index

    return duplicates


def find_unmodeled_fields(result: BatchResult) -> set[str]:
    """找出未納入候選 Business Key，也未列入已知非 Key 欄位的來源欄位。"""
    known_fields = set(RECORD_KEY_FIELDS) | NON_KEY_FIELDS

    return set().union(*(record.keys() - known_fields for record in result.records)) if result.records else set()
