import hashlib
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from trade_analytics.ingestion.batch import BatchResult


logger = logging.getLogger(__name__)


@dataclass
class ManifestEntry:
    query: dict[str, Any]
    record_count: int
    size_bytes: int
    sha256: str


@dataclass
class BatchManifest:
    schema_version: int
    entries: list[ManifestEntry]


def serialize_decimal(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)

    raise TypeError(f"無法序列化的型別：{type(value).__name__}")


def serialize_records(records: list[dict[str, Any]]) -> bytes:
    """將交易紀錄轉換為固定格式的 NDJSON Bytes。"""
    lines = [json.dumps(record, default=serialize_decimal, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) for record in records]
    text = "\n".join(lines) + ("\n" if lines else "")
    return text.encode("utf-8")


def calculate_sha256(data: bytes) -> str:
    """計算資料的 SHA-256 Checksum。"""
    return hashlib.sha256(data).hexdigest()


def build_manifest(results: list[BatchResult]) -> BatchManifest:
    """為整個 Batch 建立 Manifest。"""
    entries = []

    for result in results:
        data = serialize_records(result.records)
        entry = ManifestEntry(
            query=result.query.model_dump(mode="json"),
            record_count=len(result.records),
            size_bytes=len(data),
            sha256=calculate_sha256(data),
        )
        entries.append(entry)

    logger.debug("Manifest 已建立：%d entries", len(entries))
    return BatchManifest(schema_version=1, entries=entries)


def verify_artifact(data: bytes, entry: ManifestEntry) -> None:
    """驗證 NDJSON Bytes 是否與 Manifest 一致。"""
    if len(data) != entry.size_bytes:
        raise ValueError("Artifact 大小與 Manifest 不一致")

    if calculate_sha256(data) != entry.sha256:
        raise ValueError("Artifact SHA-256 Checksum 不一致")

    if len(data.splitlines()) != entry.record_count:
        raise ValueError("Artifact 筆數與 Manifest 不一致")


def verify_manifest(results: list[BatchResult], manifest: BatchManifest) -> None:
    """驗證整批結果是否與 Manifest 一致。"""
    if manifest.schema_version != 1:
        raise ValueError("不支援的 Manifest Schema Version")

    if len(results) != len(manifest.entries):
        raise ValueError("Batch Query 數量與 Manifest 不一致")

    for result, entry in zip(results, manifest.entries):
        if result.query.model_dump(mode="json") != entry.query:
            raise ValueError("Query 與 Manifest 不一致")

        data = serialize_records(result.records)
        verify_artifact(data, entry)

    logger.debug("Manifest 驗證通過：%d entries", len(manifest.entries))
