
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import bigquery_config, bigquery_schema


logger = logging.getLogger(__name__)

VALID_STATUSES = {
    "STARTED",
    "SUCCEEDED",
    "FAILED",
    "VALIDATION_FAILED",
    "UNKNOWN",
}


class MetadataWriteError(RuntimeError):
    """表示 Ingestion Metadata 寫入 BigQuery 失敗。"""


def build_ingestion_row(
    payload: ndjson_serializer.SerializedLandingPayload,
    job_id: str,
    status: str,
    started_at: datetime,
    completed_at: datetime | None = None,
    loaded_record_count: int | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """建立符合 ingestion_runs Schema 的執行紀錄。"""
    if status not in VALID_STATUSES:
        raise ValueError(f"不支援的 Ingestion Status：{status}")

    if not job_id:
        raise ValueError("load_job_id 不可為空")

    if started_at.tzinfo is None or (
        completed_at is not None and completed_at.tzinfo is None
    ):
        raise ValueError("Ingestion Timestamp 必須包含時區")

    if status == "STARTED" and completed_at is not None:
        raise ValueError("STARTED 不可設定 completed_at")

    if status != "STARTED" and completed_at is None:
        raise ValueError("最終狀態必須設定 completed_at")

    if loaded_record_count is not None and loaded_record_count < 0:
        raise ValueError("loaded_record_count 不可為負數")

    return {
        "batch_id": payload.batch_id,
        "source_query_key": payload.source_query_key,
        "revision_id": None,
        "query_type": payload.query_type,
        "status": status,
        "record_count": payload.record_count,
        "loaded_record_count": loaded_record_count,
        "sha256": payload.sha256,
        "size_bytes": payload.size_bytes,
        "load_job_id": job_id,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "completed_at": (
            completed_at.astimezone(timezone.utc).isoformat()
            if completed_at is not None else None
        ),
        "error_message": error_message,
    }


def write_ingestion_metadata(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    row: dict[str, Any],
) -> None:
    """將執行紀錄寫入 BigQuery，並檢查 Streaming Insert 錯誤。"""
    table_id = config.raw_table(bigquery_schema.INGESTION_RUNS_TABLE)

    try:
        errors = client.insert_rows_json(
            table_id,
            [row],
            skip_invalid_rows=False,
            ignore_unknown_values=False,
        )
    except Exception as exc:
        logger.exception(
            "Ingestion Metadata 寫入失敗：table=%s job_id=%s",
            table_id,
            row["load_job_id"],
        )
        raise MetadataWriteError(
            f"Ingestion Metadata 寫入結果不明：job_id={row['load_job_id']}"
        ) from exc

    if errors:
        logger.error(
            "Ingestion Metadata 插入失敗：job_id=%s errors=%s",
            row["load_job_id"],
            errors,
        )
        raise MetadataWriteError(
            f"Ingestion Metadata 插入失敗：job_id={row['load_job_id']} "
            f"errors={errors}"
        )

    logger.info(
        "Ingestion Metadata 寫入成功：job_id=%s status=%s",
        row["load_job_id"],
        row["status"],
    )
