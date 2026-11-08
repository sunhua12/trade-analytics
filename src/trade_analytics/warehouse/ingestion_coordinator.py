
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_loader,
    bigquery_schema,
    ingestion_metadata,
)


logger = logging.getLogger(__name__)


def current_utc_time() -> datetime:
    """取得具備 UTC 時區資訊的目前時間。"""
    return datetime.now(timezone.utc)


def record_ingestion_status(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    payload: ndjson_serializer.SerializedLandingPayload,
    job_id: str,
    status: str,
    started_at: datetime,
    loaded_record_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """根據執行狀態建立並寫入 Ingestion Metadata。"""
    row = ingestion_metadata.build_ingestion_row(
        payload=payload,
        job_id=job_id,
        status=status,
        started_at=started_at,
        completed_at=current_utc_time() if status != "STARTED" else None,
        loaded_record_count=loaded_record_count,
        error_message=error_message,
    )
    ingestion_metadata.write_ingestion_metadata(client, config, row)


def classify_load_error(exc: Exception) -> str:
    """將 Load Job 例外分類為失敗、驗證失敗或狀態未知。"""
    if isinstance(exc, bigquery_loader.LoadJobFailed):
        return "FAILED"

    if isinstance(exc, bigquery_loader.LoadJobRowCountMismatch):
        return "VALIDATION_FAILED"

    return "UNKNOWN"


def run_landing_load_with_metadata(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    payload: ndjson_serializer.SerializedLandingPayload,
) -> bigquery_loader.LandingLoadResult:
    """依序記錄開始狀態、執行 Landing Load，並記錄最終狀態。"""
    bigquery_loader.validate_serialized_payload(payload)

    table_id = config.raw_table(bigquery_schema.LANDING_TABLE)
    job_id = bigquery_loader.build_load_job_id(payload, table_id)
    started_at = current_utc_time()

    record_ingestion_status(
        client, config, payload, job_id, "STARTED", started_at
    )

    try:
        result = bigquery_loader.load_landing_payload(client, config, payload)
    except Exception as exc:
        status = classify_load_error(exc)

        try:
            record_ingestion_status(
                client,
                config,
                payload,
                job_id,
                status,
                started_at,
                error_message=str(exc),
            )
        except ingestion_metadata.MetadataWriteError:
            logger.exception(
                "無法記錄 Landing Load 的終止狀態：job_id=%s status=%s",
                job_id,
                status,
            )

        raise

    record_ingestion_status(
        client,
        config,
        payload,
        job_id,
        "SUCCEEDED",
        started_at,
        loaded_record_count=result.loaded_record_count,
    )

    return result
