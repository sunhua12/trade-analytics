import hashlib
import json
import logging
from dataclasses import dataclass
from io import BytesIO

from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import bigquery_config, bigquery_schema

logger = logging.getLogger(__name__)


class LandingLoadError(RuntimeError):
    """表示 BigQuery Landing Load 發生錯誤。"""


class LoadJobFailed(LandingLoadError):
    """表示 BigQuery Load Job 已確認失敗。"""


class LoadJobStateUnknown(LandingLoadError):
    """表示尚無法確認 BigQuery Load Job 的最終狀態。"""


class LoadJobRowCountMismatch(LandingLoadError):
    """表示已完成的 Load Job 載入筆數與預期不同。"""


@dataclass(frozen=True)
class LandingLoadResult:
    batch_id: str
    source_query_key: str
    job_id: str
    table_id: str
    loaded_record_count: int
    sha256: str


def load_landing_payload(client: bigquery.Client, config: bigquery_config.BigQueryConfig,
                         payload: ndjson_serializer.SerializedLandingPayload) -> LandingLoadResult:
    """提交或查詢固定 ID 的 Load Job，並確認最終載入結果。"""
    validate_serialized_payload(payload)

    table_id = config.raw_table(bigquery_schema.LANDING_TABLE)
    job_id = build_load_job_id(payload, table_id)

    logger.info("開始 Landing Load：batch_id=%s job_id=%s records=%d",
                payload.batch_id, job_id, payload.record_count)

    try:
        job = client.load_table_from_file(
            BytesIO(payload.ndjson_bytes),
            destination=table_id,
            size=payload.size_bytes,
            location=config.location,
            project=config.project_id,
            job_id=job_id,
            job_config=build_load_job_config(),
        )
    except google_exceptions.Conflict:
        logger.warning("Load Job 已存在，查詢既有 Job：job_id=%s", job_id)
        job = get_existing_job(client, config, job_id)
    except Exception as exc:
        logger.exception("Load Job 提交結果不明：job_id=%s", job_id)
        raise LoadJobStateUnknown(f"無法確認 Load Job 是否已提交：job_id={job_id}") from exc

    if job.job_id != job_id:
        raise LoadJobStateUnknown(f"Load Job ID 不一致：expected={job_id} actual={job.job_id}")

    job = wait_for_load_job(client, config, job)
    loaded_count = validate_load_job_result(job, payload.record_count)

    result = LandingLoadResult(
        batch_id=payload.batch_id,
        source_query_key=payload.source_query_key,
        job_id=job_id,
        table_id=table_id,
        loaded_record_count=loaded_count,
        sha256=payload.sha256,
    )

    logger.info("Landing Load 成功：job_id=%s table=%s records=%d",
                result.job_id, result.table_id, result.loaded_record_count)

    return result


def validate_serialized_payload(payload: ndjson_serializer.SerializedLandingPayload) -> None:
    """驗證序列化資料的筆數、大小與 SHA-256 Metadata。"""
    if payload.record_count <= 0:
        raise ValueError("不可提交空的 Landing Payload")

    if not payload.batch_id or not payload.source_query_key or not payload.query_type:
        raise ValueError("缺少 Landing Payload 識別資訊")

    if not payload.ndjson_bytes or payload.size_bytes != len(payload.ndjson_bytes):
        raise ValueError("Landing Payload Byte Size 不一致")

    if hashlib.sha256(payload.ndjson_bytes).hexdigest() != payload.sha256:
        raise ValueError("Landing Payload SHA-256 不一致")

    if len(payload.ndjson_bytes.splitlines()) != payload.record_count:
        raise ValueError("Landing Payload Record Count 不一致")


def build_load_job_id(payload: ndjson_serializer.SerializedLandingPayload, table_id: str) -> str:
    """根據 Batch、Query、Checksum 與目的地產生固定的 Load Job ID。"""
    identity = {
        "batch_id": payload.batch_id,
        "source_query_key": payload.source_query_key,
        "sha256": payload.sha256,
        "table_id": table_id,
        "record_count": payload.record_count,
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"landing_{hashlib.sha256(encoded).hexdigest()}"


def wait_for_load_job(client: bigquery.Client, config: bigquery_config.BigQueryConfig,
                      job: bigquery.LoadJob) -> bigquery.LoadJob:
    """等待 Load Job 完成，等待失敗時重新查詢同一個 Job。"""
    try:
        job.result()
        return job
    except Exception as exc:
        logger.warning("等待 Load Job 發生例外，重新查詢狀態：job_id=%s error=%s", job.job_id, exc)
        return get_existing_job(client, config, job.job_id)


def get_existing_job(client: bigquery.Client, config: bigquery_config.BigQueryConfig, job_id: str) -> bigquery.LoadJob:
    """依 Job ID 查詢既有 Job，無法確認時保留未知狀態。"""
    try:
        return client.get_job(job_id, project=config.project_id, location=config.location)
    except Exception as exc:
        logger.error("無法查詢 BigQuery Job：job_id=%s error=%s", job_id, exc)
        raise LoadJobStateUnknown(f"無法確認 Load Job 狀態：job_id={job_id}") from exc


def build_load_job_config() -> bigquery.LoadJobConfig:
    """建立符合 Landing Schema 的 BigQuery Load Job 設定。"""
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        schema=bigquery_schema.LANDING_SCHEMA,
        max_bad_records=0,
        ignore_unknown_values=False,
    )


def validate_load_job_result(job: bigquery.LoadJob, expected_count: int) -> int:
    """確認 Job 已完成且成功，並驗證實際載入筆數。"""
    if job.state != "DONE":
        raise LoadJobStateUnknown(f"Load Job 尚未確認完成：job_id={job.job_id} state={job.state}")

    if job.error_result:
        logger.error(
            "BigQuery Load Job 失敗：job_id=%s error=%s details=%s",
            job.job_id,
            job.error_result,
            job.errors,
        )
        raise LoadJobFailed(
            f"Load Job 失敗：job_id={job.job_id} error={job.error_result}"
        )

    if job.errors:
        logger.warning(
            "Load Job 回傳非致命錯誤資訊：job_id=%s errors=%s",
            job.job_id,
            job.errors,
        )

    if job.output_rows is None:
        raise LoadJobStateUnknown(
            f"Load Job 缺少載入筆數資訊：job_id={job.job_id}"
        )

    if job.output_rows != expected_count:
        raise LoadJobRowCountMismatch(
            f"BigQuery 載入筆數不一致：expected={expected_count} "
            f"actual={job.output_rows} job_id={job.job_id}"
        )

    return job.output_rows
