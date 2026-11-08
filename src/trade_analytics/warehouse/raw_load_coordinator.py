import hashlib
import json
import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from trade_analytics.contracts import raw_load_contract, source_completeness
from trade_analytics.ingestion import query
from trade_analytics.processing import landing_payload
from trade_analytics.utils import manifest
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_schema,
    ingestion_metadata,
    raw_initial_loader,
    raw_load_decision,
)

logger = logging.getLogger(__name__)


class RawIntegrityError(RuntimeError):
    """表示已完成的 Raw Job 未通過資料完整性驗證。"""


def source_records_bytes(payload: landing_payload.LandingPayload) -> bytes:
    """依來源順序序列化原始紀錄，供 Checksum 核對使用。"""
    records = [json.loads(row["source_record_json"]) for row in payload.records]
    return manifest.serialize_records(records)


def validate_raw_source(
        payload: landing_payload.LandingPayload,
        comtrade_query: query.ComtradeQuery,
        report: source_completeness.SourceQualityReport,
) -> raw_load_contract.RawLoadScope:
    """驗證 Raw Scope 與 Source Quality Report 是否對應同一批資料。"""
    scope = raw_load_contract.validate_raw_load_scope(payload, comtrade_query)
    raw_load_decision.validate_source_evidence(scope, report)

    checksum = hashlib.sha256(source_records_bytes(payload)).hexdigest()
    if checksum != report.records_sha256:
        raise ValueError("Source Quality Report 與 Payload Checksum 不一致")

    return scope


def build_raw_identity(
        scope: raw_load_contract.RawLoadScope,
        report: source_completeness.SourceQualityReport,
) -> tuple[str, str]:
    """產生本次 Raw Load 的固定 Revision ID 與 Job ID。"""
    revision_id = raw_initial_loader.build_revision_id(
        scope, report.records_sha256
    )
    job_id = raw_initial_loader.build_raw_job_id(scope, revision_id)
    return revision_id, job_id


def write_raw_audit(
        client: bigquery.Client,
        config: bigquery_config.BigQueryConfig,
        scope: raw_load_contract.RawLoadScope,
        report: source_completeness.SourceQualityReport,
        job_id: str,
        revision_id: str,
        status: str,
        started_at: datetime,
        loaded_record_count: int | None = None,
        error_message: str | None = None,
) -> None:
    """將 Raw Load 的執行狀態寫入既有 Ingestion Metadata Table。"""

    row = {
        "batch_id": scope.batch_id,
        "source_query_key": scope.source_query_key,
        "revision_id": revision_id,
        "query_type": scope.query_type,
        "status": status,
        "record_count": scope.record_count,
        "loaded_record_count": loaded_record_count,
        "sha256": report.records_sha256,
        "size_bytes": None,
        "load_job_id": job_id,
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "completed_at": (
            datetime.now(timezone.utc).isoformat()
            if status != "STARTED" else None
        ),
        "error_message": error_message,
    }

    ingestion_metadata.write_ingestion_metadata(client, config, row)


def verify_raw_records(
        client: bigquery.Client,
        config: bigquery_config.BigQueryConfig,
        scope: raw_load_contract.RawLoadScope,
        report: source_completeness.SourceQualityReport,
        revision_id: str,
) -> None:
    """核對 Raw Scope 的筆數、來源識別、Row Index 與內容 Checksum。"""
    table_id = config.raw_table(bigquery_schema.RAW_TABLE)
    sql = f"""
        SELECT batch_id, source_query_key, revision_id,
               source_row_index, source_record_json
        FROM `{table_id}`
        WHERE period_start_date = @period_start_date
          AND period = @period
          AND reporter_code = @reporter_code
          AND cmd_code = @cmd_code
          AND query_type = @query_type
          AND flow_code = @flow_code
        ORDER BY source_row_index
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "period_start_date", "DATE", scope.period_start_date
            ),
            bigquery.ScalarQueryParameter("period", "STRING", scope.period),
            bigquery.ScalarQueryParameter(
                "reporter_code", "INT64", scope.reporter_code
            ),
            bigquery.ScalarQueryParameter("cmd_code", "STRING", scope.cmd_code),
            bigquery.ScalarQueryParameter(
                "query_type", "STRING", scope.query_type
            ),
            bigquery.ScalarQueryParameter("flow_code", "STRING", scope.flow_code),
        ]
    )

    rows = list(client.query(
        sql, job_config=job_config, location=config.location
    ).result())

    if len(rows) != scope.record_count:
        raise RawIntegrityError(
            f"Raw Record Count 不一致：expected={scope.record_count} actual={len(rows)}"
        )

    records = []
    for index, row in enumerate(rows):
        if (
                row["batch_id"] != scope.batch_id
                or row["source_query_key"] != scope.source_query_key
                or row["revision_id"] != revision_id
                or row["source_row_index"] != index
        ):
            raise RawIntegrityError(f"Raw Metadata 或 Row Index 不一致：row={index}")

        try:
            source = json.loads(row["source_record_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise RawIntegrityError(f"Raw Source JSON 無效：row={index}") from exc

        records.append(source)

    actual_sha256 = hashlib.sha256(
        manifest.serialize_records(records)
    ).hexdigest()

    if actual_sha256 != report.records_sha256:
        raise RawIntegrityError("Raw Source Checksum 不一致")


def classify_raw_error(exc: Exception) -> str:
    """將 Raw Load 例外分類成可確認的失敗或未知狀態。"""
    if isinstance(exc, raw_initial_loader.RawInitialLoadError):
        if not isinstance(exc, raw_initial_loader.RawInitialLoadStateUnknown):
            return "FAILED"

    if isinstance(exc, RawIntegrityError):
        return "VALIDATION_FAILED"

    return "UNKNOWN"


def run_initial_raw_with_audit(
        client: bigquery.Client,
        config: bigquery_config.BigQueryConfig,
        payload: landing_payload.LandingPayload,
        comtrade_query: query.ComtradeQuery,
        report: source_completeness.SourceQualityReport,
        *,
        allow_bounded_preview: bool = False,
) -> raw_initial_loader.RawInitialLoadResult:
    """協調來源驗證、首次 Raw 載入與執行狀態紀錄。"""
    scope = validate_raw_source(payload, comtrade_query, report)

    if not allow_bounded_preview:
        raise ValueError("必須明確允許 BOUNDED_PREVIEW 才能載入 Raw")

    decision = raw_load_decision.decide_initial_raw_load(
        client, config, scope, report
    )
    revision_id, job_id = build_raw_identity(scope, report)
    started_at = datetime.now(timezone.utc)

    write_raw_audit(
        client, config, scope, report, job_id, revision_id,
        "STARTED", started_at,
    )

    try:
        result = raw_initial_loader.load_initial_raw(
            client,
            config,
            payload,
            comtrade_query,
            decision,
            report.records_sha256,
            allow_bounded_preview=True,
        )

        verify_raw_records(
            client, config, scope, report, revision_id
        )

    except Exception as exc:
        status = classify_raw_error(exc)

        try:
            write_raw_audit(
                client, config, scope, report, job_id, revision_id,
                status, started_at, error_message=str(exc),
            )
        except ingestion_metadata.MetadataWriteError:
            logger.exception(
                "Raw Load 終止狀態記錄失敗：job_id=%s", job_id
            )

        raise

    write_raw_audit(
        client, config, scope, report, job_id, revision_id,
        "SUCCEEDED", started_at,
        loaded_record_count=result.record_count,
    )

    logger.info(
        "Raw Initial Load 完成：job_id=%s records=%d",
        job_id,
        result.record_count,
    )

    return result


def recover_initial_raw(
        client: bigquery.Client,
        config: bigquery_config.BigQueryConfig,
        payload: landing_payload.LandingPayload,
        comtrade_query: query.ComtradeQuery,
        report: source_completeness.SourceQualityReport,
) -> raw_initial_loader.RawInitialLoadResult:
    """只查詢原 Job 並核對 Raw，不重新提交 INSERT。"""
    scope = validate_raw_source(payload, comtrade_query, report)
    revision_id, job_id = build_raw_identity(scope, report)

    try:
        job = client.get_job(
            job_id, project=config.project_id, location=config.location
        )
    except Exception as exc:
        raise raw_initial_loader.RawInitialLoadStateUnknown(
            f"無法確認 Raw Job 狀態：job_id={job_id}"
        ) from exc

    if job.job_id != job_id or job.state != "DONE":
        raise raw_initial_loader.RawInitialLoadStateUnknown(
            f"Raw Job 尚未確認完成：job_id={job_id} state={job.state}"
        )

    if job.error_result:
        raise raw_initial_loader.RawInitialLoadError(
            f"Raw Job 已確認失敗：job_id={job_id} error={job.error_result}"
        )

    verify_raw_records(client, config, scope, report, revision_id)

    write_raw_audit(
        client,
        config,
        scope,
        report,
        job_id,
        revision_id,
        "SUCCEEDED",
        datetime.now(timezone.utc),
        loaded_record_count=scope.record_count,
    )

    return raw_initial_loader.RawInitialLoadResult(
        batch_id=scope.batch_id,
        source_query_key=scope.source_query_key,
        revision_id=revision_id,
        job_id=job_id,
        table_id=config.raw_table(bigquery_schema.RAW_TABLE),
        record_count=scope.record_count,
    )
