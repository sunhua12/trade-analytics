
import hashlib
import json
import logging
import re
from dataclasses import dataclass

from google.cloud import bigquery

from trade_analytics.contracts import query_identity, raw_load_contract
from trade_analytics.ingestion import query
from trade_analytics.processing import landing_payload
from trade_analytics.utils import manifest
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_schema,
    raw_load_decision,
)


logger = logging.getLogger(__name__)


class RawInitialLoadError(RuntimeError):
    """表示首次 Raw Load 無法成功完成。"""


class RawInitialLoadStateUnknown(RawInitialLoadError):
    """表示無法確認 Raw Load Job 的最終狀態。"""


@dataclass(frozen=True)
class RawInitialLoadResult:
    """記錄首次 Raw Load 的識別資訊與執行結果。"""

    batch_id: str
    source_query_key: str
    revision_id: str
    job_id: str
    table_id: str
    record_count: int


def validate_raw_load_inputs(
    payload: landing_payload.LandingPayload,
    comtrade_query: query.ComtradeQuery,
    decision: raw_load_decision.RawLoadDecision,
) -> None:
    """確認 Raw Load Decision、Payload 與來源內容完全一致。"""
    scope = raw_load_contract.validate_raw_load_scope(payload, comtrade_query)

    if decision.action != "INITIAL_LOAD" or decision.existing_record_count != 0:
        raise ValueError("Raw Load Decision 不允許首次載入")

    if decision.scope != scope:
        raise ValueError("Raw Load Decision 與 Payload Scope 不一致")

    if decision.source_assurance_level != "BOUNDED_PREVIEW":
        raise ValueError("Source Assurance Level 不符合 MVP 規則")

    source_records = [
        json.loads(record["source_record_json"])
        for record in payload.records
    ]
    records_sha256 = hashlib.sha256(
        manifest.serialize_records(source_records)
    ).hexdigest()

    if not re.fullmatch(r"[0-9a-f]{64}", records_sha256):
        raise ValueError("來源 SHA-256 格式無效")

    logger.info(
        "Raw Load Input 驗證成功：batch_id=%s records=%d",
        scope.batch_id,
        scope.record_count,
    )


def build_revision_id(
    scope: raw_load_contract.RawLoadScope,
    source_sha256: str,
) -> str:
    """根據來源 Query Key 與內容 Checksum 建立固定的 Revision ID。"""
    value = f"{scope.source_query_key}:{source_sha256}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_raw_job_id(
    scope: raw_load_contract.RawLoadScope,
    revision_id: str,
) -> str:
    """根據 Batch、Query Scope 與 Revision 建立固定的 BigQuery Job ID。"""
    value = f"{scope.batch_id}:{scope.source_query_key}:{revision_id}"
    return f"raw_{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def build_raw_insert_sql(
    config: bigquery_config.BigQueryConfig,
) -> str:
    """建立帶有資料庫端驗證與交易保護的首次 Raw INSERT SQL。"""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", config.project_id):
        raise ValueError("不合法的 BigQuery Project ID")

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", config.raw_dataset):
        raise ValueError("不合法的 BigQuery Dataset ID")

    landing = f"`{config.raw_table(bigquery_schema.LANDING_TABLE)}`"
    raw = f"`{config.raw_table(bigquery_schema.RAW_TABLE)}`"

    return f"""
    CREATE TEMP TABLE incoming AS
    SELECT *
    FROM {landing}
    WHERE batch_id = @batch_id
      AND source_query_key = @source_query_key;

    ASSERT (
        SELECT COUNT(*) = @expected_count
        FROM incoming
    ) AS 'Landing Record Count 不一致';

    ASSERT NOT EXISTS (
        SELECT 1
        FROM incoming
        WHERE period != @period
           OR reporter_code != @reporter_code
           OR cmd_code != @cmd_code
           OR query_type != @query_type
           OR flow_code != @flow_code
           OR freq_code != 'M'
           OR partner2_code != 0
           OR customs_code != 'C00'
           OR mot_code != 0
           OR is_original_classification IS NOT TRUE
           OR (
               @query_type = 'world_total'
               AND partner_code != 0
           )
           OR (
               @query_type = 'partner_detail'
               AND partner_code = 0
           )
           OR (
               hs_version IS NOT NULL
               AND hs_version != classification_code
           )
    ) AS 'Landing Query Scope 不一致';

    ASSERT (
        SELECT COUNT(DISTINCT source_row_index) = @expected_count
           AND MIN(source_row_index) = 0
           AND MAX(source_row_index) = @expected_count - 1
        FROM incoming
    ) AS 'Landing Source Row Index 不一致';

    ASSERT NOT EXISTS (
        SELECT 1
        FROM incoming
        GROUP BY
            period, reporter_code, partner_code, cmd_code,
            flow_code, classification_code, freq_code,
            partner2_code, customs_code, mot_code,
            is_original_classification
        HAVING COUNT(*) > 1
    ) AS 'Landing Record Business Key 重複';

    BEGIN TRANSACTION;

    -- 取得 Mutating Transaction 的寫入鎖，不刪除既有資料。
    DELETE FROM {raw} WHERE FALSE;

    ASSERT NOT EXISTS (
        SELECT 1
        FROM {raw}
        WHERE period_start_date = PARSE_DATE('%Y%m', @period)
          AND period = @period
          AND reporter_code = @reporter_code
          AND cmd_code = @cmd_code
          AND query_type = @query_type
          AND flow_code = @flow_code
    ) AS 'Raw Scope 已存在，拒絕首次載入';

    INSERT INTO {raw} (
        period_start_date,
        period,
        reporter_code,
        partner_code,
        cmd_code,
        primary_value,
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        net_weight,
        quantity,
        source_record_json,
        source_row_index,
        batch_id,
        source_query_key,
        revision_id,
        ingested_at
    )
    SELECT
        PARSE_DATE('%Y%m', period),
        period,
        reporter_code,
        partner_code,
        cmd_code,
        CAST(primary_value_text AS BIGNUMERIC),
        query_type,
        flow_code,
        classification_code,
        hs_version,
        freq_code,
        partner2_code,
        customs_code,
        mot_code,
        is_original_classification,
        CAST(net_weight_text AS BIGNUMERIC),
        CAST(quantity_text AS BIGNUMERIC),
        source_record_json,
        source_row_index,
        batch_id,
        source_query_key,
        @revision_id,
        CURRENT_TIMESTAMP()
    FROM incoming;

    ASSERT (
        SELECT COUNT(*) = @expected_count
        FROM {raw}
        WHERE period_start_date = PARSE_DATE('%Y%m', @period)
          AND period = @period
          AND reporter_code = @reporter_code
          AND cmd_code = @cmd_code
          AND query_type = @query_type
          AND flow_code = @flow_code
          AND batch_id = @batch_id
          AND source_query_key = @source_query_key
          AND revision_id = @revision_id
    ) AS 'Raw Insert Record Count 不一致';

    COMMIT TRANSACTION;
    """


def build_raw_query_config(
    scope: raw_load_contract.RawLoadScope,
    revision_id: str,
) -> bigquery.QueryJobConfig:
    """建立具備型別約束的 BigQuery Query Parameters。"""
    values = {
        "batch_id": scope.batch_id,
        "source_query_key": scope.source_query_key,
        "period": scope.period,
        "reporter_code": scope.reporter_code,
        "cmd_code": scope.cmd_code,
        "query_type": scope.query_type,
        "flow_code": scope.flow_code,
        "expected_count": scope.record_count,
        "revision_id": revision_id,
    }

    integer_fields = {"reporter_code", "expected_count"}

    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                name,
                "INT64" if name in integer_fields else "STRING",
                value,
            )
            for name, value in values.items()
        ]
    )


def load_initial_raw(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    payload: landing_payload.LandingPayload,
    comtrade_query: query.ComtradeQuery,
    decision: raw_load_decision.RawLoadDecision,
    source_sha256: str,
    *,
    allow_bounded_preview: bool = False,
) -> RawInitialLoadResult:
    """使用固定 Job ID 與 Transaction 執行首次 Raw 載入。"""
    validate_raw_load_inputs(payload, comtrade_query, decision)

    if not allow_bounded_preview:
        raise ValueError("必須明確允許 BOUNDED_PREVIEW 才能首次載入 Raw")

    source_records = [
        json.loads(record["source_record_json"])
        for record in payload.records
    ]
    actual_sha256 = hashlib.sha256(
        manifest.serialize_records(source_records)
    ).hexdigest()

    if source_sha256 != actual_sha256:
        raise ValueError("Source Quality Report 與 Payload Checksum 不一致")

    scope = decision.scope
    revision_id = build_revision_id(scope, source_sha256)
    job_id = build_raw_job_id(scope, revision_id)
    table_id = config.raw_table(bigquery_schema.RAW_TABLE)

    sql = build_raw_insert_sql(config)
    job_config = build_raw_query_config(scope, revision_id)

    logger.info(
        "開始 Raw Initial Load：job_id=%s batch_id=%s records=%d",
        job_id,
        scope.batch_id,
        scope.record_count,
    )

    try:
        job = client.query(
            sql,
            job_id=job_id,
            location=config.location,
            job_config=job_config,
            job_retry=None,
        )
    except Exception as exc:
        logger.exception("Raw Load 提交狀態未知：job_id=%s", job_id)
        raise RawInitialLoadStateUnknown(
            f"無法確認 Raw Load 是否已提交：job_id={job_id}"
        ) from exc

    try:
        job.result()
    except Exception as exc:
        if job.error_result:
            logger.error(
                "Raw Initial Load 失敗：job_id=%s error=%s",
                job_id,
                job.error_result,
            )
            raise RawInitialLoadError(
                f"Raw Initial Load 失敗：job_id={job_id}"
            ) from exc

        raise RawInitialLoadStateUnknown(
            f"無法確認 Raw Load 最終狀態：job_id={job_id}"
        ) from exc

    if job.job_id != job_id or job.error_result:
        raise RawInitialLoadStateUnknown(
            f"Raw Load Job 驗證失敗：job_id={job_id}"
        )

    logger.info(
        "Raw Initial Load 成功：job_id=%s table=%s records=%d",
        job_id,
        table_id,
        scope.record_count,
    )

    return RawInitialLoadResult(
        batch_id=scope.batch_id,
        source_query_key=scope.source_query_key,
        revision_id=revision_id,
        job_id=job_id,
        table_id=table_id,
        record_count=scope.record_count,
    )
