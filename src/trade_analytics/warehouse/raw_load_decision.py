
import logging
import re
from dataclasses import dataclass

from google.cloud import bigquery

from trade_analytics.contracts import raw_load_contract, source_completeness
from trade_analytics.warehouse import bigquery_config, bigquery_schema


logger = logging.getLogger(__name__)


class RawScopeAlreadyExists(ValueError):
    """表示指定 Raw Scope 已存在資料，不允許首次載入。"""


@dataclass(frozen=True)
class RawLoadDecision:
    """記錄首次 Raw Load 的來源範圍及載入決策。"""

    scope: raw_load_contract.RawLoadScope
    action: str
    existing_record_count: int
    source_assurance_level: str


def validate_source_evidence(
    scope: raw_load_contract.RawLoadScope,
    report: source_completeness.SourceQualityReport,
) -> None:
    """確認來源驗證報告與本次 Raw Scope 一致。"""
    if scope.record_count <= 0:
        raise ValueError("Raw Load 不接受空批次")

    if report.source_query_key != scope.source_query_key:
        raise ValueError("Source Quality Report 的 Query Key 不一致")

    if report.accepted_record_count != scope.record_count:
        raise ValueError("Source Quality Report 的 Record Count 不一致")

    if report.response_count < report.accepted_record_count:
        raise ValueError("來源 Response Count 小於有效紀錄數")

    if report.record_limit <= 0 or report.response_count >= report.record_limit:
        raise ValueError("來源 Response 未通過 Record Limit 驗證")

    if report.assurance_level != "BOUNDED_PREVIEW":
        raise ValueError("不支援的 Source Assurance Level")

    if not re.fullmatch(r"[0-9a-f]{64}", report.records_sha256):
        raise ValueError("Source Quality Report 的 SHA-256 無效")


def count_existing_raw_records(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    scope: raw_load_contract.RawLoadScope,
) -> int:
    """查詢 BigQuery Raw 中指定來源範圍已存在的紀錄數。"""
    table_id = config.raw_table(bigquery_schema.RAW_TABLE)

    if not re.fullmatch(r"[A-Za-z0-9_-]+", config.project_id) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*", config.raw_dataset
    ):
        raise ValueError("不合法的 BigQuery Table Identifier")

    sql = f"""
        SELECT COUNT(*) AS record_count
        FROM `{table_id}`
        WHERE period_start_date = @period_start_date
          AND period = @period
          AND reporter_code = @reporter_code
          AND cmd_code = @cmd_code
          AND query_type = @query_type
          AND flow_code = @flow_code
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

    rows = list(
        client.query(
            sql,
            job_config=job_config,
            location=config.location,
        ).result()
    )

    if len(rows) != 1:
        raise ValueError("Raw Scope Count Query 回傳筆數異常")

    count = rows[0]["record_count"]
    if type(count) is not int or count < 0:
        raise ValueError("Raw Scope Count Query 回傳無效數值")

    return count


def decide_initial_raw_load(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    scope: raw_load_contract.RawLoadScope,
    report: source_completeness.SourceQualityReport,
) -> RawLoadDecision:
    """驗證來源證據並判斷指定 Raw Scope 是否允許首次載入。"""
    validate_source_evidence(scope, report)

    existing_count = count_existing_raw_records(client, config, scope)

    if existing_count > 0:
        logger.warning(
            "Raw Scope 已存在：period=%s reporter=%d cmd=%s "
            "query_type=%s records=%d",
            scope.period,
            scope.reporter_code,
            scope.cmd_code,
            scope.query_type,
            existing_count,
        )
        raise RawScopeAlreadyExists(
            f"Raw Scope 已存在 {existing_count} 筆紀錄，拒絕首次載入"
        )

    logger.info(
        "Raw Initial Load 檢查通過：period=%s reporter=%d "
        "cmd=%s query_type=%s records=%d",
        scope.period,
        scope.reporter_code,
        scope.cmd_code,
        scope.query_type,
        scope.record_count,
    )

    return RawLoadDecision(
        scope=scope,
        action="INITIAL_LOAD",
        existing_record_count=0,
        source_assurance_level=report.assurance_level,
    )
