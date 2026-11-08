
import argparse
import logging
import os
import uuid
from dataclasses import dataclass

from google.cloud import bigquery

from trade_analytics.contracts import (
    raw_load_contract,
    source_completeness,
)
from trade_analytics.ingestion import query, real_source
from trade_analytics.processing import (
    landing_payload,
    ndjson_serializer,
)
from trade_analytics.warehouse import (
    bigquery_config,
    ingestion_coordinator,
    raw_load_coordinator,
    raw_load_decision,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmokeResult:
    """Production Smoke Test 的執行結果。"""

    batch_id: str
    record_count: int
    source_sha256: str
    applied: bool
    raw_job_id: str | None = None


def build_parser() -> argparse.ArgumentParser:
    """建立 Smoke Test CLI 參數。"""
    parser = argparse.ArgumentParser(
        description="UN Comtrade Production Data Smoke Test"
    )

    parser.add_argument(
        "--period",
        required=True,
        help="YYYYMM，例如 202401",
    )

    parser.add_argument(
        "--reporter",
        required=True,
        type=int,
        help="UN Comtrade Reporter Code",
    )

    parser.add_argument(
        "--cmd",
        required=True,
        help="HS Commodity Code，例如 854231",
    )

    parser.add_argument(
        "--query-type",
        required=True,
        choices=["partner_detail", "world_total"],
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="明確允許實際寫入正式 BigQuery",
    )

    parser.add_argument(
        "--allow-bounded-preview",
        action="store_true",
        help="明確接受 Preview API 的完整性限制",
    )

    return parser


def run_production_smoke(
    client: bigquery.Client,
    config: bigquery_config.BigQueryConfig,
    comtrade_query: query.ComtradeQuery,
    *,
    apply: bool = False,
    allow_bounded_preview: bool = False,
) -> SmokeResult:
    """擷取、驗證真實 API 資料，必要時寫入 Landing 與 Raw。"""

    # 第一層安全防護：沒有兩個旗標就不允許正式寫入。
    if apply and not allow_bounded_preview:
        raise ValueError(
            "正式載入必須同時指定 --apply "
            "與 --allow-bounded-preview"
        )

    # Step 1：取得真實 API 資料。
    source = real_source.fetch_real_source(
        comtrade_query
    )

    # Step 2：驗證來源完整性、附加欄位及候選 Grain。
    report = source_completeness.validate_source_completeness(
        result=source.batch_result,
        response_payload=source.response_payload,
        approved_non_grain_fields=(
            source_completeness.APPROVED_SOURCE_NON_GRAIN_FIELDS
        ),
        require_reference_fields=True,
    )

    # Step 3：建立本次 Batch 的 Landing Payload。
    batch_id = f"smoke-{uuid.uuid4().hex}"

    payload = landing_payload.build_landing_payload(
        source.batch_result,
        batch_id=batch_id,
    )

    # Step 4：檢查 Raw Scope。
    scope = raw_load_contract.validate_raw_load_scope(
        payload,
        comtrade_query,
    )

    # Step 5：完成 Landing NDJSON Serialization。
    serialized = ndjson_serializer.serialize_landing_payload(
        payload
    )

    logger.info(
        "來源驗證成功：batch_id=%s count=%d "
        "source_sha256=%s assurance=%s",
        batch_id,
        report.accepted_record_count,
        report.records_sha256,
        report.assurance_level,
    )

    logger.info(
        "來源附加欄位已審查：%s",
        report.unmodeled_fields,
    )

    logger.info(
        "Landing Serialization 完成："
        "size_bytes=%d ndjson_sha256=%s",
        serialized.size_bytes,
        serialized.sha256,
    )

    # Step 6：查詢目前 Raw 是否已存在相同 Scope。
    # 這是唯讀 BigQuery 查詢，不會更新任何資料。
    raw_load_decision.decide_initial_raw_load(
        client,
        config,
        scope,
        report,
    )

    # Step 7A：Dry Run 到此結束。
    if not apply:
        logger.info(
            "DRY RUN 成功，未寫入 BigQuery："
            "batch_id=%s records=%d",
            batch_id,
            payload.record_count,
        )

        return SmokeResult(
            batch_id=batch_id,
            record_count=payload.record_count,
            source_sha256=report.records_sha256,
            applied=False,
        )

    # Step 7B：正式寫入 Landing。
    # 只有 --apply + --allow-bounded-preview 才會執行到此。
    logger.warning(
        "正式資料載入已授權：batch_id=%s "
        "period=%s reporter=%s cmd=%s",
        batch_id,
        scope.period,
        scope.reporter_code,
        scope.cmd_code,
    )

    landing_result = (
        ingestion_coordinator.run_landing_load_with_metadata(
            client,
            config,
            serialized,
        )
    )

    if landing_result.loaded_record_count != payload.record_count:
        raise RuntimeError(
            "Landing 實際載入筆數不一致，停止 Raw Load"
        )

    logger.info(
        "Landing Load 成功：job_id=%s records=%d",
        landing_result.job_id,
        landing_result.loaded_record_count,
    )

    # Step 8：透過 Stage 7.5 Coordinator 執行 Initial Raw Load。
    # Coordinator 會再次檢查 Raw Scope 並建立 Audit。
    raw_result = (
        raw_load_coordinator.run_initial_raw_with_audit(
            client,
            config,
            payload,
            comtrade_query,
            report,
            allow_bounded_preview=True,
        )
    )

    # Stage 7.5 已核對 Raw 筆數、Row Index 與來源 Checksum。
    if raw_result.record_count != payload.record_count:
        raise RuntimeError(
            "Raw 實際載入筆數不一致"
        )

    logger.info(
        "PRODUCTION SMOKE SUCCESS："
        "batch_id=%s raw_job_id=%s records=%d",
        batch_id,
        raw_result.job_id,
        raw_result.record_count,
    )

    return SmokeResult(
        batch_id=batch_id,
        record_count=raw_result.record_count,
        source_sha256=report.records_sha256,
        applied=True,
        raw_job_id=raw_result.job_id,
    )


def main() -> None:
    """CLI Entry Point。"""
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s %(name)s：%(message)s"
        ),
    )

    config = bigquery_config.BigQueryConfig(
        project_id=os.environ["GCP_PROJECT_ID"],
        location=os.getenv(
            "BQ_LOCATION",
            "asia-northeast1",
        ),
        raw_dataset=os.getenv(
            "BQ_RAW_DATASET",
            "trade_raw",
        ),
        analytics_dataset=os.getenv(
            "BQ_ANALYTICS_DATASET",
            "trade_analytics",
        ),
    )

    comtrade_query = query.ComtradeQuery(
        period=args.period,
        reporter_code=args.reporter,
        cmd_code=args.cmd,
        query_type=query.QueryType(args.query_type),
    )

    with bigquery.Client(
        project=config.project_id,
        location=config.location,
    ) as client:
        result = run_production_smoke(
            client,
            config,
            comtrade_query,
            apply=args.apply,
            allow_bounded_preview=args.allow_bounded_preview,
        )

    logger.info(
        "Smoke Test 完成：applied=%s records=%d batch_id=%s",
        result.applied,
        result.record_count,
        result.batch_id,
    )


if __name__ == "__main__":
    main()
