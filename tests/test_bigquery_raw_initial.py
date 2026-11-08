import hashlib
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from google.cloud import bigquery

from trade_analytics.contracts import (
    raw_load_contract,
    source_completeness,
)

from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_schema,
    raw_initial_loader,
    raw_load_coordinator,
    raw_load_decision,
)

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class WarehouseTestTables:
    client: bigquery.Client
    config: bigquery_config.BigQueryConfig
    landing_id: str
    raw_id: str
    audit_id: str


@pytest.fixture
def warehouse(monkeypatch):
    if os.getenv("RUN_BIGQUERY_INTEGRATION") != "1":
        pytest.skip("設定 RUN_BIGQUERY_INTEGRATION=1 才執行")

    config = bigquery_config.BigQueryConfig(
        project_id=os.environ["GCP_PROJECT_ID"],
        location=os.getenv("BQ_LOCATION", "asia-northeast1"),
    )
    client = bigquery.Client(
        project=config.project_id,
        location=config.location,
    )

    suffix = uuid.uuid4().hex[:12]
    landing_name = f"it_landing_{suffix}"
    raw_name = f"it_raw_{suffix}"
    audit_name = f"it_ingestion_{suffix}"

    tables = WarehouseTestTables(
        client=client,
        config=config,
        landing_id=config.raw_table(landing_name),
        raw_id=config.raw_table(raw_name),
        audit_id=config.raw_table(audit_name),
    )

    created = []

    try:
        definitions = [
            (tables.landing_id, bigquery_schema.LANDING_SCHEMA),
            (tables.raw_id, bigquery_schema.RAW_SCHEMA),
            (tables.audit_id, bigquery_schema.INGESTION_RUNS_SCHEMA),
        ]

        for table_id, schema in definitions:
            table = bigquery.Table(table_id, schema=schema)
            table.expires = datetime.now(timezone.utc) + timedelta(hours=2)

            if table_id == tables.raw_id:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.MONTH,
                    field="period_start_date",
                )
                table.clustering_fields = ["reporter_code", "cmd_code"]

            client.create_table(table, exists_ok=False)
            created.append(table_id)

        # Stage 7 的 SQL 和 Metadata Writer 依照這三個常數
        # 取得表名；只在測試作用域內改為隔離表。
        monkeypatch.setattr(
            bigquery_schema, "LANDING_TABLE", landing_name
        )
        monkeypatch.setattr(
            bigquery_schema, "RAW_TABLE", raw_name
        )
        monkeypatch.setattr(
            bigquery_schema, "INGESTION_RUNS_TABLE", audit_name
        )

        yield tables

    finally:
        for table_id in reversed(created):
            client.delete_table(table_id, not_found_ok=True)
        client.close()


@pytest.fixture
def comtrade_query():
    return query.ComtradeQuery(
        reporter_code=842,
        period="202401",
        cmd_code="854231",
        query_type=query.QueryType.PARTNER_DETAIL,
    )


@pytest.fixture
def source_records():
    def record(partner, value):
        return {
            "period": "202401",
            "reporterCode": 842,
            "partnerCode": partner,
            "cmdCode": "854231",
            "primaryValue": Decimal(value),
            "flowCode": "M",
            "classificationCode": "H6",
            "freqCode": "M",
            "partner2Code": 0,
            "customsCode": "C00",
            "motCode": 0,
            "isOriginalClassification": True,
            "netWgt": Decimal("123.450000001"),
            "qty": Decimal("10.000000001"),
        }

    return [
        record(156, "123456789.123456789"),
        record(392, "987654321.987654321"),
    ]


@pytest.fixture
def source_bundle(comtrade_query, source_records):
    result = batch.BatchResult(
        query=comtrade_query,
        records=source_records,
    )

    payload = landing_payload.build_landing_payload(
        result,
        batch_id=f"it-batch-{uuid.uuid4().hex[:12]}",
    )

    checksum = hashlib.sha256(
        raw_load_coordinator.source_records_bytes(payload)
    ).hexdigest()

    report = source_completeness.SourceQualityReport(
        source_query_key=payload.source_query_key,
        response_count=len(source_records),
        accepted_record_count=len(source_records),
        discarded_world_total_count=0,
        record_limit=500,
        records_sha256=checksum,
        unmodeled_fields=(),
    )

    return payload, report


def execute_sql(warehouse, sql, parameters=None):
    job_config = bigquery.QueryJobConfig(
        query_parameters=parameters or []
    )
    return list(
        warehouse.client.query(
            sql,
            job_config=job_config,
            location=warehouse.config.location,
        ).result()
    )


def count_rows(warehouse, table_id):
    rows = execute_sql(
        warehouse,
        f"SELECT COUNT(*) AS n FROM `{table_id}`",
    )
    return rows[0]["n"]


def seed_landing(warehouse, payload):
    job = warehouse.client.load_table_from_json(
        payload.records,
        warehouse.landing_id,
        location=warehouse.config.location,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=bigquery_schema.LANDING_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            max_bad_records=0,
        ),
    )
    job.result()

    assert job.error_result is None
    assert count_rows(warehouse, warehouse.landing_id) == payload.record_count


def run_raw(warehouse, comtrade_query, source_bundle):
    payload, report = source_bundle

    return raw_load_coordinator.run_initial_raw_with_audit(
        warehouse.client,
        warehouse.config,
        payload,
        comtrade_query,
        report,
        allow_bounded_preview=True,
    )


def test_initial_raw_load_and_precision(
        warehouse, comtrade_query, source_bundle
):
    payload, report = source_bundle
    seed_landing(warehouse, payload)

    result = run_raw(warehouse, comtrade_query, source_bundle)

    assert result.record_count == 2
    assert result.table_id == warehouse.raw_id

    rows = execute_sql(
        warehouse,
        f"""
        SELECT partner_code, primary_value,
               net_weight, quantity,
               period_start_date, batch_id,
               revision_id, source_query_key
        FROM `{warehouse.raw_id}`
        ORDER BY partner_code
        """,
    )

    assert len(rows) == 2
    assert rows[0]["partner_code"] == 156
    assert Decimal(str(rows[0]["primary_value"])) == Decimal(
        "123456789.123456789"
    )
    assert Decimal(str(rows[1]["primary_value"])) == Decimal(
        "987654321.987654321"
    )
    assert Decimal(str(rows[0]["net_weight"])) == Decimal(
        "123.450000001"
    )
    assert Decimal(str(rows[0]["quantity"])) == Decimal(
        "10.000000001"
    )
    assert rows[0]["period_start_date"].isoformat() == "2024-01-01"
    assert all(row["batch_id"] == payload.batch_id for row in rows)
    assert all(
        row["source_query_key"] == report.source_query_key
        for row in rows
    )
    assert all(row["revision_id"] == result.revision_id for row in rows)

    audit_rows = execute_sql(
        warehouse,
        f"""
        SELECT status, loaded_record_count, load_job_id
        FROM `{warehouse.audit_id}`
        WHERE batch_id = @batch_id
        ORDER BY started_at, completed_at
        """,
        [
            bigquery.ScalarQueryParameter(
                "batch_id", "STRING", payload.batch_id
            )
        ],
    )

    assert len(audit_rows) == 2
    assert {row["status"] for row in audit_rows} == {
        "STARTED", "SUCCEEDED"
    }
    assert all(row["load_job_id"] == result.job_id for row in audit_rows)
    assert any(
        row["status"] == "SUCCEEDED"
        and row["loaded_record_count"] == 2
        for row in audit_rows
    )


def test_duplicate_scope_does_not_insert_twice(
        warehouse, comtrade_query, source_bundle
):
    seed_landing(warehouse, source_bundle[0])

    first = run_raw(warehouse, comtrade_query, source_bundle)
    assert count_rows(warehouse, warehouse.raw_id) == 2

    with pytest.raises(
            raw_initial_loader.RawInitialLoadError
    ):
        # 直接重放同一個固定 Job ID 所對應的 SQL，
        # 檢查資料庫端的 Scope Guard。
        # 使用新的 Job ID 才會實際再次執行 SQL。
        scope = raw_load_contract.validate_raw_load_scope(
            source_bundle[0], comtrade_query
        )
        config = raw_initial_loader.build_raw_query_config(
            scope, first.revision_id
        )
        job = warehouse.client.query(
            raw_initial_loader.build_raw_insert_sql(warehouse.config),
            job_id=f"it_replay_{uuid.uuid4().hex}",
            job_config=config,
            job_retry=None,
            location=warehouse.config.location,
        )
        try:
            job.result()
        except Exception as exc:
            raise raw_initial_loader.RawInitialLoadError(
                "Duplicate Raw Scope rejected"
            ) from exc

    assert count_rows(warehouse, warehouse.raw_id) == 2


def test_recovery_uses_existing_job(
        warehouse, comtrade_query, source_bundle
):
    payload, report = source_bundle
    seed_landing(warehouse, payload)

    first = run_raw(warehouse, comtrade_query, source_bundle)
    before = count_rows(warehouse, warehouse.raw_id)

    recovered = raw_load_coordinator.recover_initial_raw(
        warehouse.client,
        warehouse.config,
        payload,
        comtrade_query,
        report,
    )

    assert recovered.job_id == first.job_id
    assert recovered.revision_id == first.revision_id
    assert count_rows(warehouse, warehouse.raw_id) == before

    statuses = execute_sql(
        warehouse,
        f"""
        SELECT status
        FROM `{warehouse.audit_id}`
        WHERE load_job_id = @job_id
        """,
        [
            bigquery.ScalarQueryParameter(
                "job_id", "STRING", first.job_id
            )
        ],
    )

    assert sum(row["status"] == "SUCCEEDED" for row in statuses) == 2


def test_invalid_landing_rolls_back_raw(
        warehouse, comtrade_query, source_bundle
):
    payload, report = source_bundle
    seed_landing(warehouse, payload)

    # 此測試故意繞過 Python Validation，
    # 確認 BigQuery 本身拒絕無法轉成 BIGNUMERIC 的值。
    execute_sql(
        warehouse,
        f"""
        UPDATE `{warehouse.landing_id}`
        SET primary_value_text = 'not-a-decimal'
        WHERE batch_id = @batch_id
          AND source_row_index = 1
        """,
        [
            bigquery.ScalarQueryParameter(
                "batch_id", "STRING", payload.batch_id
            )
        ],
    )

    scope = raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )
    revision_id, _ = raw_load_coordinator.build_raw_identity(
        scope, report
    )

    job = warehouse.client.query(
        raw_initial_loader.build_raw_insert_sql(warehouse.config),
        job_id=f"it_bad_{uuid.uuid4().hex}",
        job_config=raw_initial_loader.build_raw_query_config(
            scope, revision_id
        ),
        job_retry=None,
        location=warehouse.config.location,
    )

    with pytest.raises(Exception):
        job.result()

    assert count_rows(warehouse, warehouse.raw_id) == 0


def test_coordinator_rejects_existing_scope(
        warehouse, comtrade_query, source_bundle
):
    seed_landing(warehouse, source_bundle[0])

    first = run_raw(warehouse, comtrade_query, source_bundle)
    assert first.record_count == 2

    with pytest.raises(raw_load_decision.RawScopeAlreadyExists):
        run_raw(warehouse, comtrade_query, source_bundle)

    assert count_rows(warehouse, warehouse.raw_id) == 2
