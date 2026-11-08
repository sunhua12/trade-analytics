
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from google.cloud import bigquery

from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload, ndjson_serializer
from trade_analytics.warehouse import (
    bigquery_client,
    bigquery_config,
    bigquery_loader,
    bigquery_schema,
    ingestion_coordinator,
)


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_integration_flag():
    """避免一般 pytest 意外提交真實 BigQuery Jobs。"""
    if os.getenv("RUN_BIGQUERY_INTEGRATION") != "1":
        pytest.skip("設定 RUN_BIGQUERY_INTEGRATION=1 才執行 GCP Integration Tests")


@pytest.fixture
def integration_env(monkeypatch):
    """建立隔離測試表，並在測試後清除本次建立的資源。"""
    config = bigquery_config.load_bigquery_config()
    client = bigquery_client.create_bigquery_client(config)
    suffix = uuid.uuid4().hex[:12]
    landing_name = f"it_landing_{suffix}"
    metadata_name = f"it_ingestion_{suffix}"
    created_tables = []

    try:
        dataset = client.get_dataset(
            f"{config.project_id}.{config.raw_dataset}"
        )
        if dataset.location.lower() != config.location.lower():
            raise ValueError("BigQuery Dataset Location 與 Config 不一致")

        expires = datetime.now(timezone.utc) + timedelta(hours=2)

        for name, schema in (
            (landing_name, bigquery_schema.LANDING_SCHEMA),
            (metadata_name, bigquery_schema.INGESTION_RUNS_SCHEMA),
        ):
            table_id = config.raw_table(name)
            table = bigquery.Table(table_id, schema=schema)
            table.expires = expires
            client.create_table(table)
            created_tables.append(table_id)

        monkeypatch.setattr(bigquery_schema, "LANDING_TABLE", landing_name)
        monkeypatch.setattr(
            bigquery_schema, "INGESTION_RUNS_TABLE", metadata_name
        )

        yield client, config

    finally:
        for table_id in reversed(created_tables):
            client.delete_table(table_id, not_found_ok=True)
        client.close()


@pytest.fixture
def sample_batch():
    """建立符合 Stage 5 Data Contract 的兩筆來源紀錄。"""
    comtrade_query = query.ComtradeQuery(
        reporter_code=842,
        period="202401",
        cmd_code="854231",
        query_type=query.QueryType.PARTNER_DETAIL,
    )

    def make_record(partner_code, primary_value):
        return {
            "period": "202401",
            "reporterCode": 842,
            "partnerCode": partner_code,
            "cmdCode": "854231",
            "primaryValue": Decimal(primary_value),
            "flowCode": "M",
            "classificationCode": "H6",
            "freqCode": "M",
            "partner2Code": 0,
            "customsCode": "C00",
            "motCode": 0,
            "isOriginalClassification": True,
            "netWgt": Decimal("123.450"),
            "qty": Decimal("10"),
        }

    return batch.BatchResult(
        query=comtrade_query,
        records=[
            make_record(156, "123456789.123456789"),
            make_record(392, "987654321.987654321"),
        ],
    )


def query_rows(client, config, table_id, batch_id):
    """依 Batch ID 查詢測試資料表。"""
    sql = f"SELECT * FROM `{table_id}` WHERE batch_id = @batch_id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)
        ]
    )
    return list(
        client.query(
            sql,
            job_config=job_config,
            location=config.location,
        ).result()
    )


def wait_for_rows(client, config, table_id, batch_id, expected_count):
    """等待資料可被查詢，並回傳符合預期筆數的紀錄。"""
    for attempt in range(10):
        rows = query_rows(client, config, table_id, batch_id)
        if len(rows) >= expected_count:
            return rows
        if attempt < 9:
            time.sleep(2)

    raise AssertionError(
        f"查詢筆數不足：table={table_id} "
        f"expected={expected_count} actual={len(rows)}"
    )


def test_end_to_end_landing_ingestion(integration_env, sample_batch):
    """驗證完整載入、Metadata、Decimal 與相同 Job ID 重試。"""
    client, config = integration_env
    batch_id = f"it-{uuid.uuid4().hex}"

    payload = landing_payload.build_landing_payload(sample_batch, batch_id)
    serialized = ndjson_serializer.serialize_landing_payload(payload)

    result = ingestion_coordinator.run_landing_load_with_metadata(
        client, config, serialized
    )

    landing_table = config.raw_table(bigquery_schema.LANDING_TABLE)
    metadata_table = config.raw_table(bigquery_schema.INGESTION_RUNS_TABLE)

    assert result.table_id == landing_table
    assert result.loaded_record_count == 2
    assert result.sha256 == serialized.sha256

    # 驗證 BigQuery Landing 資料。
    landing_rows = wait_for_rows(
        client, config, landing_table, batch_id, expected_count=2
    )
    assert len(landing_rows) == 2

    by_partner = {row.partner_code: row for row in landing_rows}
    assert set(by_partner) == {156, 392}
    assert by_partner[156].primary_value_text == "123456789.123456789"
    assert by_partner[392].primary_value_text == "987654321.987654321"
    assert by_partner[156].source_row_index == 0
    assert by_partner[392].source_row_index == 1

    source_json = json.loads(by_partner[156].source_record_json)
    assert source_json["partnerCode"] == 156
    assert source_json["primaryValue"] == "123456789.123456789"

    for row in landing_rows:
        assert row.source_query_key == serialized.source_query_key
        assert row.query_type == serialized.query_type
        assert row.batch_id == batch_id

    # 驗證 STARTED 與 SUCCEEDED Metadata。
    metadata_rows = wait_for_rows(
        client, config, metadata_table, batch_id, expected_count=2
    )

    assert len(metadata_rows) == 2
    assert {row.status for row in metadata_rows} == {
        "STARTED",
        "SUCCEEDED",
    }

    for row in metadata_rows:
        assert row.load_job_id == result.job_id
        assert row.sha256 == serialized.sha256
        assert row.record_count == 2
        assert row.size_bytes == serialized.size_bytes

    succeeded = next(
        row for row in metadata_rows if row.status == "SUCCEEDED"
    )
    assert succeeded.loaded_record_count == 2
    assert succeeded.completed_at is not None
    assert succeeded.error_message is None

    # 相同 Payload、相同 Job ID，再次呼叫 Loader。
    replay_result = bigquery_loader.load_landing_payload(
        client, config, serialized
    )

    assert replay_result.job_id == result.job_id
    assert replay_result.loaded_record_count == 2

    # 確認重用 Job 沒有向 Landing 追加第二份資料。
    replay_rows = query_rows(
        client, config, landing_table, batch_id
    )
    assert len(replay_rows) == 2
