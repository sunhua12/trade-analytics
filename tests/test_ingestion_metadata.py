
import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import bigquery_config, bigquery_schema, ingestion_metadata


@pytest.fixture
def config():
    return bigquery_config.BigQueryConfig(
        project_id="test-project",
        location="asia-northeast1",
    )


@pytest.fixture
def payload():
    data = b'{"batch_id":"batch-001"}\n'
    return ndjson_serializer.SerializedLandingPayload(
        batch_id="batch-001",
        source_query_key="query-key-001",
        query_type="partner_detail",
        ndjson_bytes=data,
        record_count=1,
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


@pytest.fixture
def started_at():
    return datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def mock_client():
    client = Mock(spec=bigquery.Client)
    client.insert_rows_json.return_value = []
    return client


def test_build_started_row(payload, started_at):
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    assert row["status"] == "STARTED"
    assert row["batch_id"] == payload.batch_id
    assert row["source_query_key"] == payload.source_query_key
    assert row["revision_id"] is None
    assert row["record_count"] == 1
    assert row["loaded_record_count"] is None
    assert row["completed_at"] is None
    assert row["sha256"] == payload.sha256


def test_build_succeeded_row(payload, started_at):
    row = ingestion_metadata.build_ingestion_row(
        payload,
        "job-001",
        "SUCCEEDED",
        started_at,
        completed_at=started_at,
        loaded_record_count=1,
    )

    assert row["status"] == "SUCCEEDED"
    assert row["loaded_record_count"] == 1
    assert row["completed_at"] is not None
    assert row["error_message"] is None


def test_metadata_matches_table_schema(payload, started_at):
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    assert set(row) == {
        field.name for field in bigquery_schema.INGESTION_RUNS_SCHEMA
    }


def test_timestamp_normalized_to_utc(payload, started_at):
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    assert datetime.fromisoformat(row["started_at"]) == started_at


@pytest.mark.parametrize("status", ["FAILED", "VALIDATION_FAILED", "UNKNOWN"])
def test_error_status(payload, started_at, status):
    row = ingestion_metadata.build_ingestion_row(
        payload,
        "job-001",
        status,
        started_at,
        completed_at=started_at,
        error_message="test error",
    )

    assert row["status"] == status
    assert row["error_message"] == "test error"
    assert row["loaded_record_count"] is None


def test_invalid_status_rejected(payload, started_at):
    with pytest.raises(ValueError, match="不支援的 Ingestion Status"):
        ingestion_metadata.build_ingestion_row(
            payload, "job-001", "INVALID", started_at
        )


def test_missing_job_id_rejected(payload, started_at):
    with pytest.raises(ValueError, match="load_job_id 不可為空"):
        ingestion_metadata.build_ingestion_row(
            payload, "", "STARTED", started_at
        )


def test_naive_timestamp_rejected(payload):
    naive = datetime(2026, 11, 8, 12, 0)

    with pytest.raises(ValueError, match="必須包含時區"):
        ingestion_metadata.build_ingestion_row(
            payload, "job-001", "STARTED", naive
        )


def test_final_status_requires_completed_at(payload, started_at):
    with pytest.raises(ValueError, match="最終狀態必須"):
        ingestion_metadata.build_ingestion_row(
            payload, "job-001", "SUCCEEDED", started_at
        )


def test_negative_loaded_count_rejected(payload, started_at):
    with pytest.raises(ValueError, match="不可為負數"):
        ingestion_metadata.build_ingestion_row(
            payload,
            "job-001",
            "SUCCEEDED",
            started_at,
            completed_at=started_at,
            loaded_record_count=-1,
        )


def test_write_metadata(mock_client, config, payload, started_at):
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    ingestion_metadata.write_ingestion_metadata(mock_client, config, row)

    mock_client.insert_rows_json.assert_called_once_with(
        config.raw_table(bigquery_schema.INGESTION_RUNS_TABLE),
        [row],
        skip_invalid_rows=False,
        ignore_unknown_values=False,
    )


def test_streaming_insert_errors_rejected(
    mock_client, config, payload, started_at
):
    mock_client.insert_rows_json.return_value = [
        {"index": 0, "errors": [{"reason": "invalid"}]}
    ]
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    with pytest.raises(ingestion_metadata.MetadataWriteError):
        ingestion_metadata.write_ingestion_metadata(mock_client, config, row)


def test_streaming_exception_wrapped(mock_client, config, payload, started_at):
    mock_client.insert_rows_json.side_effect = ConnectionError("Connection lost")
    row = ingestion_metadata.build_ingestion_row(
        payload, "job-001", "STARTED", started_at
    )

    with pytest.raises(ingestion_metadata.MetadataWriteError):
        ingestion_metadata.write_ingestion_metadata(mock_client, config, row)
