
import hashlib
from dataclasses import replace
from unittest.mock import Mock

import pytest
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_loader,
    bigquery_schema,
)


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
def mock_client(config, payload):
    client = Mock(spec=bigquery.Client)
    job = Mock()
    job.job_id = bigquery_loader.build_load_job_id(
        payload, config.raw_table(bigquery_schema.LANDING_TABLE)
    )
    job.state = "DONE"
    job.error_result = None
    job.errors = None
    job.output_rows = 1
    client.load_table_from_file.return_value = job
    client.get_job.return_value = job
    return client


def test_build_load_job_config():
    job_config = bigquery_loader.build_load_job_config()

    assert job_config.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert job_config.create_disposition == bigquery.CreateDisposition.CREATE_NEVER
    assert job_config.schema == bigquery_schema.LANDING_SCHEMA
    assert job_config.max_bad_records == 0
    assert job_config.ignore_unknown_values is False


def test_deterministic_job_id(config, payload):
    table_id = config.raw_table(bigquery_schema.LANDING_TABLE)
    first = bigquery_loader.build_load_job_id(payload, table_id)
    second = bigquery_loader.build_load_job_id(payload, table_id)

    assert first == second
    assert first.startswith("landing_")


def test_job_id_changes_when_content_changes(config, payload):
    table_id = config.raw_table(bigquery_schema.LANDING_TABLE)
    changed = replace(payload, sha256="another-checksum")

    assert bigquery_loader.build_load_job_id(payload, table_id) != (
        bigquery_loader.build_load_job_id(changed, table_id)
    )


def test_load_landing_payload(mock_client, config, payload):
    result = bigquery_loader.load_landing_payload(mock_client, config, payload)

    assert result.batch_id == payload.batch_id
    assert result.source_query_key == payload.source_query_key
    assert result.loaded_record_count == 1
    assert result.sha256 == payload.sha256
    assert result.job_id == mock_client.load_table_from_file.return_value.job_id


def test_load_job_arguments(mock_client, config, payload):
    bigquery_loader.load_landing_payload(mock_client, config, payload)
    args, kwargs = mock_client.load_table_from_file.call_args

    assert args[0].getvalue() == payload.ndjson_bytes
    assert kwargs["destination"] == config.raw_table(bigquery_schema.LANDING_TABLE)
    assert kwargs["size"] == payload.size_bytes
    assert kwargs["location"] == config.location
    assert kwargs["project"] == config.project_id
    assert kwargs["job_id"] == mock_client.load_table_from_file.return_value.job_id


def test_job_waits_for_completion(mock_client, config, payload):
    bigquery_loader.load_landing_payload(mock_client, config, payload)

    mock_client.load_table_from_file.return_value.result.assert_called_once_with()


def test_existing_job_reused(mock_client, config, payload):
    mock_client.load_table_from_file.side_effect = google_exceptions.Conflict(
        "Job already exists"
    )

    result = bigquery_loader.load_landing_payload(mock_client, config, payload)

    assert result.loaded_record_count == 1
    mock_client.get_job.assert_called_once_with(
        result.job_id, project=config.project_id, location=config.location
    )


def test_job_result_exception_recovered(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.result.side_effect = ConnectionError("Connection lost")

    result = bigquery_loader.load_landing_payload(mock_client, config, payload)

    assert result.loaded_record_count == 1
    mock_client.get_job.assert_called_once()


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_incomplete_job_state_rejected(mock_client, config, payload, state):
    job = mock_client.load_table_from_file.return_value
    job.state = state

    with pytest.raises(bigquery_loader.LoadJobStateUnknown):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


def test_failed_job_rejected(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.error_result = {"reason": "invalid", "message": "Invalid data"}

    with pytest.raises(bigquery_loader.LoadJobFailed, match="Load Job 失敗"):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


def test_nonfatal_errors_do_not_fail_job(mock_client, config, payload, caplog):
    job = mock_client.load_table_from_file.return_value
    job.errors = [{"reason": "warning", "message": "Nonfatal information"}]

    with caplog.at_level("WARNING"):
        result = bigquery_loader.load_landing_payload(mock_client, config, payload)

    assert result.loaded_record_count == 1
    assert "非致命" in caplog.text


def test_missing_output_rows_rejected(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.output_rows = None

    with pytest.raises(bigquery_loader.LoadJobStateUnknown):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


def test_row_count_mismatch_rejected(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.output_rows = 0

    with pytest.raises(bigquery_loader.LoadJobRowCountMismatch):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


def test_submission_exception_unknown(mock_client, config, payload):
    mock_client.load_table_from_file.side_effect = ConnectionError("Connection lost")

    with pytest.raises(bigquery_loader.LoadJobStateUnknown):
        bigquery_loader.load_landing_payload(mock_client, config, payload)

    mock_client.get_job.assert_not_called()


def test_job_lookup_failure_unknown(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.result.side_effect = ConnectionError("Connection lost")
    mock_client.get_job.side_effect = google_exceptions.ServiceUnavailable(
        "BigQuery unavailable"
    )

    with pytest.raises(bigquery_loader.LoadJobStateUnknown):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


def test_mismatched_job_id_rejected(mock_client, config, payload):
    job = mock_client.load_table_from_file.return_value
    job.job_id = "wrong-job-id"

    with pytest.raises(bigquery_loader.LoadJobStateUnknown, match="Job ID 不一致"):
        bigquery_loader.load_landing_payload(mock_client, config, payload)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"record_count": 0}, "空的 Landing Payload"),
        ({"batch_id": ""}, "識別資訊"),
        ({"size_bytes": 999}, "Byte Size 不一致"),
        ({"sha256": "invalid"}, "SHA-256 不一致"),
        ({"record_count": 2}, "Record Count 不一致"),
    ],
)
def test_invalid_payload_rejected(mock_client, config, payload, changes, message):
    invalid_payload = replace(payload, **changes)

    with pytest.raises(ValueError, match=message):
        bigquery_loader.load_landing_payload(mock_client, config, invalid_payload)

    mock_client.load_table_from_file.assert_not_called()

def test_validate_successful_job():
    job = Mock(spec=bigquery.LoadJob)
    job.job_id = "test-job"
    job.state = "DONE"
    job.error_result = None
    job.errors = None
    job.output_rows = 10

    result = bigquery_loader.validate_load_job_result(job, expected_count=10)

    assert result == 10