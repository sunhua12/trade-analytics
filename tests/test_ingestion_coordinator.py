
import hashlib
from unittest.mock import Mock, call, patch

import pytest
from google.cloud import bigquery

from trade_analytics.processing import ndjson_serializer
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_loader,
    ingestion_coordinator,
    ingestion_metadata,
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
def client():
    return Mock(spec=bigquery.Client)


@pytest.fixture
def load_result():
    return bigquery_loader.LandingLoadResult(
        batch_id="batch-001",
        source_query_key="query-key-001",
        job_id="job-001",
        table_id="test-project.trade_raw.comtrade_landing",
        loaded_record_count=1,
        sha256="test-checksum",
    )


def test_success_records_started_and_succeeded(
    client, config, payload, load_result
):
    with (
        patch.object(
            ingestion_coordinator.bigquery_loader,
            "load_landing_payload",
            return_value=load_result,
        ),
        patch.object(
            ingestion_coordinator,
            "record_ingestion_status",
        ) as record_status,
    ):
        result = ingestion_coordinator.run_landing_load_with_metadata(
            client, config, payload
        )

    assert result == load_result
    assert record_status.call_count == 2
    assert record_status.call_args_list[0].args[4] == "STARTED"
    assert record_status.call_args_list[1].args[4] == "SUCCEEDED"
    assert record_status.call_args_list[1].kwargs["loaded_record_count"] == 1


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (bigquery_loader.LoadJobFailed("failed"), "FAILED"),
        (
            bigquery_loader.LoadJobRowCountMismatch("row count mismatch"),
            "VALIDATION_FAILED",
        ),
        (bigquery_loader.LoadJobStateUnknown("unknown"), "UNKNOWN"),
        (ConnectionError("connection lost"), "UNKNOWN"),
    ],
)
def test_failure_records_correct_status(
    client, config, payload, error, expected_status
):
    with (
        patch.object(
            ingestion_coordinator.bigquery_loader,
            "load_landing_payload",
            side_effect=error,
        ),
        patch.object(
            ingestion_coordinator,
            "record_ingestion_status",
        ) as record_status,
    ):
        with pytest.raises(type(error)):
            ingestion_coordinator.run_landing_load_with_metadata(
                client, config, payload
            )

    assert record_status.call_count == 2
    assert record_status.call_args_list[1].args[4] == expected_status
    assert record_status.call_args_list[1].kwargs["error_message"] == str(error)


def test_started_metadata_failure_prevents_load(client, config, payload):
    with (
        patch.object(
            ingestion_coordinator,
            "record_ingestion_status",
            side_effect=ingestion_metadata.MetadataWriteError("write failed"),
        ),
        patch.object(
            ingestion_coordinator.bigquery_loader,
            "load_landing_payload",
        ) as load,
    ):
        with pytest.raises(ingestion_metadata.MetadataWriteError):
            ingestion_coordinator.run_landing_load_with_metadata(
                client, config, payload
            )

    load.assert_not_called()


def test_failed_metadata_write_preserves_load_error(client, config, payload):
    load_error = bigquery_loader.LoadJobFailed("job failed")

    with (
        patch.object(
            ingestion_coordinator.bigquery_loader,
            "load_landing_payload",
            side_effect=load_error,
        ),
        patch.object(
            ingestion_coordinator,
            "record_ingestion_status",
            side_effect=[
                None,
                ingestion_metadata.MetadataWriteError("metadata failed"),
            ],
        ),
    ):
        with pytest.raises(bigquery_loader.LoadJobFailed, match="job failed"):
            ingestion_coordinator.run_landing_load_with_metadata(
                client, config, payload
            )


def test_success_metadata_failure_propagated(
    client, config, payload, load_result
):
    with (
        patch.object(
            ingestion_coordinator.bigquery_loader,
            "load_landing_payload",
            return_value=load_result,
        ),
        patch.object(
            ingestion_coordinator,
            "record_ingestion_status",
            side_effect=[
                None,
                ingestion_metadata.MetadataWriteError("metadata failed"),
            ],
        ),
    ):
        with pytest.raises(ingestion_metadata.MetadataWriteError):
            ingestion_coordinator.run_landing_load_with_metadata(
                client, config, payload
            )


def test_invalid_payload_does_not_write_metadata(client, config, payload):
    invalid = ndjson_serializer.SerializedLandingPayload(
        batch_id=payload.batch_id,
        source_query_key=payload.source_query_key,
        query_type=payload.query_type,
        ndjson_bytes=b"",
        record_count=0,
        size_bytes=0,
        sha256=hashlib.sha256(b"").hexdigest(),
    )

    with patch.object(
        ingestion_coordinator, "record_ingestion_status"
    ) as record_status:
        with pytest.raises(ValueError, match="空的 Landing Payload"):
            ingestion_coordinator.run_landing_load_with_metadata(
                client, config, invalid
            )

    record_status.assert_not_called()
