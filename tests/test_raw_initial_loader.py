
import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from trade_analytics.contracts import raw_load_contract
from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload
from trade_analytics.utils import manifest
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_schema,
    raw_initial_loader,
    raw_load_decision,
)


@pytest.fixture
def config():
    return bigquery_config.BigQueryConfig(
        project_id="test-project",
        location="asia-northeast1",
    )


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
    def make_record(partner_code, value):
        return {
            "period": "202401",
            "reporterCode": 842,
            "partnerCode": partner_code,
            "cmdCode": "854231",
            "primaryValue": Decimal(value),
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

    return [
        make_record(156, "123456789.123456789"),
        make_record(392, "987654321.987654321"),
    ]


@pytest.fixture
def payload(comtrade_query, source_records):
    result = batch.BatchResult(
        query=comtrade_query,
        records=source_records,
    )
    return landing_payload.build_landing_payload(result, "batch-001")


@pytest.fixture
def scope(payload, comtrade_query):
    return raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )


@pytest.fixture
def decision(scope):
    return raw_load_decision.RawLoadDecision(
        scope=scope,
        action="INITIAL_LOAD",
        existing_record_count=0,
        source_assurance_level="BOUNDED_PREVIEW",
    )


@pytest.fixture
def source_sha256(payload):
    source_records = [
        json.loads(record["source_record_json"])
        for record in payload.records
    ]
    return hashlib.sha256(
        manifest.serialize_records(source_records)
    ).hexdigest()


@pytest.fixture
def mock_client(config, scope, source_sha256):
    client = Mock(spec=bigquery.Client)
    job = client.query.return_value
    revision_id = raw_initial_loader.build_revision_id(
        scope, source_sha256
    )
    job.job_id = raw_initial_loader.build_raw_job_id(
        scope, revision_id
    )
    job.error_result = None
    return client


def execute_load(
    mock_client, config, payload, comtrade_query, decision, source_sha256,
    **kwargs,
):
    return raw_initial_loader.load_initial_raw(
        mock_client,
        config,
        payload,
        comtrade_query,
        decision,
        source_sha256,
        **kwargs,
    )


def test_build_raw_insert_sql(config):
    sql = raw_initial_loader.build_raw_insert_sql(config)

    assert "CREATE TEMP TABLE incoming" in sql
    assert "BEGIN TRANSACTION" in sql
    assert "COMMIT TRANSACTION" in sql
    assert "DELETE FROM" in sql
    assert "WHERE FALSE" in sql
    assert "ASSERT NOT EXISTS" in sql
    assert "INSERT INTO" in sql
    assert "CAST(primary_value_text AS BIGNUMERIC)" in sql
    assert "CAST(net_weight_text AS BIGNUMERIC)" in sql
    assert "CAST(quantity_text AS BIGNUMERIC)" in sql
    assert config.raw_table(bigquery_schema.LANDING_TABLE) in sql
    assert config.raw_table(bigquery_schema.RAW_TABLE) in sql


def test_query_parameters(scope):
    job_config = raw_initial_loader.build_raw_query_config(
        scope, "revision-001"
    )
    params = {
        item.name: item.value
        for item in job_config.query_parameters
    }

    assert params["batch_id"] == scope.batch_id
    assert params["source_query_key"] == scope.source_query_key
    assert params["period"] == scope.period
    assert params["reporter_code"] == 842
    assert params["cmd_code"] == "854231"
    assert params["query_type"] == "partner_detail"
    assert params["expected_count"] == 2
    assert params["revision_id"] == "revision-001"


def test_revision_id_is_deterministic(scope, source_sha256):
    first = raw_initial_loader.build_revision_id(
        scope, source_sha256
    )
    second = raw_initial_loader.build_revision_id(
        scope, source_sha256
    )

    assert first == second
    assert len(first) == 64


def test_job_id_is_deterministic(scope):
    first = raw_initial_loader.build_raw_job_id(
        scope, "revision-001"
    )
    second = raw_initial_loader.build_raw_job_id(
        scope, "revision-001"
    )

    assert first == second
    assert first.startswith("raw_")


def test_initial_raw_load_success(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    result = execute_load(
        mock_client, config, payload, comtrade_query,
        decision, source_sha256,
        allow_bounded_preview=True,
    )

    assert result.batch_id == "batch-001"
    assert result.source_query_key == payload.source_query_key
    assert result.record_count == 2
    assert result.table_id == config.raw_table(bigquery_schema.RAW_TABLE)
    assert result.job_id == mock_client.query.return_value.job_id


def test_load_job_arguments(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    execute_load(
        mock_client, config, payload, comtrade_query,
        decision, source_sha256,
        allow_bounded_preview=True,
    )

    kwargs = mock_client.query.call_args.kwargs

    assert kwargs["location"] == config.location
    assert kwargs["job_retry"] is None
    assert kwargs["job_id"] == mock_client.query.return_value.job_id
    assert isinstance(kwargs["job_config"], bigquery.QueryJobConfig)


def test_load_waits_for_job(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    execute_load(
        mock_client, config, payload, comtrade_query,
        decision, source_sha256,
        allow_bounded_preview=True,
    )

    mock_client.query.return_value.result.assert_called_once_with()


def test_bounded_preview_requires_explicit_approval(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    with pytest.raises(ValueError, match="明確允許 BOUNDED_PREVIEW"):
        execute_load(
            mock_client, config, payload, comtrade_query,
            decision, source_sha256,
        )

    mock_client.query.assert_not_called()


def test_invalid_decision_rejected(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    invalid = replace(decision, action="SKIP")

    with pytest.raises(ValueError, match="不允許首次載入"):
        execute_load(
            mock_client, config, payload, comtrade_query,
            invalid, source_sha256,
            allow_bounded_preview=True,
        )

    mock_client.query.assert_not_called()


def test_scope_mismatch_rejected(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    invalid_scope = replace(decision.scope, reporter_code=392)
    invalid_decision = replace(decision, scope=invalid_scope)

    with pytest.raises(ValueError, match="Payload Scope 不一致"):
        execute_load(
            mock_client, config, payload, comtrade_query,
            invalid_decision, source_sha256,
            allow_bounded_preview=True,
        )

    mock_client.query.assert_not_called()


def test_checksum_mismatch_rejected(
    mock_client, config, payload, comtrade_query, decision
):
    with pytest.raises(ValueError, match="Checksum 不一致"):
        execute_load(
            mock_client, config, payload, comtrade_query,
            decision, "0" * 64,
            allow_bounded_preview=True,
        )

    mock_client.query.assert_not_called()


def test_job_submission_unknown(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    mock_client.query.side_effect = ConnectionError("Connection lost")

    with pytest.raises(raw_initial_loader.RawInitialLoadStateUnknown):
        execute_load(
            mock_client, config, payload, comtrade_query,
            decision, source_sha256,
            allow_bounded_preview=True,
        )


def test_job_execution_failed(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    job = mock_client.query.return_value
    job.result.side_effect = RuntimeError("BigQuery rejected SQL")
    job.error_result = {"reason": "invalidQuery"}

    with pytest.raises(raw_initial_loader.RawInitialLoadError):
        execute_load(
            mock_client, config, payload, comtrade_query,
            decision, source_sha256,
            allow_bounded_preview=True,
        )


def test_job_execution_unknown(
    mock_client, config, payload, comtrade_query, decision, source_sha256
):
    job = mock_client.query.return_value
    job.result.side_effect = ConnectionError("Connection lost")

    with pytest.raises(raw_initial_loader.RawInitialLoadStateUnknown):
        execute_load(
            mock_client, config, payload, comtrade_query,
            decision, source_sha256,
            allow_bounded_preview=True,
        )


def test_invalid_table_identifier():
    config = bigquery_config.BigQueryConfig(
        project_id="test-project",
        location="asia-northeast1",
        raw_dataset="trade_raw;DROP",
    )

    with pytest.raises(ValueError, match="Dataset ID"):
        raw_initial_loader.build_raw_insert_sql(config)
