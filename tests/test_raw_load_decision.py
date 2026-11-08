
from dataclasses import replace
from datetime import date
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from trade_analytics.contracts import raw_load_contract, source_completeness
from trade_analytics.warehouse import (
    bigquery_config,
    bigquery_schema,
    raw_load_decision,
)


@pytest.fixture
def config():
    return bigquery_config.BigQueryConfig(
        project_id="test-project",
        location="asia-northeast1",
    )


@pytest.fixture
def scope():
    return raw_load_contract.RawLoadScope(
        batch_id="batch-001",
        source_query_key="query-key-001",
        query_type="partner_detail",
        period="202401",
        period_start_date=date(2024, 1, 1),
        reporter_code=842,
        cmd_code="854231",
        flow_code="M",
        record_count=2,
    )


@pytest.fixture
def report():
    return source_completeness.SourceQualityReport(
        source_query_key="query-key-001",
        response_count=3,
        accepted_record_count=2,
        discarded_world_total_count=1,
        record_limit=500,
        records_sha256="a" * 64,
        unmodeled_fields=(),
    )


@pytest.fixture
def mock_client():
    client = Mock(spec=bigquery.Client)
    job = client.query.return_value
    job.result.return_value = [{"record_count": 0}]
    return client


def test_valid_source_evidence(scope, report):
    raw_load_decision.validate_source_evidence(scope, report)


def test_initial_load_allowed(mock_client, config, scope, report):
    decision = raw_load_decision.decide_initial_raw_load(
        mock_client, config, scope, report
    )

    assert decision.action == "INITIAL_LOAD"
    assert decision.scope == scope
    assert decision.existing_record_count == 0
    assert decision.source_assurance_level == "BOUNDED_PREVIEW"


def test_existing_scope_rejected(mock_client, config, scope, report):
    mock_client.query.return_value.result.return_value = [
        {"record_count": 2}
    ]

    with pytest.raises(
        raw_load_decision.RawScopeAlreadyExists,
        match="Raw Scope 已存在",
    ):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, report
        )


def test_count_query_uses_parameters(mock_client, config, scope):
    count = raw_load_decision.count_existing_raw_records(
        mock_client, config, scope
    )

    assert count == 0

    args, kwargs = mock_client.query.call_args
    sql = args[0]
    job_config = kwargs["job_config"]

    assert config.raw_table(bigquery_schema.RAW_TABLE) in sql
    assert "period_start_date = @period_start_date" in sql
    assert "reporter_code = @reporter_code" in sql
    assert "query_type = @query_type" in sql
    assert kwargs["location"] == config.location

    params = {p.name: p.value for p in job_config.query_parameters}
    assert params["period_start_date"] == date(2024, 1, 1)
    assert params["period"] == "202401"
    assert params["reporter_code"] == 842
    assert params["cmd_code"] == "854231"
    assert params["query_type"] == "partner_detail"
    assert params["flow_code"] == "M"


def test_query_failure_propagates(mock_client, config, scope, report):
    mock_client.query.side_effect = RuntimeError("BigQuery unavailable")

    with pytest.raises(RuntimeError, match="BigQuery unavailable"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, report
        )


def test_missing_query_result_rejected(mock_client, config, scope):
    mock_client.query.return_value.result.return_value = []

    with pytest.raises(ValueError, match="回傳筆數異常"):
        raw_load_decision.count_existing_raw_records(
            mock_client, config, scope
        )


@pytest.mark.parametrize("count", [-1, None, "0", True])
def test_invalid_existing_count_rejected(
    mock_client, config, scope, count
):
    mock_client.query.return_value.result.return_value = [
        {"record_count": count}
    ]

    with pytest.raises(ValueError, match="回傳無效數值"):
        raw_load_decision.count_existing_raw_records(
            mock_client, config, scope
        )


def test_source_query_key_mismatch_rejected(
    mock_client, config, scope, report
):
    invalid = replace(report, source_query_key="wrong-key")

    with pytest.raises(ValueError, match="Query Key 不一致"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, invalid
        )

    mock_client.query.assert_not_called()


def test_source_count_mismatch_rejected(
    mock_client, config, scope, report
):
    invalid = replace(report, accepted_record_count=1)

    with pytest.raises(ValueError, match="Record Count 不一致"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, invalid
        )

    mock_client.query.assert_not_called()


def test_truncated_source_rejected(mock_client, config, scope, report):
    invalid = replace(report, response_count=500)

    with pytest.raises(ValueError, match="Record Limit"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, invalid
        )

    mock_client.query.assert_not_called()


def test_invalid_checksum_rejected(mock_client, config, scope, report):
    invalid = replace(report, records_sha256="invalid")

    with pytest.raises(ValueError, match="SHA-256"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, scope, invalid
        )

    mock_client.query.assert_not_called()


def test_empty_scope_rejected(mock_client, config, scope, report):
    invalid = replace(scope, record_count=0)

    with pytest.raises(ValueError, match="空批次"):
        raw_load_decision.decide_initial_raw_load(
            mock_client, config, invalid, report
        )

    mock_client.query.assert_not_called()


def test_invalid_table_identifier_rejected(scope):
    config = bigquery_config.BigQueryConfig(
        project_id="test-project",
        location="asia-northeast1",
        raw_dataset="trade_raw;DROP_TABLE",
    )

    with pytest.raises(ValueError, match="Table Identifier"):
        raw_load_decision.count_existing_raw_records(
            Mock(spec=bigquery.Client), config, scope
        )
