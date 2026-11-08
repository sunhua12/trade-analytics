import hashlib
import json
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from trade_analytics.contracts import (
    raw_load_contract,
    source_completeness,
)
from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload
from trade_analytics.warehouse import ingestion_metadata
from trade_analytics.utils import manifest
from trade_analytics.warehouse import (
    bigquery_config,
    raw_initial_loader,
    raw_load_coordinator,
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
def source_record():
    return {
        "period": "202401",
        "reporterCode": 842,
        "partnerCode": 156,
        "cmdCode": "854231",
        "primaryValue": Decimal("123456789.123456789"),
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


@pytest.fixture
def payload(comtrade_query, source_record):
    result = batch.BatchResult(
        query=comtrade_query, records=[source_record]
    )
    return landing_payload.build_landing_payload(result, "batch-001")


@pytest.fixture
def scope(payload, comtrade_query):
    return raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )


@pytest.fixture
def report(payload):
    checksum = hashlib.sha256(
        raw_load_coordinator.source_records_bytes(payload)
    ).hexdigest()

    return source_completeness.SourceQualityReport(
        source_query_key=payload.source_query_key,
        response_count=1,
        accepted_record_count=1,
        discarded_world_total_count=0,
        record_limit=500,
        records_sha256=checksum,
        unmodeled_fields=(),
    )


@pytest.fixture
def client():
    return Mock(spec=bigquery.Client)


@pytest.fixture
def decision(scope):
    return raw_load_decision.RawLoadDecision(
        scope=scope,
        action="INITIAL_LOAD",
        existing_record_count=0,
        source_assurance_level="BOUNDED_PREVIEW",
    )


@pytest.fixture
def load_result(config, scope, report):
    revision_id, job_id = raw_load_coordinator.build_raw_identity(
        scope, report
    )
    return raw_initial_loader.RawInitialLoadResult(
        batch_id=scope.batch_id,
        source_query_key=scope.source_query_key,
        revision_id=revision_id,
        job_id=job_id,
        table_id=config.raw_table("comtrade_records"),
        record_count=1,
    )


def test_validate_raw_source(payload, comtrade_query, report):
    scope = raw_load_coordinator.validate_raw_source(
        payload, comtrade_query, report
    )

    assert scope.record_count == 1
    assert scope.source_query_key == report.source_query_key


def test_invalid_source_checksum_rejected(
        payload, comtrade_query, report
):
    from dataclasses import replace

    invalid = replace(report, records_sha256="0" * 64)

    with pytest.raises(ValueError, match="Checksum 不一致"):
        raw_load_coordinator.validate_raw_source(
            payload, comtrade_query, invalid
        )


def test_success_records_audit(
        client, config, payload, comtrade_query, report,
        decision, load_result
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
            return_value=load_result,
        ),
        patch.object(
            raw_load_coordinator,
            "verify_raw_records",
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
        ) as audit,
    ):
        result = raw_load_coordinator.run_initial_raw_with_audit(
            client, config, payload, comtrade_query, report,
            allow_bounded_preview=True,
        )

    assert result == load_result
    assert audit.call_count == 2
    assert audit.call_args_list[0].args[6] == "STARTED"
    assert audit.call_args_list[1].args[6] == "SUCCEEDED"
    assert audit.call_args_list[1].kwargs["loaded_record_count"] == 1


def test_preview_requires_explicit_approval(
        client, config, payload, comtrade_query, report
):
    with patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
    ) as decide:
        with pytest.raises(ValueError, match="明確允許"):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report
            )

    decide.assert_not_called()


def test_failed_job_records_failed(
        client, config, payload, comtrade_query, report, decision
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
            side_effect=raw_initial_loader.RawInitialLoadError("failed"),
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
        ) as audit,
    ):
        with pytest.raises(raw_initial_loader.RawInitialLoadError):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report,
                allow_bounded_preview=True,
            )

    assert audit.call_args_list[-1].args[6] == "FAILED"


def test_unknown_job_records_unknown(
        client, config, payload, comtrade_query, report, decision
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
            side_effect=raw_initial_loader.RawInitialLoadStateUnknown(
                "unknown"
            ),
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
        ) as audit,
    ):
        with pytest.raises(raw_initial_loader.RawInitialLoadStateUnknown):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report,
                allow_bounded_preview=True,
            )

    assert audit.call_args_list[-1].args[6] == "UNKNOWN"


def test_integrity_error_records_validation_failed(
        client, config, payload, comtrade_query, report,
        decision, load_result
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
            return_value=load_result,
        ),
        patch.object(
            raw_load_coordinator,
            "verify_raw_records",
            side_effect=raw_load_coordinator.RawIntegrityError(
                "checksum mismatch"
            ),
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
        ) as audit,
    ):
        with pytest.raises(raw_load_coordinator.RawIntegrityError):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report,
                allow_bounded_preview=True,
            )

    assert audit.call_args_list[-1].args[6] == "VALIDATION_FAILED"


def test_recovery_does_not_resubmit_job(
        client, config, payload, comtrade_query, report, scope
):
    _, expected_job_id = raw_load_coordinator.build_raw_identity(
        scope, report
    )
    job = client.get_job.return_value
    job.job_id = expected_job_id
    job.state = "DONE"
    job.error_result = None

    with (
        patch.object(
            raw_load_coordinator,
            "verify_raw_records",
        ) as verify,
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
        ) as audit,
    ):
        result = raw_load_coordinator.recover_initial_raw(
            client, config, payload, comtrade_query, report
        )

    assert result.job_id == expected_job_id
    client.query.assert_not_called()
    verify.assert_called_once()
    assert audit.call_args.args[6] == "SUCCEEDED"


@pytest.mark.parametrize("state", ["PENDING", "RUNNING"])
def test_incomplete_recovery_rejected(
        client, config, payload, comtrade_query, report, scope, state
):
    _, job_id = raw_load_coordinator.build_raw_identity(scope, report)
    job = client.get_job.return_value
    job.job_id = job_id
    job.state = state
    job.error_result = None

    with pytest.raises(raw_initial_loader.RawInitialLoadStateUnknown):
        raw_load_coordinator.recover_initial_raw(
            client, config, payload, comtrade_query, report
        )


def test_failed_recovery_rejected(
        client, config, payload, comtrade_query, report, scope
):
    _, job_id = raw_load_coordinator.build_raw_identity(scope, report)
    job = client.get_job.return_value
    job.job_id = job_id
    job.state = "DONE"
    job.error_result = {"reason": "invalidQuery"}

    with pytest.raises(raw_initial_loader.RawInitialLoadError):
        raw_load_coordinator.recover_initial_raw(
            client, config, payload, comtrade_query, report
        )


def test_started_audit_failure_prevents_load(
        client, config, payload, comtrade_query, report, decision
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
            side_effect=ingestion_metadata.MetadataWriteError("audit failed"),
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
        ) as load,
    ):
        with pytest.raises(ingestion_metadata.MetadataWriteError):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report,
                allow_bounded_preview=True,
            )

    load.assert_not_called()


def test_success_audit_failure_does_not_retry_raw(
        client, config, payload, comtrade_query, report,
        decision, load_result
):
    with (
        patch.object(
            raw_load_coordinator.raw_load_decision,
            "decide_initial_raw_load",
            return_value=decision,
        ),
        patch.object(
            raw_load_coordinator.raw_initial_loader,
            "load_initial_raw",
            return_value=load_result,
        ) as load,
        patch.object(
            raw_load_coordinator,
            "verify_raw_records",
        ),
        patch.object(
            raw_load_coordinator,
            "write_raw_audit",
            side_effect=[
                None,
                ingestion_metadata.MetadataWriteError("audit failed"),
            ],
        ),
    ):
        with pytest.raises(ingestion_metadata.MetadataWriteError):
            raw_load_coordinator.run_initial_raw_with_audit(
                client, config, payload, comtrade_query, report,
                allow_bounded_preview=True,
            )

    load.assert_called_once()
