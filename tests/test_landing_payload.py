import copy
import decimal

import pytest

from trade_analytics.ingestion import batch
from trade_analytics.ingestion import query as query_module
from trade_analytics.processing import landing_payload
from trade_analytics.contracts import query_identity


@pytest.fixture
def query():
    return query_module.ComtradeQuery(
        reporter_code=842,
        period="202401",
        cmd_code="854231",
        query_type=query_module.QueryType.PARTNER_DETAIL,
    )


@pytest.fixture
def record():
    return {
        "period": "202401",
        "reporterCode": 842,
        "partnerCode": 156,
        "cmdCode": "854231",
        "primaryValue": decimal.Decimal("123456789.123456789"),
        "flowCode": "M",
        "classificationCode": "H6",
        "freqCode": "M",
        "partner2Code": 0,
        "customsCode": "C00",
        "motCode": 0,
        "isOriginalClassification": True,
        "netWgt": decimal.Decimal("123.450"),
        "qty": decimal.Decimal("10"),
    }


def test_build_landing_payload(query, record):
    result = batch.BatchResult(query=query, records=[record])

    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    assert payload.batch_id == "batch-001"
    assert payload.source_query_key == query_identity.build_source_query_key(query)
    assert payload.query_type == query.query_type.value
    assert payload.record_count == 1
    assert payload.records[0]["source_row_index"] == 0
    assert payload.records[0]["primary_value_text"] == "123456789.123456789"


def test_multiple_records(query, record):
    another = copy.deepcopy(record)
    another["partnerCode"] = 392

    result = batch.BatchResult(query=query, records=[record, another])
    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    assert payload.record_count == 2
    assert [row["source_row_index"] for row in payload.records] == [0, 1]


def test_empty_batch(query):
    result = batch.BatchResult(query=query, records=[])

    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    assert payload.record_count == 0
    assert payload.records == []


@pytest.mark.parametrize("batch_id", ["", "   ", None])
def test_invalid_batch_id(query, record, batch_id):
    result = batch.BatchResult(query=query, records=[record])

    with pytest.raises(ValueError, match="batch_id 不可為空"):
        landing_payload.build_landing_payload(result, batch_id=batch_id)


def test_duplicate_records_rejected(query, record):
    result = batch.BatchResult(query=query, records=[record, copy.deepcopy(record)])

    with pytest.raises(ValueError, match="重複 Record Business Key"):
        landing_payload.build_landing_payload(result, batch_id="batch-001")


def test_query_scope_mismatch_rejected(query, record):
    invalid_record = copy.deepcopy(record)
    invalid_record["period"] = "202402"
    result = batch.BatchResult(query=query, records=[invalid_record])

    with pytest.raises(ValueError):
        landing_payload.build_landing_payload(result, batch_id="batch-001")


def test_unknown_source_field_warning(query, record, caplog):
    record["unexpectedField"] = "test"
    result = batch.BatchResult(query=query, records=[record])

    with caplog.at_level("WARNING"):
        payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    assert payload.record_count == 1
    assert "unexpectedField" in caplog.text


def test_source_query_key_is_stable(query, record):
    result = batch.BatchResult(query=query, records=[record])

    first = landing_payload.build_landing_payload(result, batch_id="batch-001")
    second = landing_payload.build_landing_payload(result, batch_id="batch-002")

    assert first.source_query_key == second.source_query_key
    assert first.batch_id != second.batch_id


def test_records_contain_landing_metadata(query, record):
    result = batch.BatchResult(query=query, records=[record])
    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")
    row = payload.records[0]

    assert row["batch_id"] == payload.batch_id
    assert row["source_query_key"] == payload.source_query_key
    assert row["query_type"] == payload.query_type
