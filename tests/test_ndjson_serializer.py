
import hashlib
import json
from copy import deepcopy
from decimal import Decimal

import pytest

from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload, ndjson_serializer
from trade_analytics.utils import manifest
from trade_analytics.warehouse import bigquery_schema


@pytest.fixture
def query_data():
    return query.ComtradeQuery(
        reporter_code=842,
        period="202401",
        cmd_code="854231",
        query_type=query.QueryType.PARTNER_DETAIL,
    )


@pytest.fixture
def record():
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
def payload(query_data, record):
    result = batch.BatchResult(query=query_data, records=[record])
    return landing_payload.build_landing_payload(result, batch_id="batch-001")


def decode_rows(serialized):
    return [
        json.loads(line)
        for line in serialized.ndjson_bytes.splitlines()
        if line
    ]


def test_serialize_landing_payload(payload):
    result = ndjson_serializer.serialize_landing_payload(payload)

    assert result.batch_id == payload.batch_id
    assert result.source_query_key == payload.source_query_key
    assert result.query_type == payload.query_type
    assert result.record_count == 1
    assert isinstance(result.ndjson_bytes, bytes)


def test_ndjson_matches_existing_serializer(payload):
    result = ndjson_serializer.serialize_landing_payload(payload)

    assert result.ndjson_bytes == manifest.serialize_records(payload.records)


def test_ndjson_matches_landing_schema(payload):
    result = ndjson_serializer.serialize_landing_payload(payload)
    rows = decode_rows(result)
    expected_fields = {field.name for field in bigquery_schema.LANDING_SCHEMA}

    assert len(rows) == 1
    assert set(rows[0]) == expected_fields


def test_decimal_precision_preserved(payload):
    result = ndjson_serializer.serialize_landing_payload(payload)
    row = decode_rows(result)[0]

    assert row["primary_value_text"] == "123456789.123456789"
    assert row["net_weight_text"] == "123.450"
    assert row["quantity_text"] == "10"


def test_checksum_and_size(payload):
    result = ndjson_serializer.serialize_landing_payload(payload)

    assert result.size_bytes == len(result.ndjson_bytes)
    assert result.sha256 == hashlib.sha256(result.ndjson_bytes).hexdigest()


def test_deterministic_serialization(payload):
    first = ndjson_serializer.serialize_landing_payload(payload)
    second = ndjson_serializer.serialize_landing_payload(payload)

    assert first.ndjson_bytes == second.ndjson_bytes
    assert first.sha256 == second.sha256


def test_multiple_records(query_data, record):
    another = deepcopy(record)
    another["partnerCode"] = 392
    result = batch.BatchResult(query=query_data, records=[record, another])
    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    serialized = ndjson_serializer.serialize_landing_payload(payload)
    rows = decode_rows(serialized)

    assert serialized.record_count == 2
    assert len(rows) == 2
    assert [row["source_row_index"] for row in rows] == [0, 1]


def test_empty_payload(query_data):
    result = batch.BatchResult(query=query_data, records=[])
    payload = landing_payload.build_landing_payload(result, batch_id="batch-001")

    serialized = ndjson_serializer.serialize_landing_payload(payload)

    assert serialized.record_count == 0
    assert serialized.ndjson_bytes == b""
    assert serialized.size_bytes == 0
    assert serialized.sha256 == hashlib.sha256(b"").hexdigest()


def test_missing_field_rejected(payload):
    del payload.records[0]["primary_value_text"]

    with pytest.raises(ValueError, match="Landing Schema 不一致"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_extra_field_rejected(payload):
    payload.records[0]["unknown_field"] = "test"

    with pytest.raises(ValueError, match="Landing Schema 不一致"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_wrong_field_type_rejected(payload):
    payload.records[0]["primary_value_text"] = 123.45

    with pytest.raises(TypeError, match="Landing 欄位型別不符"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_bool_not_accepted_as_integer(payload):
    payload.records[0]["partner_code"] = True

    with pytest.raises(TypeError, match="Landing 欄位型別不符"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_required_field_cannot_be_none(payload):
    payload.records[0]["period"] = None

    with pytest.raises(ValueError, match="必要欄位不可為 None"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_nullable_field_accepts_none(payload):
    payload.records[0]["hs_version"] = None
    payload.records[0]["net_weight_text"] = None

    serialized = ndjson_serializer.serialize_landing_payload(payload)
    row = decode_rows(serialized)[0]

    assert row["hs_version"] is None
    assert row["net_weight_text"] is None


def test_metadata_mismatch_rejected(payload):
    payload.records[0]["batch_id"] = "wrong-batch"

    with pytest.raises(ValueError, match="Landing Metadata 不一致"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_source_row_index_mismatch_rejected(payload):
    payload.records[0]["source_row_index"] = 99

    with pytest.raises(ValueError, match="source_row_index 不連續"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_invalid_source_json_rejected(payload):
    payload.records[0]["source_record_json"] = "{invalid-json}"

    with pytest.raises(ValueError, match="source_record_json 非合法 JSON"):
        ndjson_serializer.serialize_landing_payload(payload)


def test_validate_landing_schema(payload):
    ndjson_serializer.validate_landing_schema(payload)
