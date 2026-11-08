
import copy
import decimal

import pytest

from trade_analytics.ingestion import batch
from trade_analytics.ingestion import query as query_module
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
        "primaryValue": decimal.Decimal("1200.50"),
        "flowCode": "M",
        "classificationCode": "H6",
        "freqCode": "M",
        "partner2Code": 0,
        "customsCode": "C00",
        "motCode": 0,
        "isOriginalClassification": True,
    }


def test_query_scope(query):
    scope = query_identity.build_query_scope(query)

    assert scope["key_version"] == 1
    assert scope["query_type"] == "partner_detail"
    assert scope["params"]["period"] == "202401"
    assert scope["params"]["reporterCode"] == "842"
    assert "partnerCode" not in scope["params"]


def test_same_query_has_same_key(query):
    first = query_identity.build_source_query_key(query)
    second = query_identity.build_source_query_key(query)

    assert first == second
    assert len(first) == 64


@pytest.mark.parametrize("field,value", [
    ("period", "202402"),
    ("reporter_code", 156),
    ("cmd_code", "854232"),
    ("query_type", query_module.QueryType.WORLD_TOTAL),
])
def test_different_query_has_different_key(query, field, value):
    changed_query = query.model_copy(update={field: value})

    assert query_identity.build_source_query_key(query) != query_identity.build_source_query_key(changed_query)


def test_record_value_revision_keeps_same_key(query, record):
    updated = copy.deepcopy(record)
    updated["primaryValue"] = decimal.Decimal("1350.75")

    assert query_identity.build_record_business_key(record, query) == query_identity.build_record_business_key(updated, query)


def test_different_partner_has_different_key(query, record):
    updated = copy.deepcopy(record)
    updated["partnerCode"] = 392

    assert query_identity.build_record_business_key(record, query) != query_identity.build_record_business_key(updated, query)


def test_different_customs_code_has_different_key(query, record):
    updated = copy.deepcopy(record)
    updated["customsCode"] = "C01"

    assert query_identity.build_record_business_key(record, query) != query_identity.build_record_business_key(updated, query)


def test_reordered_records_keep_same_keys(query, record):
    another = copy.deepcopy(record)
    another["partnerCode"] = 392

    first_keys = {query_identity.build_record_business_key(item, query) for item in [record, another]}
    second_keys = {query_identity.build_record_business_key(item, query) for item in [another, record]}

    assert first_keys == second_keys


def test_find_duplicate_record_keys(query, record):
    result = batch.BatchResult(query=query, records=[record, copy.deepcopy(record)])

    duplicates = query_identity.find_duplicate_record_keys(result)

    assert len(duplicates) == 1
    assert duplicates[0]["first_row_index"] == 0
    assert duplicates[0]["duplicate_row_index"] == 1


def test_validate_record_uniqueness_rejects_duplicates(query, record):
    result = batch.BatchResult(query=query, records=[record, copy.deepcopy(record)])

    with pytest.raises(ValueError, match="重複 Record Business Key"):
        query_identity.validate_record_uniqueness(result)


def test_validate_record_uniqueness_passes(query, record):
    another = copy.deepcopy(record)
    another["partnerCode"] = 392
    result = batch.BatchResult(query=query, records=[record, another])

    query_identity.validate_record_uniqueness(result)


def test_missing_business_key_field_raises(query, record):
    del record["customsCode"]

    with pytest.raises(KeyError, match="customsCode"):
        query_identity.build_record_business_key(record, query)
