
from copy import deepcopy
from decimal import Decimal

import pytest

from trade_analytics.batch import BatchResult
from trade_analytics.query import ComtradeQuery, QueryType
from trade_analytics.query_identity import (
    build_query_scope,
    build_source_query_key,
    build_record_business_key,
    find_duplicate_record_keys,
    validate_record_uniqueness,
)


@pytest.fixture
def query():
    return ComtradeQuery(
        reporter_code=842,
        period="202401",
        cmd_code="854231",
        query_type=QueryType.PARTNER_DETAIL,
    )


@pytest.fixture
def record():
    return {
        "period": "202401",
        "reporterCode": 842,
        "partnerCode": 156,
        "cmdCode": "854231",
        "primaryValue": Decimal("1200.50"),
        "flowCode": "M",
        "classificationCode": "H6",
        "freqCode": "M",
        "partner2Code": 0,
        "customsCode": "C00",
        "motCode": 0,
        "isOriginalClassification": True,
    }


def test_query_scope(query):
    scope = build_query_scope(query)

    assert scope["key_version"] == 1
    assert scope["query_type"] == "partner_detail"
    assert scope["params"]["period"] == "202401"
    assert scope["params"]["reporterCode"] == "842"
    assert "partnerCode" not in scope["params"]


def test_same_query_has_same_key(query):
    first = build_source_query_key(query)
    second = build_source_query_key(query)

    assert first == second
    assert len(first) == 64


@pytest.mark.parametrize("field,value", [
    ("period", "202402"),
    ("reporter_code", 156),
    ("cmd_code", "854232"),
    ("query_type", QueryType.WORLD_TOTAL),
])
def test_different_query_has_different_key(query, field, value):
    changed_query = query.model_copy(update={field: value})

    assert build_source_query_key(query) != build_source_query_key(changed_query)


def test_record_value_revision_keeps_same_key(query, record):
    updated = deepcopy(record)
    updated["primaryValue"] = Decimal("1350.75")

    assert build_record_business_key(record, query) == build_record_business_key(updated, query)


def test_different_partner_has_different_key(query, record):
    updated = deepcopy(record)
    updated["partnerCode"] = 392

    assert build_record_business_key(record, query) != build_record_business_key(updated, query)


def test_different_customs_code_has_different_key(query, record):
    updated = deepcopy(record)
    updated["customsCode"] = "C01"

    assert build_record_business_key(record, query) != build_record_business_key(updated, query)


def test_reordered_records_keep_same_keys(query, record):
    another = deepcopy(record)
    another["partnerCode"] = 392

    first_keys = {build_record_business_key(item, query) for item in [record, another]}
    second_keys = {build_record_business_key(item, query) for item in [another, record]}

    assert first_keys == second_keys


def test_find_duplicate_record_keys(query, record):
    result = BatchResult(query=query, records=[record, deepcopy(record)])

    duplicates = find_duplicate_record_keys(result)

    assert len(duplicates) == 1
    assert duplicates[0]["first_row_index"] == 0
    assert duplicates[0]["duplicate_row_index"] == 1


def test_validate_record_uniqueness_rejects_duplicates(query, record):
    result = BatchResult(query=query, records=[record, deepcopy(record)])

    with pytest.raises(ValueError, match="重複 Record Business Key"):
        validate_record_uniqueness(result)


def test_validate_record_uniqueness_passes(query, record):
    another = deepcopy(record)
    another["partnerCode"] = 392
    result = BatchResult(query=query, records=[record, another])

    validate_record_uniqueness(result)


def test_missing_business_key_field_raises(query, record):
    del record["customsCode"]

    with pytest.raises(KeyError, match="customsCode"):
        build_record_business_key(record, query)
