
from copy import deepcopy
from datetime import date
from decimal import Decimal

import pytest

from trade_analytics.contracts import raw_load_contract
from trade_analytics.ingestion import batch, query
from trade_analytics.processing import landing_payload


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
        query=comtrade_query,
        records=[source_record],
    )
    return landing_payload.build_landing_payload(result, "batch-001")


def test_valid_raw_load_scope(payload, comtrade_query):
    scope = raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )

    assert scope.batch_id == "batch-001"
    assert scope.query_type == "partner_detail"
    assert scope.period == "202401"
    assert scope.period_start_date == date(2024, 1, 1)
    assert scope.reporter_code == 842
    assert scope.cmd_code == "854231"
    assert scope.record_count == 1


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("202401", date(2024, 1, 1)),
        ("202412", date(2024, 12, 1)),
        ("202602", date(2026, 2, 1)),
    ],
)
def test_parse_period_start_date(period, expected):
    assert raw_load_contract.parse_period_start_date(period) == expected


@pytest.mark.parametrize(
    "period", ["202413", "202400", "20241", "2024-01", ""]
)
def test_invalid_period_rejected(period):
    with pytest.raises(ValueError, match="無效的 Period"):
        raw_load_contract.parse_period_start_date(period)


@pytest.mark.parametrize(
    "value", ["123.45", "0", "-10.5", "123456789.123456789"]
)
def test_valid_decimal_text(value):
    raw_load_contract.validate_decimal_text(value, "primary_value")


@pytest.mark.parametrize(
    "value", ["NaN", "Infinity", "1e10", "abc", "1.2.3", 12.5]
)
def test_invalid_decimal_text_rejected(value):
    with pytest.raises(ValueError):
        raw_load_contract.validate_decimal_text(value, "primary_value")


def test_nullable_decimal_accepts_none():
    raw_load_contract.validate_decimal_text(
        None, "net_weight", nullable=True
    )


def test_required_decimal_rejects_none():
    with pytest.raises(ValueError):
        raw_load_contract.validate_decimal_text(
            None, "primary_value"
        )


def test_empty_batch_rejected(comtrade_query):
    result = batch.BatchResult(query=comtrade_query, records=[])
    empty_payload = landing_payload.build_landing_payload(
        result, "batch-001"
    )

    with pytest.raises(ValueError, match="不接受空批次"):
        raw_load_contract.validate_raw_load_scope(
            empty_payload, comtrade_query
        )


def test_source_query_key_mismatch_rejected(payload, comtrade_query):
    payload.records[0]["source_query_key"] = "wrong-key"

    with pytest.raises(ValueError):
        raw_load_contract.validate_raw_load_scope(
            payload, comtrade_query
        )


def test_scope_mismatch_rejected(payload, comtrade_query):
    payload.records[0]["reporter_code"] = 392

    with pytest.raises(ValueError, match="Raw Scope 不一致"):
        raw_load_contract.validate_raw_load_scope(
            payload, comtrade_query
        )


def test_source_json_mismatch_rejected(payload, comtrade_query):
    payload.records[0]["partner_code"] = 392

    with pytest.raises(ValueError, match="來源與 Landing 欄位不一致"):
        raw_load_contract.validate_raw_load_scope(
            payload, comtrade_query
        )


def test_decimal_precision_preserved(payload, comtrade_query):
    scope = raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )

    assert scope.record_count == 1
    assert (
        payload.records[0]["primary_value_text"]
        == "123456789.123456789"
    )


def test_duplicate_business_key_rejected(comtrade_query, source_record):
    another = deepcopy(source_record)
    another["primaryValue"] = Decimal("999.99")

    result = batch.BatchResult(
        query=comtrade_query,
        records=[source_record, another],
    )

    with pytest.raises(ValueError, match="重複 Record Business Key"):
        landing_payload.build_landing_payload(result, "batch-001")


def test_multiple_partners_valid(comtrade_query, source_record):
    another = deepcopy(source_record)
    another["partnerCode"] = 392

    result = batch.BatchResult(
        query=comtrade_query,
        records=[source_record, another],
    )
    payload = landing_payload.build_landing_payload(result, "batch-001")

    scope = raw_load_contract.validate_raw_load_scope(
        payload, comtrade_query
    )

    assert scope.record_count == 2


def test_invalid_hs_version_rejected(payload, comtrade_query):
    payload.records[0]["hs_version"] = "H5"

    with pytest.raises(ValueError, match="HS Version"):
        raw_load_contract.validate_raw_load_scope(
            payload, comtrade_query
        )


def test_primary_value_mismatch_rejected(payload, comtrade_query):
    payload.records[0]["primary_value_text"] = "999.99"

    with pytest.raises(ValueError, match="來源與 Landing 數值不一致"):
        raw_load_contract.validate_raw_load_scope(
            payload, comtrade_query
        )
