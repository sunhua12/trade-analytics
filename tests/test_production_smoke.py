from copy import deepcopy
from decimal import Decimal

import pytest

from trade_analytics.contracts import source_completeness
from trade_analytics.ingestion import batch, query, schemas


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
def response_payload(source_record):
    another = deepcopy(source_record)
    another["partnerCode"] = 392

    return {
        "count": 2,
        "data": [source_record, another],
        "error": "",
    }


@pytest.fixture
def batch_result(comtrade_query, response_payload):
    parsed = schemas.ComtradeResponse.model_validate(
        response_payload
    )

    records = [
        record.model_dump(by_alias=True)
        for record in parsed.data
    ]

    return batch.BatchResult(
        query=comtrade_query,
        records=records,
    )


def test_valid_source_response(batch_result, response_payload):
    report = source_completeness.validate_source_completeness(
        batch_result,
        response_payload,
    )

    assert report.response_count == 2
    assert report.accepted_record_count == 2
    assert report.discarded_world_total_count == 0
    assert report.record_limit == 500
    assert len(report.records_sha256) == 64
    assert report.assurance_level == "BOUNDED_PREVIEW"


@pytest.mark.parametrize(
    ("count", "actual"),
    [
        (1, 2),
        (3, 2),
        (-1, 2),
        (None, 2),
    ],
)
def test_invalid_response_count_rejected(
        batch_result,
        response_payload,
        count,
        actual,
):
    response_payload["count"] = count

    with pytest.raises(
            source_completeness.SourceCompletenessError
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
        )


def test_record_limit_rejected(
        batch_result,
        response_payload,
):
    with pytest.raises(
            source_completeness.SourceCompletenessError,
            match="可能遭截斷",
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
            record_limit=2,
        )


def test_api_error_rejected(
        batch_result,
        response_payload,
):
    response_payload["error"] = "Rate limit exceeded"

    with pytest.raises(
            source_completeness.SourceCompletenessError,
            match="API Error",
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
        )


def test_missing_data_rejected(
        batch_result,
        response_payload,
):
    del response_payload["data"]

    with pytest.raises(
            source_completeness.SourceCompletenessError
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
        )


def test_changed_source_record_rejected(
        batch_result,
        response_payload,
):
    response_payload["data"][0]["primaryValue"] = (
        Decimal("999.99")
    )

    with pytest.raises(
            source_completeness.SourceCompletenessError,
            match="BatchResult 與原始 API Response",
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
        )


def test_world_total_filtered(
        comtrade_query,
        source_record,
        response_payload,
):
    world = deepcopy(source_record)
    world["partnerCode"] = 0

    response_payload["data"].insert(0, world)
    response_payload["count"] = 3

    parsed = schemas.ComtradeResponse.model_validate(
        response_payload
    )

    selected = [
        record.model_dump(by_alias=True)
        for record in parsed.data
        if record.partner_code != 0
    ]

    result = batch.BatchResult(
        query=comtrade_query,
        records=selected,
    )

    report = source_completeness.validate_source_completeness(
        result,
        response_payload,
    )

    assert report.response_count == 3
    assert report.accepted_record_count == 2
    assert report.discarded_world_total_count == 1


def test_duplicate_business_key_rejected(
        comtrade_query,
        source_record,
):
    duplicate = deepcopy(source_record)
    duplicate["primaryValue"] = Decimal("999.99")

    result = batch.BatchResult(
        query=comtrade_query,
        records=[source_record, duplicate],
    )

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Business Key",
    ):
        source_completeness.validate_source_grain(result)


def test_unmodeled_field_rejected(batch_result):
    batch_result.records[0]["unexpectedDimension"] = "X"

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="尚未審查",
    ):
        source_completeness.validate_source_grain(
            batch_result
        )


def test_approved_non_grain_field(batch_result):
    batch_result.records[0]["reporterDesc"] = (
        "United States"
    )

    fields = source_completeness.validate_source_grain(
        batch_result,
        approved_non_grain_fields={"reporterDesc"},
    )

    assert fields == ("reporterDesc",)


def test_source_grain_is_deterministic(batch_result):
    first = source_completeness.validate_source_grain(
        batch_result
    )

    second = source_completeness.validate_source_grain(
        batch_result
    )

    assert first == second


def test_invalid_scope_rejected(
        batch_result,
        response_payload,
):
    batch_result.records[0]["reporterCode"] = 392

    with pytest.raises(
            source_completeness.SourceCompletenessError
    ):
        source_completeness.validate_source_completeness(
            batch_result,
            response_payload,
        )


def test_invalid_record_limit_rejected(response_payload):
    with pytest.raises(
            ValueError,
            match="record_limit",
    ):
        source_completeness.validate_response_envelope(
            response_payload,
            record_limit=0,
        )


def make_reference_result(comtrade_query, source_record):
    """建立具備 UN Comtrade 附加欄位的測試資料。"""
    record = deepcopy(source_record)

    record.update({
        "typeCode": "C",
        "refYear": 2024,
        "refMonth": 1,
        "refPeriodId": 20240101,
        "classificationSearchCode": "HS",
        "aggrLevel": 6,
        "isAggregate": True,
        "isLeaf": False,
        "mosCode": "0",
        "reporterDesc": "USA",
        "partnerDesc": "China",
        "qtyUnitCode": 8,
        "isQtyEstimated": False,
    })

    return batch.BatchResult(
        query=comtrade_query,
        records=[record],
    )


def test_source_reference_fields_valid(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    source_completeness.validate_source_reference_fields(
        result,
        require_reference_fields=True,
    )


def test_source_reference_fields_required(
        comtrade_query,
        source_record,
):
    result = batch.BatchResult(
        query=comtrade_query,
        records=[source_record],
    )

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="缺少必要參考欄位",
    ):
        source_completeness.validate_source_reference_fields(
            result,
            require_reference_fields=True,
        )


def test_reference_year_mismatch_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["refYear"] = 2023

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Reference Year",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_reference_month_mismatch_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["refMonth"] = 2

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Reference Month",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_reference_period_mismatch_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["refPeriodId"] = 20240201

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Reference Period ID",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_classification_search_code_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["classificationSearchCode"] = "SITC"

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Classification Search Code",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_invalid_aggregation_level_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["aggrLevel"] = 4

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Aggregation Level",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_invalid_mos_code_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["mosCode"] = "2"

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="Mode of Supply",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_aggregation_flags_accept_boolean_values(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    # 彙總與 Leaf 旗標不強制採用固定真假組合。
    result.records[0]["isAggregate"] = True
    result.records[0]["isLeaf"] = False

    source_completeness.validate_source_reference_fields(
        result
    )


def test_invalid_aggregation_flag_type_rejected(
        comtrade_query,
        source_record,
):
    result = make_reference_result(
        comtrade_query,
        source_record,
    )

    result.records[0]["isLeaf"] = "yes"

    with pytest.raises(
            source_completeness.SourceGrainError,
            match="isLeaf",
    ):
        source_completeness.validate_source_reference_fields(
            result
        )


def test_approved_source_fields_are_explicit():
    approved = (
        source_completeness.APPROVED_SOURCE_NON_GRAIN_FIELDS
    )

    assert "reporterDesc" in approved
    assert "aggrLevel" in approved
    assert "mosCode" in approved
    assert "cifvalue" in approved

    assert "unexpectedDimension" not in approved


def test_preview_limit_cannot_be_raised(response_payload):
    with pytest.raises(
            ValueError,
            match="record_limit",
    ):
        source_completeness.validate_response_envelope(
            response_payload,
            record_limit=501,
        )
