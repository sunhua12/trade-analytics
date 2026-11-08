import pytest
from pydantic import ValidationError

from trade_analytics.ingestion.query import ComtradeQuery, QueryType


def test_world_query_to_params():
    query = ComtradeQuery(
        period="202401",
        cmd_code="854231",
        reporter_code=842,
        query_type=QueryType.WORLD_TOTAL,
    )

    assert query.to_params()["partnerCode"] == 0


def test_partner_query_omits_partner_code():
    query = ComtradeQuery(
        period="202401",
        cmd_code="854231",
        reporter_code=842,
        query_type=QueryType.PARTNER_DETAIL,
    )

    assert "partnerCode" not in query.to_params()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("period", "202413"),
        ("cmd_code", "123"),
        ("reporter_code", 0),
    ],
)
def test_invalid_query_values_raise_validation_error(field, value):
    values = {
        "period": "202401",
        "cmd_code": "854231",
        "reporter_code": 842,
        "query_type": QueryType.WORLD_TOTAL,
    }
    values[field] = value

    with pytest.raises(ValidationError):
        ComtradeQuery(**values)