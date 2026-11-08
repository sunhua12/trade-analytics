import pytest

from trade_analytics.ingestion import query, real_source


@pytest.mark.parametrize(
    ("query_type", "expected_partner"),
    [
        (query.QueryType.WORLD_TOTAL, "0"),
        (query.QueryType.PARTNER_DETAIL, None),
    ],
)
def test_build_api_params(query_type, expected_partner):
    comtrade_query = query.ComtradeQuery(
        period="202401",
        reporter_code=842,
        cmd_code="854231",
        query_type=query_type,
    )

    params = real_source.build_api_params(comtrade_query)

    assert params["period"] == "202401"
    assert params["reporterCode"] == "842"
    assert params["cmdCode"] == "854231"
    assert params["flowCode"] == "M"
    assert params.get("partnerCode") == expected_partner
