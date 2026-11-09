import json
import decimal
import os

import httpx
import pytest
import pydantic
import tenacity

from trade_analytics.ingestion import client as client_module
from trade_analytics.ingestion import extract
from trade_analytics.ingestion import query


PARAMS = {
    "period": "202401",
    "reporterCode": "842",
    "flowCode": "M",
    "cmdCode": "854231",
    "partner2Code": 0,
    "customsCode": "C00",
    "motCode": 0,
    "partnerCode": 0,
}

BASE_RECORD = {
    "period": "202401",
    "reporterCode": 842,
    "partnerCode": 0,
    "flowCode": "M",
    "cmdCode": "854231",
    "primaryValue": 1000,
    "classificationCode": "H6",
    "freqCode": "M",
    "partner2Code": 0,
    "customsCode": "C00",
    "motCode": 0,
    "isOriginalClassification": True,
}

@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_COMTRADE_INTEGRATION") != "1",
    reason="設定 RUN_COMTRADE_INTEGRATION=1 才呼叫真實 Comtrade API",
)
def test_api():
    with httpx.Client(timeout=30.0) as http_client:
        client = client_module.ComtradeClient(http_client)
        payload = extract.fetch_trade_data(client, PARAMS)

    # 觀察外層資料
    print("\n外層資料型別：", type(payload).__name__)
    print("外層欄位：", list(payload.keys()))

    # 分別觀察 World Total 和 Partner Detail
    for name, rows in payload.items():
        print(f"\n========== {name.upper()} ==========")
        print("資料型別：", type(rows).__name__)
        print("實際回傳筆數：", len(rows))

        print("\n完整資料：")
        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))

        # 查看第一筆完整紀錄
        if rows:
            print("\n第一筆資料：")
            print(json.dumps(rows[0], indent=2, ensure_ascii=False, default=str))

            print("\n第一筆的欄位、型別與值：")
            for field, value in rows[0].items():
                print(f"{field:30} {type(value).__name__:12} {value!r}")

            print("\nprimaryValue 型別：", type(rows[0]["primaryValue"]).__name__)
            assert isinstance(rows[0]["primaryValue"], decimal.Decimal)



def make_record(partner_code: int, primary_value: int) -> dict:
    """建立符合 Pydantic Schema 的交易紀錄。"""
    return {**BASE_RECORD, "partnerCode": partner_code, "primaryValue": primary_value}


def make_response(status_code: int = 200, data: list | None = None) -> httpx.Response:
    """建立假的 HTTP Response。"""
    return httpx.Response(status_code, json={"data": data if data is not None else []})


def make_client(handler, max_attempts: int = 3):
    """建立使用 MockTransport 的 HTTP Client。"""
    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = client_module.ComtradeClient(http_client, max_attempts=max_attempts, wait=tenacity.wait_fixed(0))
    return http_client, client


# 1. JSON 解析成功
def test_parse_payload():
    text = '{"data": [{"primaryValue": 123.45}]}'
    result = extract.parse_payload(text)

    assert result["data"][0]["primaryValue"] == decimal.Decimal("123.45")


# 2. JSON 結構錯誤
def test_parse_payload_invalid():
    with pytest.raises(ValueError):
        extract.parse_payload("[]")


# 3. Pydantic Schema 驗證成功
def test_fetch_records_valid_schema():
    def handler(request):
        return make_response(200, [make_record(0, 1000)])

    http_client, client = make_client(handler)

    with http_client:
        records = extract.fetch_records(client, PARAMS)

    assert len(records) == 1
    assert records[0]["reporterCode"] == 842
    assert records[0]["primaryValue"] == decimal.Decimal("1000")


# 4. Pydantic Schema 驗證失敗
def test_fetch_records_invalid_schema():
    def handler(request):
        return make_response(200, [{"reporterCode": 842}])

    http_client, client = make_client(handler)

    with http_client:
        with pytest.raises(pydantic.ValidationError):
            extract.fetch_records(client, PARAMS)


# 5. World + Partner：成功取得資料
def test_fetch_trade_data_success():
    world_records = [make_record(0, 1000)]
    partner_records = [make_record(0, 1000), make_record(156, 600), make_record(392, 400)]
    requests_seen = []

    def handler(request):
        requests_seen.append(request)
        if request.url.params.get("partnerCode") == "0":
            return make_response(200, world_records)
        return make_response(200, partner_records)

    http_client, client = make_client(handler)

    with http_client:
        result = extract.fetch_trade_data(client, PARAMS)

    assert len(requests_seen) == 2
    assert result["world"][0]["primaryValue"] == decimal.Decimal("1000")
    assert [record["partnerCode"] for record in result["partners"]] == [156, 392]

    world_params = requests_seen[0].url.params
    partner_params = requests_seen[1].url.params

    assert world_params["partnerCode"] == "0"
    assert "partnerCode" not in partner_params


# 6. HTTP 400：不應重試
def test_http_400_no_retry():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        return make_response(400)

    http_client, client = make_client(handler)

    with http_client:
        with pytest.raises(httpx.HTTPStatusError):
            client.get(PARAMS)

    assert attempts == 1


# 7. HTTP 429：應該重試
def test_http_429_retry():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        return make_response(429 if attempts == 1 else 200)

    http_client, client = make_client(handler)

    with http_client:
        response = client.get(PARAMS)

    assert response.status_code == 200
    assert attempts == 2


# 8. HTTP 500：應該重試
def test_http_500_retry():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        return make_response(500 if attempts == 1 else 200)

    http_client, client = make_client(handler)

    with http_client:
        response = client.get(PARAMS)

    assert response.status_code == 200
    assert attempts == 2


# 9. Timeout：應該重試
def test_timeout_retry():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("Request timed out", request=request)
        return make_response(200)

    http_client, client = make_client(handler)

    with http_client:
        response = client.get(PARAMS)

    assert response.status_code == 200
    assert attempts == 2


# 10. 超過最大重試次數
def test_retry_exhausted():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        return make_response(500)

    http_client, client = make_client(handler, max_attempts=3)

    with http_client:
        with pytest.raises(httpx.HTTPStatusError):
            client.get(PARAMS)

    assert attempts == 3
