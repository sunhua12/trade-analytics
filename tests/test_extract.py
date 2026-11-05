import json
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
import requests

from trade_analytics.extract import (
    fetch_trade_data,
    parse_payload,
    validate_records,
    request_with_retry
)

# 共用測試資料
PARAMS = {
    "period": "202401",
    "reporterCode": "842",
    "flowCode": "M",
    "cmdCode": "854231",
    "partnerCode": "0",
}


def test_api():
    payload = fetch_trade_data(PARAMS)

    # 觀察 JSON 外層
    print("外層資料型別：", type(payload).__name__)
    print("外層欄位：", list(payload.keys()))

    if payload.get("error"):
        raise RuntimeError(payload["error"])

    rows = payload.get("data", [])
    print("完整資料：", rows)

    print("\n實際回傳筆數：", len(rows))

    # 查看一筆完整紀錄
    if rows:
        print("\n第一筆資料：")
        print(json.dumps(rows[0], indent=2, ensure_ascii=False))

        print("\n第一筆的欄位、型別與值：")
        for field, value in rows[0].items():
            print(f"{field:30} {type(value).__name__:12} {value!r}")


def make_response(status_code=200, data=None):
    """建立假的 HTTP Response。"""

    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(
        {"data": data if data is not None else []}
    ).encode("utf-8")

    return response


# 1. JSON 解析成功
def test_parse_payload():
    text = '{"data": [{"primaryValue": 123.45}]}'

    result = parse_payload(text)

    assert result["data"][0]["primaryValue"] == Decimal("123.45")


# 2. JSON 結構錯誤
def test_parse_payload_invalid():
    with pytest.raises(ValueError):
        parse_payload("[]")


# 3. Records 正常
def test_validate_records():
    records = [{"reporterCode": 842}]

    result = validate_records(records)

    assert result == records


# 4. Records 格式錯誤
def test_validate_records_invalid():
    with pytest.raises(ValueError):
        validate_records({"reporterCode": 842})


# 5. HTTP 200：成功取得資料
def test_fetch_records_success():
    records = [{"reporterCode": 842, "primaryValue": 100}]

    with patch(
            "trade_analytics.extract.requests.get"
    ) as mock_get:
        mock_get.return_value = make_response(
            status_code=200,
            data=records,
        )

        result = fetch_trade_data(PARAMS)

    assert result == records
    mock_get.assert_called_once()


# 6. HTTP 400：不應重試
def test_http_400_no_retry():
    with patch(
            "trade_analytics.extract.requests.get"
    ) as mock_get:
        mock_get.return_value = make_response(
            status_code=400
        )

        with pytest.raises(requests.HTTPError):
            request_with_retry(PARAMS)

    assert mock_get.call_count == 1


# 7. HTTP 429：應該重試
def test_http_429_retry():
    with (
        patch("trade_analytics.extract.requests.get") as mock_get,
        patch("trade_analytics.extract.time.sleep") as mock_sleep,
    ):
        mock_get.side_effect = [
            make_response(429),
            make_response(200),
        ]

        result = request_with_retry(PARAMS)

    assert result.status_code == 200
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(1)


# 8. HTTP 500：應該重試
def test_http_500_retry():
    with (
        patch("trade_analytics.extract.requests.get") as mock_get,
        patch("trade_analytics.extract.time.sleep"),
    ):
        mock_get.side_effect = [
            make_response(500),
            make_response(200),
        ]

        result = request_with_retry(PARAMS)

    assert result.status_code == 200
    assert mock_get.call_count == 2


# 9. Timeout：應該重試
def test_timeout_retry():
    with (
        patch("trade_analytics.extract.requests.get") as mock_get,
        patch("trade_analytics.extract.time.sleep"),
    ):
        mock_get.side_effect = [
            requests.Timeout("Request timed out"),
            make_response(200),
        ]

        result = request_with_retry(PARAMS)

    assert result.status_code == 200
    assert mock_get.call_count == 2


# 10. 超過最大重試次數
def test_retry_exhausted():
    with (
        patch("trade_analytics.extract.requests.get") as mock_get,
        patch("trade_analytics.extract.time.sleep"),
    ):
        mock_get.return_value = make_response(500)

        with pytest.raises(RuntimeError):
            request_with_retry(PARAMS)

    assert mock_get.call_count == 3
