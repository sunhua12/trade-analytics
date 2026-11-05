import json
import time
from decimal import Decimal

import requests
from requests import Response

BASE_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
MAX_RETRIES = 3
TIMEOUT = 30


def fetch_trade_data(base_params: dict) -> dict:
    """分別取得 World 與 Partner 資料。"""
    common_params = {**base_params, "partner2Code": 0, "customsCode": "C00", "motCode": 0, }

    # World：美國從全球進口某項商品的總金額
    # partnerCode=0 代表 World（全球總額）
    world_params = {**common_params, "partnerCode": 0, }

    # Partner：美國分別從哪些國家進口該商品，以及各國的進口金額
    # 因為不一定有各國的進口資料，所以不直接用partner加總算出總額的方式
    # API 沒給 partnerCode 就會回傳全部 partner 資料
    partner_params = {key: value for key, value in common_params.items() if key != "partnerCode"}

    world_records = fetch_records(world_params)
    partner_records = fetch_records(partner_params)

    # 排除 World，避免重複納入 Partner 明細
    partner_records = [record for record in partner_records if record.get("partnerCode") != 0]

    return {"world": world_records, "partners": partner_records, }


def fetch_records(params: dict) -> list[dict]:
    """取得 UN Comtrade 原始交易紀錄"""
    response = request_with_retry(params)
    payload = parse_payload(response.text)

    return validate_records(payload.get("data"))


def request_with_retry(params: dict) -> Response:
    """發送 HTTP 請求，對暫時性錯誤進行重試"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(BASE_URL, params=params, timeout=TIMEOUT, )
            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            if not is_retryable(exc):
                raise

            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"API 請求失敗，已嘗試 {MAX_RETRIES} 次") from exc

            time.sleep(2 ** attempt)

    raise RuntimeError("API 請求未完成")


def is_retryable(exc: requests.RequestException) -> bool:
    """判斷 HTTP 錯誤是否適合重試"""
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True

    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code

        return status == 429 or 500 <= status < 600

    return False


def parse_payload(response_text: str) -> dict:
    """解析 JSON 並驗證 Payload"""
    payload = json.loads(response_text, parse_float=Decimal, )

    if not isinstance(payload, dict):
        raise ValueError("API 回應不是 JSON Object")

    if payload.get("error"):
        raise ValueError(f"API 回報錯誤：{payload['error']}")

    return payload


def validate_records(records: object) -> list[dict]:
    """驗證交易紀錄結構"""

    if not isinstance(records, list):
        raise ValueError("API 回應缺少 data 陣列")

    if not all(isinstance(row, dict) for row in records):
        raise ValueError("data 包含非 Object 紀錄")

    return records
