import logging
import json
from decimal import Decimal
from typing import Any

from trade_analytics.ingestion.client import ComtradeClient
from trade_analytics.ingestion.query import ComtradeQuery, QueryType
from trade_analytics.ingestion.schemas import ComtradeResponse


logger = logging.getLogger(__name__)


def fetch_trade_data(client: ComtradeClient, base_params: dict) -> dict[str, list[dict[str, Any]]]:
    query_config = {
        "period": base_params["period"],
        "cmd_code": base_params["cmdCode"],
        "reporter_code": base_params["reporterCode"],
    }

    # World：美國從全球進口某項商品的總金額
    # partnerCode=0 代表 World（全球總額）
    world_query = ComtradeQuery(**query_config, query_type=QueryType.WORLD_TOTAL)
    world_records = fetch_records(client, world_query.to_params())

    # Partner：美國分別從哪些國家進口該商品，以及各國的進口金額
    # 因為不一定有各國的進口資料，所以不直接用 partner 加總算出總額
    # API 沒給 partnerCode 就會回傳全部 partner 資料
    partner_query = ComtradeQuery(**query_config, query_type=QueryType.PARTNER_DETAIL)
    partner_records = fetch_records(client, partner_query.to_params())
    partner_records = [record for record in partner_records if record.get("partnerCode") != 0]

    return {"world": world_records, "partners": partner_records}


def fetch_records(client: ComtradeClient, params: dict[str, str | int]) -> list[dict[str, Any]]:
    """取得 UN Comtrade 原始交易紀錄並驗證 Schema"""
    response = client.get(params)
    payload = parse_payload(response.text)
    validated_response = ComtradeResponse.model_validate(payload)

    logger.debug("API Schema 驗證通過：%d 筆", len(validated_response.data))
    return [record.model_dump(by_alias=True) for record in validated_response.data]


def parse_payload(response_text: str) -> dict:
    """解析 JSON 並驗證 Payload"""
    payload = json.loads(response_text, parse_float=Decimal)

    if not isinstance(payload, dict):
        raise ValueError("API 回應不是 JSON Object")

    if payload.get("error"):
        raise ValueError(f"API 回報錯誤：{payload['error']}")

    return payload
