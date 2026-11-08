import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx

from trade_analytics.ingestion import batch, query, schemas

logger = logging.getLogger(__name__)

PREVIEW_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"


@dataclass(frozen=True)
class RealSourceResult:
    batch_result: batch.BatchResult
    response_payload: dict[str, Any]


def build_api_params(
        comtrade_query: query.ComtradeQuery,
) -> dict[str, str]:
    """將專案 Query Model 轉換為 UN Comtrade API Parameters。"""
    params = {
        "reporterCode": str(comtrade_query.reporter_code),
        "period": comtrade_query.period,
        "cmdCode": comtrade_query.cmd_code,
        "flowCode": "M",
    }

    if comtrade_query.query_type == query.QueryType.WORLD_TOTAL:
        params["partnerCode"] = "0"
    elif comtrade_query.query_type != query.QueryType.PARTNER_DETAIL:
        raise ValueError("不支援的 Query Type")

    return params


def fetch_real_source(
        comtrade_query: query.ComtradeQuery,
        *,
        timeout: float = 30.0,
) -> RealSourceResult:
    """取得真實 API Response，保留完整來源證據。"""
    params = build_api_params(comtrade_query)

    logger.info(
        "開始擷取 UN Comtrade：period=%s reporter=%s cmd=%s type=%s",
        comtrade_query.period,
        comtrade_query.reporter_code,
        comtrade_query.cmd_code,
        comtrade_query.query_type.value,
    )

    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        response = client.get(PREVIEW_URL, params=params)
        response.raise_for_status()

        # 避免 API 浮點數先轉為 float 再失去十進位精度。
        response_payload = response.json(parse_float=Decimal)

    parsed = schemas.ComtradeResponse.model_validate(response_payload)

    # 保留 API Alias；與目前 BatchResult.records 契約一致。
    all_records = [
        record.model_dump(by_alias=True)
        for record in parsed.data
    ]

    if comtrade_query.query_type == query.QueryType.WORLD_TOTAL:
        selected = [
            row for row in all_records
            if row["partnerCode"] == 0
        ]
    else:
        selected = [
            row for row in all_records
            if row["partnerCode"] != 0
        ]

    result = batch.BatchResult(
        query=comtrade_query,
        records=selected,
    )

    logger.info(
        "UN Comtrade Response 收到：response_count=%s selected=%s",
        response_payload.get("count"),
        len(selected),
    )

    return RealSourceResult(
        batch_result=result,
        response_payload=response_payload,
    )
