import logging
from dataclasses import dataclass
from itertools import product
from typing import Any

from trade_analytics.ingestion.client import ComtradeClient
from trade_analytics.ingestion.extract import fetch_records
from trade_analytics.ingestion.query import ComtradeQuery, QueryType


logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    query: ComtradeQuery
    records: list[dict[str, Any]]


def plan_batch_queries(reporters: list[int], periods: list[str], cmd_codes: list[str]) -> list[ComtradeQuery]:
    """產生批次查詢計畫。"""
    queries = []

    for reporter, period, cmd_code in product(reporters, periods, cmd_codes):
        for query_type in (QueryType.WORLD_TOTAL, QueryType.PARTNER_DETAIL):
            query = ComtradeQuery(reporter_code=reporter, period=period, cmd_code=cmd_code, query_type=query_type)
            queries.append(query)

    logger.debug("查詢計畫已建立：%d 組", len(queries))
    return queries


def execute_batch(client: ComtradeClient, queries: list[ComtradeQuery]) -> list[BatchResult]:
    """依序執行批次查詢，保存每個 Query 的結果。"""
    results = []

    for query in queries:
        records = fetch_records(client, query.to_params())

        if query.query_type == QueryType.PARTNER_DETAIL:
            records = [record for record in records if record["partnerCode"] != 0]

        results.append(BatchResult(query=query, records=records))
        logger.info("查詢完成：reporter=%s period=%s cmd=%s type=%s，%d 筆",
                    query.reporter_code, query.period, query.cmd_code,
                    query.query_type.value, len(records))

    return results
