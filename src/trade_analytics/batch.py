from dataclasses import dataclass
from itertools import product
from typing import Any

from trade_analytics.client import ComtradeClient
from trade_analytics.extract import fetch_records
from trade_analytics.query import ComtradeQuery, QueryType


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

    return queries


def execute_batch(client: ComtradeClient, queries: list[ComtradeQuery]) -> list[BatchResult]:
    """依序執行批次查詢，保存每個 Query 的結果。"""
    results = []

    for query in queries:
        records = fetch_records(client, query.to_params())

        if query.query_type == QueryType.PARTNER_DETAIL:
            records = [record for record in records if record["partnerCode"] != 0]

        results.append(BatchResult(query=query, records=records))

    return results