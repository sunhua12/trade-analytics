import logging
import dataclasses
import itertools
import typing

from trade_analytics.ingestion import client as client_module
from trade_analytics.ingestion import extract
from trade_analytics.ingestion import query as query_module


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BatchResult:
    query: query_module.ComtradeQuery
    records: list[dict[str, typing.Any]]


def plan_batch_queries(reporters: list[int], periods: list[str], cmd_codes: list[str]) -> list[query_module.ComtradeQuery]:
    """產生批次查詢計畫。"""
    queries = []

    for reporter, period, cmd_code in itertools.product(reporters, periods, cmd_codes):
        for query_type in (query_module.QueryType.WORLD_TOTAL, query_module.QueryType.PARTNER_DETAIL):
            query = query_module.ComtradeQuery(reporter_code=reporter, period=period, cmd_code=cmd_code, query_type=query_type)
            queries.append(query)

    logger.debug("查詢計畫已建立：%d 組", len(queries))
    return queries


def execute_batch(client: client_module.ComtradeClient, queries: list[query_module.ComtradeQuery]) -> list[BatchResult]:
    """依序執行批次查詢，保存每個 Query 的結果。"""
    results = []

    for query in queries:
        records = extract.fetch_records(client, query.to_params())

        if query.query_type == query_module.QueryType.PARTNER_DETAIL:
            records = [record for record in records if record["partnerCode"] != 0]

        results.append(BatchResult(query=query, records=records))
        logger.info("查詢完成：reporter=%s period=%s cmd=%s type=%s，%d 筆",
                    query.reporter_code, query.period, query.cmd_code,
                    query.query_type.value, len(records))

    return results
