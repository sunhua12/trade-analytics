import httpx

from trade_analytics.batch import execute_batch, plan_batch_queries
from trade_analytics.client import ComtradeClient
from trade_analytics.manifest import build_manifest, verify_manifest


REPORTERS = [842, 156]
PERIODS = ["202401", "202402"]
CMD_CODES = ["854231", "854232"]

MAX_ATTEMPTS = 3
TIMEOUT = 30.0


def main():
    # 1. Batch Query Planning
    queries = plan_batch_queries(reporters=REPORTERS, periods=PERIODS, cmd_codes=CMD_CODES)

    print(f"預計執行 {len(queries)} 組 Query")

    # 2. Batch Execution
    with httpx.Client(timeout=TIMEOUT) as http_client:
        client = ComtradeClient(http_client, max_attempts=MAX_ATTEMPTS)
        results = execute_batch(client, queries)

    # 3. Manifest & Checksum
    manifest = build_manifest(results)
    verify_manifest(results, manifest)

    # 4. 輸出執行結果
    for result in results:
        query = result.query
        print(f"{query.reporter_code} / {query.period} / {query.cmd_code} / {query.query_type.value}：{len(result.records)} 筆")

    print(f"成功完成 {len(results)} 組 Query")
    print(f"Manifest Entries：{len(manifest.entries)}")

    for entry in manifest.entries:
        print(f"{entry.query['reporter_code']} / {entry.query['period']} / {entry.query['cmd_code']} / {entry.query['query_type']}：SHA-256 {entry.sha256}")


if __name__ == "__main__":
    main()