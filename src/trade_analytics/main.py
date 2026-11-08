import httpx
import logging

from trade_analytics.ingestion.batch import execute_batch, plan_batch_queries
from trade_analytics.ingestion.client import ComtradeClient
from trade_analytics.utils.logging_config import setup_logging
from trade_analytics.utils.manifest import build_manifest, verify_manifest


REPORTERS = [842, 156]
PERIODS = ["202401", "202402"]
CMD_CODES = ["854231", "854232"]

MAX_ATTEMPTS = 3
TIMEOUT = 30.0

logger = logging.getLogger("trade_analytics.main")


def main():
    setup_logging()
    # 1. Batch Query Planning
    queries = plan_batch_queries(reporters=REPORTERS, periods=PERIODS, cmd_codes=CMD_CODES)

    logger.info(f"預計執行 {len(queries)} 組 Query")

    # 2. Batch Execution
    with httpx.Client(timeout=TIMEOUT) as http_client:
        client = ComtradeClient(http_client, max_attempts=MAX_ATTEMPTS)
        results = execute_batch(client, queries)

    # 3. Manifest & Checksum
    manifest = build_manifest(results)
    verify_manifest(results, manifest)

    logger.info("成功完成 %d 組 Query，Manifest 驗證通過", len(results))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("批次執行失敗")
        raise
