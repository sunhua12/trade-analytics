import httpx
import logging

from trade_analytics.ingestion import batch
from trade_analytics.ingestion import client as client_module
from trade_analytics.utils import logging_config
from trade_analytics.utils import manifest as manifest_module


REPORTERS = [842, 156]
PERIODS = ["202401", "202402"]
CMD_CODES = ["854231", "854232"]

MAX_ATTEMPTS = 3
TIMEOUT = 30.0

logger = logging.getLogger("trade_analytics.main")


def main():
    logging_config.setup_logging()
    # 1. Batch Query Planning
    queries = batch.plan_batch_queries(reporters=REPORTERS, periods=PERIODS, cmd_codes=CMD_CODES)

    logger.info(f"預計執行 {len(queries)} 組 Query")

    # 2. Batch Execution
    with httpx.Client(timeout=TIMEOUT) as http_client:
        client = client_module.ComtradeClient(http_client, max_attempts=MAX_ATTEMPTS)
        results = batch.execute_batch(client, queries)

    # 3. Manifest & Checksum
    manifest = manifest_module.build_manifest(results)
    manifest_module.verify_manifest(results, manifest)

    logger.info("成功完成 %d 組 Query，Manifest 驗證通過", len(results))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("批次執行失敗")
        raise
