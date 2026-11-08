import logging

from trade_analytics.bigquery_client import create_bigquery_client, validate_datasets
from trade_analytics.bigquery_config import load_bigquery_config
from trade_analytics.logging_config import setup_logging


logger = logging.getLogger("trade_analytics.check_bigquery")


def main():
    setup_logging()

    config = load_bigquery_config()
    client = create_bigquery_client(config)

    try:
        validate_datasets(client, config)
        logger.info("BigQuery 環境驗證完成")
    finally:
        client.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("BigQuery 環境驗證失敗")
        raise