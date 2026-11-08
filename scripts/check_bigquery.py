import logging

from trade_analytics.warehouse import bigquery_client
from trade_analytics.warehouse import bigquery_config
from trade_analytics.utils import logging_config


logger = logging.getLogger("trade_analytics.check_bigquery")


def main():
    logging_config.setup_logging()

    config = bigquery_config.load_bigquery_config()
    client = bigquery_client.create_bigquery_client(config)

    try:
        bigquery_client.validate_datasets(client, config)
        logger.info("BigQuery 環境驗證完成")
    finally:
        client.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("BigQuery 環境驗證失敗")
        raise