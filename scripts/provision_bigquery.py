
import argparse
import logging

from trade_analytics.bigquery_client import create_bigquery_client, validate_datasets
from trade_analytics.bigquery_config import load_bigquery_config
from trade_analytics.bigquery_provisioning import provision_tables
from trade_analytics.logging_config import setup_logging


logger = logging.getLogger("trade_analytics.provision_bigquery")


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="BigQuery Table Provisioning & Validation")
    parser.add_argument("--apply", action="store_true", help="建立缺少的 BigQuery Tables")
    args = parser.parse_args()

    config = load_bigquery_config()
    client = create_bigquery_client(config)

    try:
        validate_datasets(client, config)

        logger.info("BigQuery Provisioning 開始：project=%s dataset=%s location=%s apply=%s", config.project_id, config.raw_dataset, config.location, args.apply)

        results = provision_tables(client, config, apply=args.apply)

        for table_name, status in results.items():
            logger.info("Provisioning Result：table=%s status=%s", table_name, status)

        logger.info("BigQuery Provisioning 驗證完成")
    finally:
        client.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("BigQuery Provisioning 失敗")
        raise
