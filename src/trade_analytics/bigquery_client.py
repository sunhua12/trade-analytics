import logging

from google.cloud import bigquery

from trade_analytics.bigquery_config import BigQueryConfig


logger = logging.getLogger(__name__)


def create_bigquery_client(config: BigQueryConfig) -> bigquery.Client:
    """使用 ADC 建立 BigQuery Client。"""
    client = bigquery.Client(project=config.project_id, location=config.location)
    logger.info("BigQuery Client 已建立：project=%s location=%s", config.project_id, config.location)
    return client


def validate_datasets(client: bigquery.Client, config: BigQueryConfig) -> None:
    """確認 Dataset 存在，而且 Location 與設定一致。"""
    for dataset_id in (config.raw_dataset, config.analytics_dataset):
        full_id = f"{config.project_id}.{dataset_id}"
        dataset = client.get_dataset(full_id)

        if dataset.location is None or dataset.location.lower() != config.location.lower():
            raise ValueError(f"Dataset Location 不一致：dataset={full_id} actual={dataset.location} expected={config.location}")

        logger.info("BigQuery Dataset 驗證成功：dataset=%s location=%s", full_id, dataset.location)