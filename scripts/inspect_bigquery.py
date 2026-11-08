import logging

from google.cloud import bigquery

from trade_analytics.warehouse.bigquery_client import create_bigquery_client, validate_datasets
from trade_analytics.warehouse.bigquery_config import load_bigquery_config
from trade_analytics.utils.logging_config import setup_logging


logger = logging.getLogger("trade_analytics.inspect_bigquery")


def log_schema(fields: list[bigquery.SchemaField], prefix: str = "") -> None:
    """記錄欄位名稱、型別、Nullability 與巢狀欄位。"""
    for field in fields:
        field_name = f"{prefix}{field.name}"
        logger.info("Column：name=%s type=%s mode=%s precision=%s scale=%s", field_name, field.field_type, field.mode, field.precision, field.scale)

        if field.fields:
            log_schema(list(field.fields), prefix=f"{field_name}.")


def inspect_table(client: bigquery.Client, table_item) -> None:
    """讀取單張 Table 的 Metadata 與 Schema。"""
    table = client.get_table(table_item.reference)

    logger.info("--------------------------------------------------")
    logger.info("Table：%s", table.full_table_id)
    logger.info("Type：%s", table.table_type)
    logger.info("Rows：%s", table.num_rows)
    logger.info("Size Bytes：%s", table.num_bytes)
    logger.info("Location：%s", table.location)

    if table.time_partitioning:
        partitioning = table.time_partitioning
        logger.info("Partitioning：type=%s field=%s", partitioning.type_, partitioning.field)
    elif table.range_partitioning:
        logger.info("Range Partitioning：%s", table.range_partitioning)
    else:
        logger.info("Partitioning：None")

    logger.info("Clustering：%s", table.clustering_fields)
    logger.info("Require Partition Filter：%s", table.require_partition_filter)

    if not table.schema:
        logger.info("Schema：無欄位資訊")
        return

    log_schema(list(table.schema))


def inspect_dataset(client: bigquery.Client, project_id: str, dataset_id: str) -> None:
    """檢查指定 Dataset 下的所有 Tables。"""
    full_dataset_id = f"{project_id}.{dataset_id}"
    dataset = client.get_dataset(full_dataset_id)

    logger.info("==================================================")
    logger.info("Dataset：%s", full_dataset_id)
    logger.info("Location：%s", dataset.location)

    tables = list(client.list_tables(full_dataset_id))
    logger.info("Table Count：%d", len(tables))

    for table_item in tables:
        inspect_table(client, table_item)


def main() -> None:
    setup_logging()

    config = load_bigquery_config()
    client = create_bigquery_client(config)

    try:
        validate_datasets(client, config)
        inspect_dataset(client, config.project_id, config.raw_dataset)
        inspect_dataset(client, config.project_id, config.analytics_dataset)
        logger.info("BigQuery Schema 檢查完成")
    finally:
        client.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("BigQuery Schema 檢查失敗")
        raise