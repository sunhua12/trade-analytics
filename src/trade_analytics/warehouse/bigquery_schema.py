
from google.cloud import bigquery

from trade_analytics.warehouse import bigquery_config


LANDING_TABLE = "comtrade_landing"
RAW_TABLE = "comtrade_records"
INGESTION_RUNS_TABLE = "ingestion_runs"


LANDING_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_query_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("query_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_index", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("period", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("reporter_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("partner_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("cmd_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("primary_value_text", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("flow_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("classification_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("hs_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("freq_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("partner2_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("customs_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mot_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_original_classification", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("net_weight_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("quantity_text", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("source_record_json", "STRING", mode="REQUIRED"),
]


RAW_SCHEMA = [
    bigquery.SchemaField("period_start_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("period", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("reporter_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("partner_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("cmd_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("primary_value", "BIGNUMERIC", mode="REQUIRED"),
    bigquery.SchemaField("query_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("flow_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("classification_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("hs_version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("freq_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("partner2_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("customs_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mot_code", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_original_classification", "BOOL", mode="REQUIRED"),
    bigquery.SchemaField("net_weight", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("quantity", "BIGNUMERIC", mode="NULLABLE"),
    bigquery.SchemaField("source_record_json", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_row_index", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_query_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("revision_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


INGESTION_RUNS_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_query_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("revision_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("query_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("record_count", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("loaded_record_count", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("sha256", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("size_bytes", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("load_job_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
]


def build_landing_table(config: bigquery_config.BigQueryConfig) -> bigquery.Table:
    """建立 Landing Table 定義，不會呼叫 BigQuery API。"""
    table = bigquery.Table(config.raw_table(LANDING_TABLE), schema=LANDING_SCHEMA)
    table.description = "UN Comtrade batch landing records pending validation."
    return table


def build_raw_table(config: bigquery_config.BigQueryConfig) -> bigquery.Table:
    """建立 Raw Table 定義，使用月份分區與 Reporter、商品叢集。"""
    table = bigquery.Table(config.raw_table(RAW_TABLE), schema=RAW_SCHEMA)
    table.description = "Validated UN Comtrade raw records with ingestion lineage."
    # 明確覆蓋 Dataset 的預設分區到期時間，保留歷史月份資料。
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="period_start_date",
        expiration_ms=0,
    )
    table.clustering_fields = ["reporter_code", "cmd_code"]
    return table


def build_ingestion_runs_table(config: bigquery_config.BigQueryConfig) -> bigquery.Table:
    """建立 Ingestion Metadata Table 定義。"""
    table = bigquery.Table(config.raw_table(INGESTION_RUNS_TABLE), schema=INGESTION_RUNS_SCHEMA)
    table.description = "UN Comtrade ingestion execution and revision metadata."
    return table


def build_table_definitions(config: bigquery_config.BigQueryConfig) -> list[bigquery.Table]:
    """回傳所有 BigQuery Table 定義，不會建立雲端資源。"""
    return [
        build_landing_table(config),
        build_raw_table(config),
        build_ingestion_runs_table(config),
    ]
