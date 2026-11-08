
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from trade_analytics.bigquery_config import BigQueryConfig
from trade_analytics.bigquery_schema import build_table_definitions


logger = logging.getLogger(__name__)





def schema_signature(fields: list[bigquery.SchemaField]) -> tuple:
    """將 BigQuery 的 Table Provisioning 與 ETL Data Loading 分離。
    Provisioning 模組會根據 Python 定義的 Schema 檢查 BigQuery Tables 是否存在，並驗證欄位型別、Partitioning、Clustering 等設定。
    為了避免破壞既有資料，我採用 Dry Run 與顯式 Apply 的設計。若 Table 已存在但設定不一致，程式會直接停止，而不是自動覆寫。
    這讓基礎建設具備可重複執行性（Idempotency），也能在資料載入前偵測 Schema Drift，降低 Pipeline 因環境設定不一致而失敗的風險。"""

    # 以標準化型別比較 Schema，保留欄位順序、模式及精度。
    return tuple(
        (
            field.name,
            normalize_field_type(field.field_type),
            field.mode.upper(),
            field.precision,
            field.scale,
            schema_signature(list(field.fields)),
        )
        for field in fields
    )

def normalize_field_type(field_type: str) -> str:
    """統一 BigQuery 欄位型別的同義名稱。"""
    aliases = {
        "INT64": "INTEGER",
        "BOOL": "BOOLEAN",
        "FLOAT64": "FLOAT",
        "STRUCT": "RECORD",
        "DECIMAL": "NUMERIC",
        "BIGDECIMAL": "BIGNUMERIC",
    }

    normalized = field_type.upper()
    return aliases.get(normalized, normalized)

def partition_signature(table: bigquery.Table) -> tuple | None:
    """取得 Time Partitioning 的關鍵設定。"""
    partitioning = table.time_partitioning

    if partitioning is None:
        return None

    return (partitioning.type_, partitioning.field, partitioning.expiration_ms)


def validate_table(actual: bigquery.Table, expected: bigquery.Table) -> None:
    """確認雲端 Table 與本地 Schema 定義一致。"""
    table_id = f"{expected.project}.{expected.dataset_id}.{expected.table_id}"
    errors = []

    if actual.table_type != "TABLE":
        errors.append(f"Table Type 不一致：actual={actual.table_type}")

    actual_schema = schema_signature(list(actual.schema))
    expected_schema = schema_signature(list(expected.schema))

    if actual_schema != expected_schema:
        for index in range(max(len(actual_schema), len(expected_schema))):
            actual_field = actual_schema[index] if index < len(actual_schema) else None
            expected_field = expected_schema[index] if index < len(expected_schema) else None

            if actual_field != expected_field:
                logger.error("Schema 差異：index=%d actual=%s expected=%s", index, actual_field, expected_field)

        errors.append("Schema 不一致")

    actual_partition = partition_signature(actual)
    expected_partition = partition_signature(expected)
    if (
        (actual_partition is None) != (expected_partition is None)
        or (
            actual_partition is not None
            and expected_partition is not None
            and (
                actual_partition[:2] != expected_partition[:2]
                or (expected_partition[2] is not None and (actual_partition[2] or 0) != expected_partition[2])
            )
        )
    ):
        errors.append(f"Partitioning 不一致：actual={actual_partition} expected={expected_partition}")

    if (actual.range_partitioning is None) != (expected.range_partitioning is None):
        errors.append("Range Partitioning 不一致")

    if (actual.clustering_fields or []) != (expected.clustering_fields or []):
        errors.append("Clustering 不一致")

    if bool(actual.require_partition_filter) != bool(expected.require_partition_filter):
        errors.append("Require Partition Filter 不一致")

    if errors:
        raise ValueError(f"BigQuery Table 設定不一致：{table_id}；{'；'.join(errors)}")

    logger.info("BigQuery Table 驗證通過：%s", table_id)


def provision_table(client: bigquery.Client, expected: bigquery.Table, apply: bool = False) -> str:
    """檢查或建立單張 Table，不修改既有 Table。"""
    table_id = f"{expected.project}.{expected.dataset_id}.{expected.table_id}"

    try:
        actual = client.get_table(expected.reference)
    except NotFound:
        if not apply:
            logger.info("Dry Run：Table 尚未存在，預計建立：%s", table_id)
            return "planned"

        client.create_table(expected, exists_ok=False)
        logger.info("BigQuery Table 已建立：%s", table_id)

        actual = client.get_table(expected.reference)
        validate_table(actual, expected)
        return "created"

    validate_table(actual, expected)
    return "existing"


def provision_tables(client: bigquery.Client, config: BigQueryConfig, apply: bool = False) -> dict[str, str]:
    """檢查或建立 Stage 5 所需的三張 Table。"""
    definitions = build_table_definitions(config)
    results = {}

    # 先檢查所有既有 Table，避免發現 Schema Drift 後仍建立其他 Table。
    for table in definitions:
        table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"

        try:
            actual = client.get_table(table.reference)
        except NotFound:
            continue

        validate_table(actual, table)
        logger.info("既有 Table 預先檢查通過：%s", table_id)

    for table in definitions:
        results[table.table_id] = provision_table(client, table, apply=apply)

    return results
