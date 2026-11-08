import os
import dataclasses


@dataclasses.dataclass(frozen=True)
class BigQueryConfig:
    project_id: str
    location: str
    raw_dataset: str = "trade_raw"
    analytics_dataset: str = "trade_analytics"

    def raw_table(self, table_name: str) -> str:
        return f"{self.project_id}.{self.raw_dataset}.{table_name}"

    def analytics_table(self, table_name: str) -> str:
        return f"{self.project_id}.{self.analytics_dataset}.{table_name}"


def load_bigquery_config() -> BigQueryConfig:
    """從環境變數載入 BigQuery 設定。"""
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    location = os.getenv("BQ_LOCATION", "").strip()
    raw_dataset = os.getenv("BQ_RAW_DATASET", "trade_raw").strip()
    analytics_dataset = os.getenv("BQ_ANALYTICS_DATASET", "trade_analytics").strip()

    if not project_id:
        raise ValueError("缺少 GCP_PROJECT_ID 環境變數")

    if not location:
        raise ValueError("缺少 BQ_LOCATION 環境變數")

    if not raw_dataset or not analytics_dataset:
        raise ValueError("BigQuery Dataset 名稱不可為空")

    return BigQueryConfig(project_id=project_id, location=location, raw_dataset=raw_dataset, analytics_dataset=analytics_dataset)