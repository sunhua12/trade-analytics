
import pytest

from google.cloud import bigquery

from trade_analytics.warehouse.bigquery_config import BigQueryConfig
from trade_analytics.warehouse.bigquery_schema import (
    LANDING_TABLE,
    RAW_TABLE,
    INGESTION_RUNS_TABLE,
    LANDING_SCHEMA,
    RAW_SCHEMA,
    INGESTION_RUNS_SCHEMA,
    build_landing_table,
    build_raw_table,
    build_ingestion_runs_table,
    build_table_definitions,
)


@pytest.fixture
def config() -> BigQueryConfig:
    return BigQueryConfig(project_id="test-project", location="asia-northeast1")


def get_fields(schema: list[bigquery.SchemaField]) -> dict[str, bigquery.SchemaField]:
    return {field.name: field for field in schema}


def test_table_names():
    assert LANDING_TABLE == "comtrade_landing"
    assert RAW_TABLE == "comtrade_records"
    assert INGESTION_RUNS_TABLE == "ingestion_runs"


def test_build_landing_table(config):
    table = build_landing_table(config)

    assert table.reference.project == "test-project"
    assert table.reference.dataset_id == "trade_raw"
    assert table.reference.table_id == "comtrade_landing"
    assert table.schema == LANDING_SCHEMA
    assert table.time_partitioning is None
    assert table.clustering_fields is None


def test_build_raw_table(config):
    table = build_raw_table(config)

    assert table.reference.project == "test-project"
    assert table.reference.dataset_id == "trade_raw"
    assert table.reference.table_id == "comtrade_records"
    assert table.schema == RAW_SCHEMA
    assert table.time_partitioning.type_ == "MONTH"
    assert table.time_partitioning.field == "period_start_date"
    assert table.time_partitioning.expiration_ms == 0
    assert table.clustering_fields == ["reporter_code", "cmd_code"]


def test_build_ingestion_runs_table(config):
    table = build_ingestion_runs_table(config)

    assert table.reference.project == "test-project"
    assert table.reference.dataset_id == "trade_raw"
    assert table.reference.table_id == "ingestion_runs"
    assert table.schema == INGESTION_RUNS_SCHEMA
    assert table.time_partitioning is None
    assert table.clustering_fields is None


def test_build_table_definitions(config):
    tables = build_table_definitions(config)

    assert len(tables) == 3
    assert [table.table_id for table in tables] == [
        "comtrade_landing",
        "comtrade_records",
        "ingestion_runs",
    ]
    assert len({(table.project, table.dataset_id, table.table_id) for table in tables}) == 3


@pytest.mark.parametrize("schema", [LANDING_SCHEMA, RAW_SCHEMA, INGESTION_RUNS_SCHEMA])
def test_schema_has_no_duplicate_columns(schema):
    names = [field.name for field in schema]

    assert len(names) == len(set(names))


def test_landing_required_fields():
    fields = get_fields(LANDING_SCHEMA)

    required_columns = {
        "batch_id",
        "source_query_key",
        "query_type",
        "source_row_index",
        "period",
        "reporter_code",
        "partner_code",
        "cmd_code",
        "primary_value_text",
        "source_record_json",
    }

    assert required_columns.issubset(fields.keys())
    assert all(fields[name].mode == "REQUIRED" for name in required_columns)


def test_landing_numeric_fields_use_string():
    fields = get_fields(LANDING_SCHEMA)

    assert fields["primary_value_text"].field_type == "STRING"
    assert fields["primary_value_text"].mode == "REQUIRED"
    assert fields["net_weight_text"].field_type == "STRING"
    assert fields["net_weight_text"].mode == "NULLABLE"
    assert fields["quantity_text"].field_type == "STRING"
    assert fields["quantity_text"].mode == "NULLABLE"


def test_landing_hs_version_nullable():
    fields = get_fields(LANDING_SCHEMA)

    assert fields["classification_code"].field_type == "STRING"
    assert fields["hs_version"].field_type == "STRING"
    assert fields["hs_version"].mode == "NULLABLE"


def test_raw_primary_value_precision():
    fields = get_fields(RAW_SCHEMA)

    assert fields["primary_value"].field_type == "BIGNUMERIC"
    assert fields["primary_value"].mode == "REQUIRED"
    assert fields["net_weight"].field_type == "BIGNUMERIC"
    assert fields["net_weight"].mode == "NULLABLE"
    assert fields["quantity"].field_type == "BIGNUMERIC"
    assert fields["quantity"].mode == "NULLABLE"


def test_raw_partition_field():
    fields = get_fields(RAW_SCHEMA)

    assert fields["period_start_date"].field_type == "DATE"
    assert fields["period_start_date"].mode == "REQUIRED"


def test_raw_lineage_fields():
    fields = get_fields(RAW_SCHEMA)

    required_columns = {
        "batch_id",
        "source_query_key",
        "revision_id",
        "ingested_at",
        "source_record_json",
        "source_row_index",
    }

    assert required_columns.issubset(fields.keys())
    assert all(fields[name].mode == "REQUIRED" for name in required_columns)
    assert fields["ingested_at"].field_type == "TIMESTAMP"


def test_ingestion_runs_metadata_fields():
    fields = get_fields(INGESTION_RUNS_SCHEMA)

    expected_types = {
        "batch_id": "STRING",
        "source_query_key": "STRING",
        "revision_id": "STRING",
        "query_type": "STRING",
        "status": "STRING",
        "record_count": "INT64",
        "loaded_record_count": "INT64",
        "sha256": "STRING",
        "size_bytes": "INT64",
        "load_job_id": "STRING",
        "started_at": "TIMESTAMP",
        "completed_at": "TIMESTAMP",
        "error_message": "STRING",
    }

    assert expected_types.keys() <= fields.keys()
    assert all(fields[name].field_type == expected_type for name, expected_type in expected_types.items())


def test_ingestion_runs_required_fields():
    fields = get_fields(INGESTION_RUNS_SCHEMA)

    required_columns = {"batch_id", "source_query_key", "query_type", "status", "started_at"}

    assert all(fields[name].mode == "REQUIRED" for name in required_columns)
    assert fields["completed_at"].mode == "NULLABLE"
    assert fields["error_message"].mode == "NULLABLE"
