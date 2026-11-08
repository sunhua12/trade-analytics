
from unittest import mock

import pytest
from google.api_core import exceptions
from google.cloud import bigquery

from trade_analytics.warehouse import bigquery_config
from trade_analytics.warehouse import bigquery_provisioning
from trade_analytics.warehouse import bigquery_schema

@pytest.fixture
def config():
    return bigquery_config.BigQueryConfig(project_id="test-project", location="asia-northeast1")


def make_existing_table(expected):
    """模擬 BigQuery 回傳的已建立 Table。"""
    table = bigquery.Table(expected.reference, schema=list(expected.schema))
    table.time_partitioning = expected.time_partitioning
    table.clustering_fields = expected.clustering_fields
    table.require_partition_filter = bool(expected.require_partition_filter)
    table._properties["type"] = "TABLE"
    return table


def test_schema_signature():
    first = [bigquery.SchemaField("amount", "BIGNUMERIC", mode="REQUIRED")]
    second = [bigquery.SchemaField("amount", "BIGNUMERIC", mode="REQUIRED")]
    different = [bigquery.SchemaField("amount", "FLOAT64", mode="REQUIRED")]

    assert bigquery_provisioning.schema_signature(first) == bigquery_provisioning.schema_signature(second)
    assert bigquery_provisioning.schema_signature(first) != bigquery_provisioning.schema_signature(different)


def test_dry_run_does_not_create_table(config):
    client = mock.Mock()
    client.get_table.side_effect = exceptions.NotFound("Table not found")
    expected = bigquery_schema.build_landing_table(config)

    status = bigquery_provisioning.provision_table(client, expected, apply=False)

    assert status == "planned"
    client.create_table.assert_not_called()


def test_apply_creates_missing_table(config):
    client = mock.Mock()
    expected = bigquery_schema.build_landing_table(config)
    actual = make_existing_table(expected)

    client.get_table.side_effect = [exceptions.NotFound("Table not found"), actual]

    status = bigquery_provisioning.provision_table(client, expected, apply=True)

    assert status == "created"
    client.create_table.assert_called_once_with(expected, exists_ok=False)
    assert client.get_table.call_count == 2


def test_existing_table_is_not_modified(config):
    client = mock.Mock()
    expected = bigquery_schema.build_landing_table(config)
    client.get_table.return_value = make_existing_table(expected)

    status = bigquery_provisioning.provision_table(client, expected, apply=True)

    assert status == "existing"
    client.create_table.assert_not_called()
    client.update_table.assert_not_called()
    client.delete_table.assert_not_called()


def test_schema_drift_raises(config):
    expected = bigquery_schema.build_raw_table(config)
    actual = make_existing_table(expected)
    actual.schema = [bigquery.SchemaField("unexpected", "STRING")]

    with pytest.raises(ValueError, match="Schema 不一致"):
        bigquery_provisioning.validate_table(actual, expected)


def test_partition_drift_raises(config):
    expected = bigquery_schema.build_raw_table(config)
    actual = make_existing_table(expected)
    actual.time_partitioning = bigquery.TimePartitioning(type_="DAY", field="period_start_date")

    with pytest.raises(ValueError, match="Partitioning 不一致"):
        bigquery_provisioning.validate_table(actual, expected)


def test_dataset_default_partition_expiration_is_rejected(config):
    expected = bigquery_schema.build_raw_table(config)
    actual = make_existing_table(expected)
    actual.time_partitioning = bigquery.TimePartitioning(
        type_="MONTH", field="period_start_date", expiration_ms=5184000000
    )

    with pytest.raises(ValueError, match="Partitioning 不一致"):
        bigquery_provisioning.validate_table(actual, expected)


def test_removed_partition_expiration_is_accepted(config):
    expected = bigquery_schema.build_raw_table(config)
    actual = make_existing_table(expected)
    actual.time_partitioning = bigquery.TimePartitioning(type_="MONTH", field="period_start_date")

    bigquery_provisioning.validate_table(actual, expected)


def test_explicit_partition_expiration_drift_raises(config):
    expected = bigquery_schema.build_raw_table(config)
    expected.time_partitioning.expiration_ms = 86400000
    actual = make_existing_table(expected)
    actual.time_partitioning = bigquery.TimePartitioning(
        type_="MONTH", field="period_start_date", expiration_ms=5184000000
    )

    with pytest.raises(ValueError, match="Partitioning 不一致"):
        bigquery_provisioning.validate_table(actual, expected)


def test_clustering_drift_raises(config):
    expected = bigquery_schema.build_raw_table(config)
    actual = make_existing_table(expected)
    actual.clustering_fields = ["partner_code"]

    with pytest.raises(ValueError, match="Clustering 不一致"):
        bigquery_provisioning.validate_table(actual, expected)


def test_provision_tables_dry_run(config):
    client = mock.Mock()
    client.get_table.side_effect = exceptions.NotFound("Table not found")

    result = bigquery_provisioning.provision_tables(client, config, apply=False)

    assert result == {
        "comtrade_landing": "planned",
        "comtrade_records": "planned",
        "ingestion_runs": "planned",
    }
    client.create_table.assert_not_called()


def test_provision_tables_checks_existing_schema_before_create(config):
    client = mock.Mock()
    definitions = bigquery_schema.build_table_definitions(config)
    invalid = make_existing_table(definitions[1])
    invalid.schema = [bigquery.SchemaField("unexpected", "STRING")]

    def get_table(reference):
        if reference.table_id == "comtrade_records":
            return invalid
        raise exceptions.NotFound("Table not found")

    client.get_table.side_effect = get_table

    with pytest.raises(ValueError, match="Schema 不一致"):
        bigquery_provisioning.provision_tables(client, config, apply=True)

    client.create_table.assert_not_called()

def test_normalize_field_type():
    assert bigquery_provisioning.normalize_field_type("INT64") == "INTEGER"
    assert bigquery_provisioning.normalize_field_type("INTEGER") == "INTEGER"
    assert bigquery_provisioning.normalize_field_type("BOOL") == "BOOLEAN"
    assert bigquery_provisioning.normalize_field_type("BOOLEAN") == "BOOLEAN"
    assert bigquery_provisioning.normalize_field_type("BIGNUMERIC") == "BIGNUMERIC"


def test_schema_aliases_are_equivalent():
    expected = [
        bigquery.SchemaField("reporter_code", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("is_original_classification", "BOOL", mode="REQUIRED"),
    ]

    actual = [
        bigquery.SchemaField("reporter_code", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("is_original_classification", "BOOLEAN", mode="REQUIRED"),
    ]

    assert bigquery_provisioning.schema_signature(expected) == bigquery_provisioning.schema_signature(actual)


def test_schema_real_type_mismatch():
    expected = [bigquery.SchemaField("reporter_code", "INT64", mode="REQUIRED")]
    actual = [bigquery.SchemaField("reporter_code", "STRING", mode="REQUIRED")]

    assert bigquery_provisioning.schema_signature(expected) != bigquery_provisioning.schema_signature(actual)
