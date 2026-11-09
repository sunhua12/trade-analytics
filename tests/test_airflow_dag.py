"""Stage 9.4: Airflow DAG structure and safety tests.

Test coverage:
- DAG discovery and configuration
- Task dependencies
- Manual scheduling and retry policies
- Execution environment
- Ingestion authorization
- Runner command arguments
- Failure propagation

All subprocess and task execution tests are mocked.
No real API requests, BigQuery writes, or dbt builds occur.
"""

import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from airflow.dag_processing.dagbag import DagBag

from dags import trade_analytics_pipeline as pipeline_module

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"
DAG_FILE = DAGS_DIR / "trade_analytics_pipeline.py"
DAG_ID = "trade_analytics_pipeline"

CONTEXT_PATCH_TARGET = (
    "dags.trade_analytics_pipeline.get_current_context"
)


# ============================================================
# FIXTURES AND HELPERS
# ============================================================


@pytest.fixture(scope="module")
def pipeline_dag():
    """Load the project DAG using Airflow 3.x DagBag."""

    bag = DagBag(
        dag_folder=str(DAGS_DIR),
        safe_mode=False,
    )

    assert bag.import_errors == {}, bag.import_errors

    dag = bag.get_dag(DAG_ID)

    assert dag is not None, (
        f"DAG not discovered: {DAG_ID}"
    )

    return dag


@pytest.fixture
def valid_params() -> dict[str, Any]:
    """Provide authorized synthetic ingestion parameters."""

    return {
        "period": "202402",
        "reporter": 842,
        "cmd": "854231",
        "query_type": "partner_detail",
        "apply": True,
        "allow_bounded_preview": True,
    }


def get_task_callable(
        task_id: str,
) -> Callable[..., Any]:
    """Retrieve a TaskFlow operator's Python callable.

    This does not create a DagRun or execute an Airflow task.
    """

    dag = pipeline_module.trade_analytics_pipeline()

    operator = dag.get_task(task_id)

    python_callable = getattr(
        operator,
        "python_callable",
    )

    assert callable(python_callable), (
        f"Task does not expose a callable: {task_id}"
    )

    return cast(
        Callable[..., Any],
        python_callable,
    )


# ============================================================
# 1. DAG STRUCTURE
# ============================================================


def test_dag_file_exists():
    """The production DAG file must exist."""

    assert DAG_FILE.is_file()


def test_dag_id(pipeline_dag):
    """Airflow must discover the expected DAG."""

    assert pipeline_dag.dag_id == DAG_ID


def test_dag_contains_exactly_two_tasks(pipeline_dag):
    """The MVP must use one ingestion task and one dbt task."""

    assert set(pipeline_dag.task_ids) == {
        "run_ingestion",
        "dbt_build",
    }


def test_task_dependencies(pipeline_dag):
    """dbt must depend on successful ingestion."""

    ingestion = pipeline_dag.get_task(
        "run_ingestion"
    )

    dbt = pipeline_dag.get_task(
        "dbt_build"
    )

    assert ingestion.upstream_task_ids == set()

    assert ingestion.downstream_task_ids == {
        "dbt_build"
    }

    assert dbt.upstream_task_ids == {
        "run_ingestion"
    }

    assert dbt.downstream_task_ids == set()


def test_manual_schedule(pipeline_dag):
    """The MVP must not run on an automatic schedule."""

    assert pipeline_dag.schedule is None

    assert pipeline_dag.catchup is False


def test_max_active_runs(pipeline_dag):
    """The DAG must prevent overlapping DAG runs."""

    assert pipeline_dag.max_active_runs == 1


def test_no_automatic_retries(pipeline_dag):
    """Neither task may retry automatically in the MVP."""

    for task_id in (
            "run_ingestion",
            "dbt_build",
    ):
        task = pipeline_dag.get_task(task_id)

        assert task.retries == 0, (
            f"Unexpected retries for {task_id}"
        )


def test_dbt_requires_upstream_success(pipeline_dag):
    """dbt must use the all_success trigger rule."""

    task = pipeline_dag.get_task(
        "dbt_build"
    )

    trigger_rule = getattr(
        task.trigger_rule,
        "value",
        task.trigger_rule,
    )

    assert trigger_rule == "all_success"


# ============================================================
# 2. EXECUTION ENVIRONMENT
# ============================================================


def test_missing_project_id_rejected(monkeypatch):
    """Runner must reject a missing GCP project ID."""

    monkeypatch.delenv(
        "GCP_PROJECT_ID",
        raising=False,
    )

    with pytest.raises(
            RuntimeError,
            match="GCP_PROJECT_ID",
    ):
        pipeline_module.build_runner_environment()


def test_runner_environment(monkeypatch):
    """Runner must receive expected project paths."""

    monkeypatch.setenv(
        "GCP_PROJECT_ID",
        "test-gcp-project",
    )

    env = pipeline_module.build_runner_environment()

    assert env["GCP_PROJECT_ID"] == (
        "test-gcp-project"
    )

    assert env["TRADE_ANALYTICS_ROOT"] == str(
        PROJECT_ROOT
    )

    python_paths = env["PYTHONPATH"].split(
        os.pathsep
    )

    assert str(PROJECT_ROOT / "src") in (
        python_paths
    )


# ============================================================
# 3. INGESTION AUTHORIZATION
# ============================================================


def test_ingestion_rejects_apply_false(valid_params):
    """A dry run must not be accepted by the production DAG."""

    params = {
        **valid_params,
        "apply": False,
    }

    with (
        patch(
            CONTEXT_PATCH_TARGET,
            return_value={"params": params},
        ),
        patch.object(
            pipeline_module,
            "execute_runner",
        ) as mock_runner,
    ):
        task_callable = get_task_callable(
            "run_ingestion"
        )

        with pytest.raises(
                ValueError,
                match="apply=true",
        ):
            task_callable()

        mock_runner.assert_not_called()


def test_ingestion_rejects_preview_false(
        valid_params,
):
    """BOUNDED_PREVIEW must require explicit approval."""

    params = {
        **valid_params,
        "allow_bounded_preview": False,
    }

    with (
        patch(
            CONTEXT_PATCH_TARGET,
            return_value={"params": params},
        ),
        patch.object(
            pipeline_module,
            "execute_runner",
        ) as mock_runner,
    ):
        task_callable = get_task_callable(
            "run_ingestion"
        )

        with pytest.raises(
                ValueError,
                match="allow_bounded_preview=true",
        ):
            task_callable()

        mock_runner.assert_not_called()


# ============================================================
# 4. RUNNER COMMAND ARGUMENTS
# ============================================================


def test_ingestion_command_arguments(valid_params):
    """Authorized parameters must reach the Stage 9.2 Runner."""

    with (
        patch(
            CONTEXT_PATCH_TARGET,
            return_value={"params": valid_params},
        ),
        patch.object(
            pipeline_module,
            "execute_runner",
        ) as mock_runner,
    ):
        task_callable = get_task_callable(
            "run_ingestion"
        )

        task_callable()

    mock_runner.assert_called_once_with(
        [
            "ingest",
            "--period",
            "202402",
            "--reporter",
            "842",
            "--cmd",
            "854231",
            "--query-type",
            "partner_detail",
            "--apply",
            "--allow-bounded-preview",
        ],
        timeout_seconds=1800,
    )


def test_dbt_command_arguments():
    """The dbt task must invoke the dbt-build Runner command."""

    with patch.object(
            pipeline_module,
            "execute_runner",
    ) as mock_runner:
        task_callable = get_task_callable(
            "dbt_build"
        )

        task_callable()

    mock_runner.assert_called_once_with(
        ["dbt-build"],
        timeout_seconds=3600,
    )


# ============================================================
# 5. FAILURE PROPAGATION
# ============================================================


def test_ingestion_failure_propagates(valid_params):
    """An ingestion failure must not be reported as success."""

    error = subprocess.CalledProcessError(
        returncode=1,
        cmd=[
            "python",
            "-m",
            "trade_analytics.warehouse.production_smoke",
        ],
    )

    with (
        patch(
            CONTEXT_PATCH_TARGET,
            return_value={"params": valid_params},
        ),
        patch.object(
            pipeline_module,
            "execute_runner",
            side_effect=error,
        ),
    ):
        task_callable = get_task_callable(
            "run_ingestion"
        )

        with pytest.raises(
                subprocess.CalledProcessError,
        ):
            task_callable()


def test_dbt_failure_propagates():
    """A dbt failure must not be reported as success."""

    error = subprocess.CalledProcessError(
        returncode=2,
        cmd=[
            "dbt",
            "build",
        ],
    )

    with patch.object(
            pipeline_module,
            "execute_runner",
            side_effect=error,
    ):
        task_callable = get_task_callable(
            "dbt_build"
        )

        with pytest.raises(
                subprocess.CalledProcessError,
        ):
            task_callable()


def test_subprocess_failure_propagates(
        monkeypatch,
        tmp_path,
):
    """execute_runner must propagate subprocess failures."""

    monkeypatch.setenv(
        "GCP_PROJECT_ID",
        "test-gcp-project",
    )

    fake_python = tmp_path / "python"
    fake_python.touch()

    error = subprocess.CalledProcessError(
        returncode=1,
        cmd=[
            "python",
            "-m",
            "trade_analytics.orchestration.runner",
        ],
    )

    with (
        patch.object(
            pipeline_module,
            "AIRFLOW_PYTHON",
            fake_python,
        ),
        patch.object(
            pipeline_module.subprocess,
            "run",
            side_effect=error,
        ) as mock_run,
    ):
        with pytest.raises(
                subprocess.CalledProcessError,
        ):
            pipeline_module.execute_runner(
                ["dbt-build"],
                timeout_seconds=60,
            )

    mock_run.assert_called_once()

    args, kwargs = mock_run.call_args

    command = args[0]

    assert command[0] == str(fake_python)

    assert command[1:3] == [
        "-m",
        pipeline_module.RUNNER_MODULE,
    ]

    assert command[-1] == "dbt-build"

    assert kwargs["check"] is True

    assert kwargs["timeout"] == 60

    assert kwargs.get(
        "shell",
        False,
    ) is False

    assert kwargs["cwd"] == (
        PROJECT_ROOT
    )

    assert kwargs["env"]["GCP_PROJECT_ID"] == (
        "test-gcp-project"
    )
