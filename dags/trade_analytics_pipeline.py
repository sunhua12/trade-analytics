
"""Stage 9.3: Single-ingestion-task Airflow orchestration.

Pipeline:
    run_ingestion -> dbt_build

Safety:
    - Manual trigger only.
    - Explicit write authorization required.
    - No automatic ingestion retries.
    - No new batch identity is created by this DAG.
    - Raw recovery remains a manual Stage 7 procedure.
"""

import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task


logger = logging.getLogger(__name__)

DAG_ID = "trade_analytics_pipeline"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

AIRFLOW_PYTHON = (
    PROJECT_ROOT / ".venv-airflow" / "bin" / "python"
)

RUNNER_MODULE = "trade_analytics.orchestration.runner"

RUNNER_CHECK_TIMEOUT = 60
INGESTION_TASK_TIMEOUT = timedelta(minutes=35)
DBT_TASK_TIMEOUT = timedelta(minutes=65)


def build_runner_environment() -> dict[str, str]:
    """Create the environment inherited by the Runner subprocess."""
    env = os.environ.copy()

    project_id = env.get("GCP_PROJECT_ID", "").strip()

    if not project_id:
        raise RuntimeError(
            "缺少 GCP_PROJECT_ID。"
            "請在啟動 Airflow 前設定此環境變數。"
        )

    env["TRADE_ANALYTICS_ROOT"] = str(PROJECT_ROOT)

    source_path = str(PROJECT_ROOT / "src")
    existing_path = env.get("PYTHONPATH", "")

    env["PYTHONPATH"] = (
        source_path
        if not existing_path
        else source_path + os.pathsep + existing_path
    )

    env.setdefault("BQ_LOCATION", "asia-northeast1")
    env.setdefault("BQ_RAW_DATASET", "trade_raw")
    env.setdefault(
        "BQ_ANALYTICS_DATASET",
        "trade_analytics",
    )

    return env


def execute_runner(
    arguments: list[str],
    *,
    timeout_seconds: int,
) -> None:
    """Invoke the Stage 9.2 Runner in the Airflow environment."""
    if not AIRFLOW_PYTHON.is_file():
        raise FileNotFoundError(
            f"Airflow Python 不存在：{AIRFLOW_PYTHON}"
        )

    command = [
        str(AIRFLOW_PYTHON),
        "-m",
        RUNNER_MODULE,
        *arguments,
    ]

    logger.info(
        "執行 Stage 9.2 Runner：subcommand=%s",
        arguments[0],
    )

    # check=True:
    # An ingestion/dbt failure propagates to the Airflow task.
    #
    # No shell=True:
    # Arguments are passed without shell interpolation.
    #
    # stdout/stderr:
    # Inherit the task process streams for Airflow logging.
    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=build_runner_environment(),
        check=True,
        timeout=timeout_seconds,
    )


@dag(
    dag_id=DAG_ID,
    description=(
        "UN Comtrade ingestion followed by validated dbt build"
    ),
    schedule=None,
    start_date=datetime(
        2026, 11, 1, tzinfo=timezone.utc
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "trade-analytics",
        "retries": 0,
    },
    params={
        "period": Param(
            "202401",
            type="string",
            pattern=r"^[0-9]{6}$",
            description="UN Comtrade reporting month: YYYYMM",
        ),
        "reporter": Param(
            842,
            type="integer",
            minimum=1,
            description="UN Comtrade reporter code",
        ),
        "cmd": Param(
            "854231",
            type="string",
            pattern=r"^[0-9A-Za-z]+$",
            description="HS commodity code",
        ),
        "query_type": Param(
            "world_total",
            type="string",
            enum=[
                "world_total",
                "partner_detail",
            ],
            description="Source query type",
        ),
        "apply": Param(
            False,
            type="boolean",
            description=(
                "Explicitly authorize writes to BigQuery Raw"
            ),
        ),
        "allow_bounded_preview": Param(
            False,
            type="boolean",
            description=(
                "Accept the BOUNDED_PREVIEW source limitation"
            ),
        ),
    },
    tags=["trade-analytics", "stage-9", "elt"],
)
def trade_analytics_pipeline():
    """Orchestrate one ingestion run followed by a dbt build."""

    @task(
        task_id="run_ingestion",
        retries=0,
        execution_timeout=INGESTION_TASK_TIMEOUT,
    )
    def run_ingestion() -> None:
        """Execute the complete Stage 7 production ingestion."""
        params = get_current_context()["params"]

        # A dry run must not be treated as a successful
        # production pipeline execution.
        if params["apply"] is not True:
            raise ValueError(
                "正式 DAG 必須設定 apply=true。"
                "若只要測試來源，請使用 Runner CLI Dry Run。"
            )

        if params["allow_bounded_preview"] is not True:
            raise ValueError(
                "正式 DAG 必須設定 "
                "allow_bounded_preview=true。"
            )

        period = str(params["period"])
        reporter = int(params["reporter"])
        cmd = str(params["cmd"])
        query_type = str(params["query_type"])

        logger.info(
            "開始正式 Ingestion："
            "period=%s reporter=%d cmd=%s type=%s",
            period,
            reporter,
            cmd,
            query_type,
        )

        execute_runner(
            [
                "ingest",
                "--period",
                period,
                "--reporter",
                str(reporter),
                "--cmd",
                cmd,
                "--query-type",
                query_type,
                "--apply",
                "--allow-bounded-preview",
            ],
            timeout_seconds=1800,
        )

        logger.info(
            "Ingestion 已成功完成，允許下游 dbt Build"
        )

    @task(
        task_id="dbt_build",
        retries=0,
        execution_timeout=DBT_TASK_TIMEOUT,
    )
    def dbt_build() -> None:
        """Build and test the dbt development project."""
        logger.info(
            "開始 dbt Build：target=dev"
        )

        execute_runner(
            ["dbt-build"],
            timeout_seconds=3600,
        )

        logger.info(
            "dbt Build 成功，Pipeline 完成"
        )

    ingestion = run_ingestion()
    build = dbt_build()

    ingestion >> build


trade_analytics_pipeline()
