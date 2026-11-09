
from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="airflow_environment_check",
    description="Verify local Airflow DAG discovery and execution.",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["setup", "stage-9"],
)
def airflow_environment_check():

    @task
    def verify_environment():
        import logging

        logger = logging.getLogger(__name__)

        logger.info("Airflow environment is working.")

        return "OK"

    verify_environment()


airflow_environment_check()
