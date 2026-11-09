
"""CLI interfaces for Stage 9 Airflow orchestration."""

import argparse
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)

INGESTION_TIMEOUT_SECONDS = 1800
DBT_TIMEOUT_SECONDS = 3600

QUERY_TYPES = ("world_total", "partner_detail")


@dataclass(frozen=True)
class RunnerSettings:
    """Local execution paths and BigQuery project identity."""

    project_root: Path
    project_id: str
    etl_python: Path
    dbt_executable: Path
    dbt_project_dir: Path

    @classmethod
    def from_environment(cls) -> "RunnerSettings":
        root_value = os.getenv("TRADE_ANALYTICS_ROOT")

        if root_value:
            root = Path(root_value).expanduser().resolve()
        else:
            root = Path(__file__).resolve().parents[3]

        project_id = os.getenv("GCP_PROJECT_ID", "").strip()

        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID 環境變數不可為空"
            )

        return cls(
            project_root=root,
            project_id=project_id,
            etl_python=root / ".venv" / "bin" / "python",
            dbt_executable=root / ".venv-dbt" / "bin" / "dbt",
            dbt_project_dir=root / "dbt",
        )


def validate_settings(settings: RunnerSettings) -> None:
    """Validate the local project and executable paths."""

    required_paths = {
        "ETL Python": settings.etl_python,
        "dbt executable": settings.dbt_executable,
        "dbt project": (
            settings.dbt_project_dir / "dbt_project.yml"
        ),
        "Python source": settings.project_root / "src",
    }

    for name, path in required_paths.items():
        if not path.exists():
            raise FileNotFoundError(
                f"{name} 不存在：{path}"
            )

    for name, path in {
        "ETL Python": settings.etl_python,
        "dbt executable": settings.dbt_executable,
    }.items():
        if not path.is_file():
            raise ValueError(
                f"{name} 不是檔案：{path}"
            )

        if not os.access(path, os.X_OK):
            raise PermissionError(
                f"{name} 不可執行：{path}"
            )


def build_process_env(
    settings: RunnerSettings,
) -> dict[str, str]:
    """Build the child-process environment."""

    env = os.environ.copy()

    env["GCP_PROJECT_ID"] = settings.project_id

    src_path = str(settings.project_root / "src")
    existing_pythonpath = env.get("PYTHONPATH", "")

    if existing_pythonpath:
        env["PYTHONPATH"] = (
            src_path + os.pathsep + existing_pythonpath
        )
    else:
        env["PYTHONPATH"] = src_path

    # Stage 7 and Stage 8 already use these defaults.
    env.setdefault("BQ_LOCATION", "asia-northeast1")
    env.setdefault("BQ_RAW_DATASET", "trade_raw")
    env.setdefault(
        "BQ_ANALYTICS_DATASET",
        "trade_analytics",
    )

    return env


def validate_query_arguments(
    period: str,
    reporter: int,
    cmd: str,
    query_type: str,
) -> None:
    """Reject unsupported or malformed source-query arguments."""

    if not re.fullmatch(r"\d{6}", period):
        raise ValueError(
            "period 必須為 YYYYMM 格式"
        )

    try:
        parsed = datetime.strptime(period, "%Y%m")
    except ValueError as exc:
        raise ValueError(
            f"period 不是合法月份：{period}"
        ) from exc

    if parsed.strftime("%Y%m") != period:
        raise ValueError(
            f"period 不是合法月份：{period}"
        )

    if type(reporter) is not int or reporter <= 0:
        raise ValueError(
            "reporter 必須為正整數"
        )

    if not cmd or not re.fullmatch(r"[0-9A-Za-z]+", cmd):
        raise ValueError(
            "cmd 必須為非空商品代碼"
        )

    if query_type not in QUERY_TYPES:
        raise ValueError(
            f"不支援 query_type：{query_type}"
        )


def build_ingestion_command(
    settings: RunnerSettings,
    *,
    period: str,
    reporter: int,
    cmd: str,
    query_type: str,
    apply: bool = False,
    allow_bounded_preview: bool = False,
) -> list[str]:
    """Build the existing Stage 7 production-smoke CLI command."""

    validate_query_arguments(
        period,
        reporter,
        cmd,
        query_type,
    )

    if apply and not allow_bounded_preview:
        raise ValueError(
            "正式載入必須明確授權 BOUNDED_PREVIEW"
        )

    if allow_bounded_preview and not apply:
        raise ValueError(
            "--allow-bounded-preview 僅能搭配 --apply"
        )

    command = [
        str(settings.etl_python),
        "-m",
        "trade_analytics.warehouse.production_smoke",
        "--period",
        period,
        "--reporter",
        str(reporter),
        "--cmd",
        cmd,
        "--query-type",
        query_type,
    ]

    if apply:
        command.extend([
            "--apply",
            "--allow-bounded-preview",
        ])

    return command


def run_ingestion(
    settings: RunnerSettings,
    *,
    period: str,
    reporter: int,
    cmd: str,
    query_type: str,
    apply: bool = False,
    allow_bounded_preview: bool = False,
) -> None:
    """Execute the complete Stage 7 ingestion pipeline.

    This function does not automatically retry a failed load.
    Recovery must be handled through the existing Stage 7
    reconciliation and recovery procedures.
    """

    validate_settings(settings)

    command = build_ingestion_command(
        settings,
        period=period,
        reporter=reporter,
        cmd=cmd,
        query_type=query_type,
        apply=apply,
        allow_bounded_preview=allow_bounded_preview,
    )

    logger.info(
        "開始 Ingestion：period=%s reporter=%s "
        "cmd=%s query_type=%s apply=%s",
        period,
        reporter,
        cmd,
        query_type,
        apply,
    )

    subprocess.run(
        command,
        cwd=settings.project_root,
        env=build_process_env(settings),
        check=True,
        timeout=INGESTION_TIMEOUT_SECONDS,
    )

    logger.info(
        "Ingestion 執行成功：period=%s "
        "reporter=%s cmd=%s",
        period,
        reporter,
        cmd,
    )


def build_dbt_command(
    settings: RunnerSettings,
) -> list[str]:
    """Build a dbt command pinned to the dev target."""

    return [
        str(settings.dbt_executable),
        "build",
        "--target",
        "dev",
        "--project-dir",
        str(settings.dbt_project_dir),
    ]


def run_dbt_build(
    settings: RunnerSettings,
) -> None:
    """Build and test all dbt resources in the dev target."""

    validate_settings(settings)

    logger.info(
        "開始 dbt Build：target=dev project=%s",
        settings.dbt_project_dir,
    )

    subprocess.run(
        build_dbt_command(settings),
        cwd=settings.dbt_project_dir,
        env=build_process_env(settings),
        check=True,
        timeout=DBT_TIMEOUT_SECONDS,
    )

    logger.info(
        "dbt Build 執行成功：target=dev"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage 9 orchestration CLI"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser(
        "check",
        help="Verify local ETL and dbt executable paths",
    )

    ingestion_parser = subparsers.add_parser(
        "ingest",
        help="Run Stage 7 ingestion",
    )

    ingestion_parser.add_argument(
        "--period",
        required=True,
    )

    ingestion_parser.add_argument(
        "--reporter",
        type=int,
        required=True,
    )

    ingestion_parser.add_argument(
        "--cmd",
        required=True,
    )

    ingestion_parser.add_argument(
        "--query-type",
        required=True,
        choices=QUERY_TYPES,
    )

    ingestion_parser.add_argument(
        "--apply",
        action="store_true",
    )

    ingestion_parser.add_argument(
        "--allow-bounded-preview",
        action="store_true",
    )

    subparsers.add_parser(
        "dbt-build",
        help="Build and test the dbt dev project",
    )

    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s：%(message)s"
        ),
    )

    args = build_parser().parse_args()
    settings = RunnerSettings.from_environment()

    if args.command == "check":
        validate_settings(settings)

        logger.info(
            "執行環境確認成功：project=%s",
            settings.project_id,
        )

        logger.info(
            "ETL Python：%s",
            settings.etl_python,
        )

        logger.info(
            "dbt executable：%s",
            settings.dbt_executable,
        )

    elif args.command == "ingest":
        run_ingestion(
            settings,
            period=args.period,
            reporter=args.reporter,
            cmd=args.cmd,
            query_type=args.query_type,
            apply=args.apply,
            allow_bounded_preview=(
                args.allow_bounded_preview
            ),
        )

    elif args.command == "dbt-build":
        run_dbt_build(settings)


if __name__ == "__main__":
    main()
