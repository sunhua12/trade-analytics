
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from trade_analytics.orchestration import runner


@pytest.fixture
def settings(tmp_path: Path) -> runner.RunnerSettings:
    """Create fake executable paths without installing Airflow."""

    etl_python = tmp_path / ".venv" / "bin" / "python"
    dbt_executable = (
        tmp_path / ".venv-dbt" / "bin" / "dbt"
    )

    etl_python.parent.mkdir(parents=True)
    dbt_executable.parent.mkdir(parents=True)

    etl_python.write_text("#!/bin/sh\nexit 0\n")
    dbt_executable.write_text("#!/bin/sh\nexit 0\n")

    etl_python.chmod(0o755)
    dbt_executable.chmod(0o755)

    dbt_project_dir = tmp_path / "dbt"
    dbt_project_dir.mkdir()

    (dbt_project_dir / "dbt_project.yml").write_text(
        "name: trade_analytics\n"
    )

    (tmp_path / "src").mkdir()

    return runner.RunnerSettings(
        project_root=tmp_path,
        project_id="test-gcp-project",
        etl_python=etl_python,
        dbt_executable=dbt_executable,
        dbt_project_dir=dbt_project_dir,
    )


def test_validate_settings(settings):
    runner.validate_settings(settings)


def test_missing_executable_rejected(settings):
    settings.etl_python.unlink()

    with pytest.raises(
        FileNotFoundError,
        match="ETL Python",
    ):
        runner.validate_settings(settings)


def test_ingestion_defaults_to_dry_run(settings):
    command = runner.build_ingestion_command(
        settings,
        period="202401",
        reporter=842,
        cmd="854231",
        query_type="world_total",
    )

    assert command[:3] == [
        str(settings.etl_python),
        "-m",
        "trade_analytics.warehouse.production_smoke",
    ]

    assert "--apply" not in command
    assert "--allow-bounded-preview" not in command


def test_apply_requires_explicit_preview_approval(settings):
    with pytest.raises(
        ValueError,
        match="明確授權",
    ):
        runner.build_ingestion_command(
            settings,
            period="202401",
            reporter=842,
            cmd="854231",
            query_type="world_total",
            apply=True,
            allow_bounded_preview=False,
        )


def test_apply_builds_expected_command(settings):
    command = runner.build_ingestion_command(
        settings,
        period="202401",
        reporter=842,
        cmd="854231",
        query_type="world_total",
        apply=True,
        allow_bounded_preview=True,
    )

    assert command[-2:] == [
        "--apply",
        "--allow-bounded-preview",
    ]


@pytest.mark.parametrize(
    "period",
    ["202400", "202413", "20241", "invalid"],
)
def test_invalid_period_rejected(settings, period):
    with pytest.raises(ValueError):
        runner.build_ingestion_command(
            settings,
            period=period,
            reporter=842,
            cmd="854231",
            query_type="world_total",
        )


def test_run_ingestion_invokes_existing_cli(settings):
    with patch.object(
        runner.subprocess,
        "run",
    ) as mock_run:
        runner.run_ingestion(
            settings,
            period="202401",
            reporter=842,
            cmd="854231",
            query_type="world_total",
            apply=True,
            allow_bounded_preview=True,
        )

    mock_run.assert_called_once()

    args, kwargs = mock_run.call_args

    assert args[0][0] == str(settings.etl_python)
    assert kwargs["check"] is True
    assert kwargs["shell"] is not True if (
        "shell" in kwargs
    ) else True

    assert kwargs["cwd"] == settings.project_root
    assert kwargs["env"]["GCP_PROJECT_ID"] == (
        "test-gcp-project"
    )


def test_ingestion_failure_is_propagated(settings):
    with patch.object(
        runner.subprocess,
        "run",
        side_effect=subprocess.CalledProcessError(
            returncode=1,
            cmd=["python", "-m", "production_smoke"],
        ),
    ):
        with pytest.raises(
            subprocess.CalledProcessError
        ):
            runner.run_ingestion(
                settings,
                period="202401",
                reporter=842,
                cmd="854231",
                query_type="world_total",
            )


def test_dbt_build_uses_dev_target(settings):
    command = runner.build_dbt_command(settings)

    assert command == [
        str(settings.dbt_executable),
        "build",
        "--target",
        "dev",
        "--project-dir",
        str(settings.dbt_project_dir),
    ]


def test_dbt_build_failure_is_propagated(settings):
    with patch.object(
        runner.subprocess,
        "run",
        side_effect=subprocess.CalledProcessError(
            returncode=2,
            cmd=["dbt", "build"],
        ),
    ):
        with pytest.raises(
            subprocess.CalledProcessError
        ):
            runner.run_dbt_build(settings)
