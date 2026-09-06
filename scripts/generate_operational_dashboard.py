from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from commerce_pipeline.dashboard import (
    build_dashboard_model,
    render_dashboard_html,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RELIABILITY_PREFIX = (
    "RELIABILITY_PROFILE_RESULT="
)
DEPLOYMENT_PREFIX = (
    "DEPLOYMENT_VALIDATION_RESULT="
)


def load_json_file(
    path: Path,
) -> dict[str, Any]:
    result = json.loads(
        path.read_text(encoding="utf-8")
    )

    if not isinstance(result, dict):
        raise RuntimeError(
            f"JSON input was not an object: {path}"
        )

    return result


def run_result_command(
    arguments: list[str],
    *,
    result_prefix: str,
) -> dict[str, Any]:
    completed = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=360,
        check=False,
    )

    combined_output = (
        completed.stdout
        + "\n"
        + completed.stderr
    )

    if completed.returncode != 0:
        raise RuntimeError(
            "Dashboard input command failed: "
            + " ".join(arguments)
            + "\n"
            + combined_output[-5000:]
        )

    for line in completed.stdout.splitlines():
        prefix_position = line.find(
            result_prefix
        )

        if prefix_position == -1:
            continue

        payload = line[
            prefix_position
            + len(result_prefix):
        ]
        result = json.loads(payload)

        if not isinstance(result, dict):
            raise RuntimeError(
                "Dashboard input result was not "
                "a JSON object"
            )

        return result

    raise RuntimeError(
        f"Result prefix was not found: "
        f"{result_prefix}\n"
        + combined_output[-5000:]
    )


def resolve_input(
    optional_path: str | None,
    *,
    command: list[str],
    result_prefix: str,
) -> dict[str, Any]:
    if optional_path is None:
        return run_result_command(
            command,
            result_prefix=result_prefix,
        )

    path = Path(optional_path)

    if not path.is_absolute():
        path = PROJECT_ROOT / path

    return load_json_file(path)


def project_relative(path: Path) -> str:
    try:
        return str(
            path.relative_to(PROJECT_ROOT)
        )
    except ValueError:
        return str(path)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a self-contained operational "
            "dashboard from reliability and deployment "
            "validation results."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/dashboard",
        help=(
            "Directory for the generated HTML and JSON "
            "dashboard files."
        ),
    )
    parser.add_argument(
        "--reliability-json",
        help=(
            "Optional existing reliability-result JSON "
            "file. The live profiler runs when omitted."
        ),
    )
    parser.add_argument(
        "--deployment-json",
        help=(
            "Optional existing deployment-result JSON "
            "file. Live validation runs when omitted."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    reliability = resolve_input(
        arguments.reliability_json,
        command=[
            sys.executable,
            "scripts/profile_reliability.py",
        ],
        result_prefix=RELIABILITY_PREFIX,
    )

    deployment = resolve_input(
        arguments.deployment_json,
        command=[
            sys.executable,
            "scripts/validate_deployment.py",
            "--mode",
            "live",
        ],
        result_prefix=DEPLOYMENT_PREFIX,
    )

    generated_at = datetime.now(
        timezone.utc
    ).isoformat()

    model = build_dashboard_model(
        reliability,
        deployment,
        generated_at=generated_at,
    )
    rendered_html = render_dashboard_html(
        model
    )

    output_directory = Path(
        arguments.output_dir
    )

    if not output_directory.is_absolute():
        output_directory = (
            PROJECT_ROOT / output_directory
        )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    json_path = (
        output_directory
        / "operational_dashboard.json"
    )
    html_path = (
        output_directory
        / "operational_dashboard.html"
    )

    json_path.write_text(
        json.dumps(
            model,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    html_path.write_text(
        rendered_html,
        encoding="utf-8",
    )

    result = {
        "status": (
            "valid"
            if model["healthy"]
            else "failed"
        ),
        "dashboard_status": model["status"],
        "generated_at": generated_at,
        "html_path": project_relative(
            html_path
        ),
        "json_path": project_relative(
            json_path
        ),
        "card_count": len(
            model["cards"]
        ),
        "service_count": len(
            model["services"]
        ),
        "reconciliation_table_count": len(
            model["reconciliation"]
        ),
        "failed_check_count": len(
            model["failed_checks"]
        ),
    }

    print(
        "DASHBOARD_BUILD_RESULT="
        + json.dumps(
            result,
            sort_keys=True,
        )
    )

    if result["status"] != "valid":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
