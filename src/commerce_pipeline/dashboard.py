from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Any


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value

    return {}


def _integer(value: Any) -> int:
    if value is None:
        return 0

    return int(value)


def _number(value: Any) -> float:
    if value is None:
        return 0.0

    return float(value)


def _check_summary(
    checks: dict[str, Any],
) -> dict[str, Any]:
    normalized = {
        str(name): value is True
        for name, value in checks.items()
    }
    passed = sum(normalized.values())
    total = len(normalized)

    return {
        "passed": passed,
        "total": total,
        "all_passed": (
            total > 0
            and passed == total
        ),
        "checks": normalized,
    }


def _format_integer(value: Any) -> str:
    return f"{_integer(value):,}"


def _format_seconds(value: Any) -> str:
    return f"{_number(value):,.3f} seconds"


def _format_milliseconds(value: Any) -> str:
    return f"{_number(value):,.3f} ms"


def build_dashboard_model(
    reliability: dict[str, Any],
    deployment: dict[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(reliability, dict):
        raise TypeError(
            "reliability result must be a dictionary"
        )

    if not isinstance(deployment, dict):
        raise TypeError(
            "deployment result must be a dictionary"
        )

    if generated_at is None:
        generated_at = datetime.now(
            timezone.utc
        ).isoformat()

    reliability_checks = _check_summary(
        _mapping(reliability.get("checks"))
    )
    deployment_checks = _check_summary(
        _mapping(deployment.get("checks"))
    )

    reconciliation = _mapping(
        reliability.get("event_reconciliation")
    )
    warehouse_quality = _mapping(
        reliability.get("warehouse_quality")
    )
    bronze_storage = _mapping(
        reliability.get("bronze_storage")
    )
    freshness = _mapping(
        reliability.get("freshness")
    )
    latency = _mapping(
        reliability.get("latency_ms")
    )
    current_counts = _mapping(
        reliability.get("current_state_counts")
    )
    source_counts = _mapping(
        current_counts.get("source")
    )
    target_counts = _mapping(
        current_counts.get(
            "bronze_derived_warehouse_state"
        )
    )

    live_deployment = _mapping(
        deployment.get("live")
    )
    containers = _mapping(
        live_deployment.get("containers")
    )
    dag = _mapping(
        live_deployment.get("dag")
    )

    bronze_events = _integer(
        reconciliation.get(
            "live_bronze_event_count"
        )
    )
    warehouse_events = _integer(
        reconciliation.get(
            "warehouse_event_count"
        )
    )
    event_difference = _integer(
        reconciliation.get("difference")
    )
    duplicate_events = _integer(
        warehouse_quality.get(
            "duplicate_event_ids"
        )
    )
    quarantine_records = _integer(
        bronze_storage.get(
            "quarantine_records"
        )
    )
    warehouse_age = _number(
        freshness.get(
            "warehouse_data_age_seconds"
        )
    )
    freshness_slo = _number(
        reliability.get(
            "freshness_slo_seconds"
        )
    )

    overall_valid = (
        reliability.get("status") == "valid"
        and deployment.get("status") == "valid"
        and reliability_checks["all_passed"]
        and deployment_checks["all_passed"]
    )

    cards = [
        {
            "label": "Operational checks",
            "value": (
                f"{reliability_checks['passed']}/"
                f"{reliability_checks['total']}"
            ),
            "healthy": (
                reliability_checks["all_passed"]
            ),
            "detail": "Live reliability validations",
        },
        {
            "label": "Deployment checks",
            "value": (
                f"{deployment_checks['passed']}/"
                f"{deployment_checks['total']}"
            ),
            "healthy": (
                deployment_checks["all_passed"]
            ),
            "detail": "Configuration and live health",
        },
        {
            "label": "Bronze events",
            "value": _format_integer(
                bronze_events
            ),
            "healthy": bronze_events > 0,
            "detail": "Live Parquet event count",
        },
        {
            "label": "Warehouse events",
            "value": _format_integer(
                warehouse_events
            ),
            "healthy": (
                warehouse_events > 0
                and warehouse_events
                == bronze_events
            ),
            "detail": "Raw warehouse event count",
        },
        {
            "label": "Event difference",
            "value": _format_integer(
                event_difference
            ),
            "healthy": event_difference == 0,
            "detail": "Bronze minus warehouse",
        },
        {
            "label": "Duplicate event IDs",
            "value": _format_integer(
                duplicate_events
            ),
            "healthy": duplicate_events == 0,
            "detail": "Raw warehouse duplicates",
        },
        {
            "label": "Warehouse data age",
            "value": _format_seconds(
                warehouse_age
            ),
            "healthy": (
                freshness_slo > 0
                and warehouse_age
                <= freshness_slo
            ),
            "detail": (
                "SLO: "
                + _format_seconds(
                    freshness_slo
                )
            ),
        },
        {
            "label": "Quarantined records",
            "value": _format_integer(
                quarantine_records
            ),
            "healthy": (
                bronze_storage.get("status")
                == "valid"
            ),
            "detail": "Records retain failure reasons",
        },
    ]

    service_rows = []

    for service_name in sorted(containers):
        assessment = _mapping(
            containers[service_name]
        )
        service_rows.append(
            {
                "service": str(service_name),
                "status": str(
                    assessment.get(
                        "status",
                        "unknown",
                    )
                ),
                "healthy": (
                    assessment.get("healthy")
                    is True
                ),
            }
        )

    reconciliation_rows = []

    for table_name in sorted(
        set(source_counts)
        | set(target_counts)
    ):
        source_count = _integer(
            source_counts.get(table_name)
        )
        target_count = _integer(
            target_counts.get(table_name)
        )

        reconciliation_rows.append(
            {
                "table": str(table_name),
                "source_count": source_count,
                "target_count": target_count,
                "difference": (
                    target_count - source_count
                ),
                "reconciled": (
                    source_count == target_count
                ),
            }
        )

    latency_rows = [
        {
            "stage": "Debezium connector",
            "average": _format_milliseconds(
                latency.get(
                    "connector_average"
                )
            ),
            "maximum": _format_milliseconds(
                latency.get(
                    "connector_maximum"
                )
            ),
        },
        {
            "stage": "Bronze processing",
            "average": _format_milliseconds(
                latency.get(
                    "bronze_processing_average"
                )
            ),
            "maximum": _format_milliseconds(
                latency.get(
                    "bronze_processing_maximum"
                )
            ),
        },
        {
            "stage": "Warehouse load",
            "average": _format_milliseconds(
                latency.get(
                    "warehouse_load_average"
                )
            ),
            "maximum": _format_milliseconds(
                latency.get(
                    "warehouse_load_maximum"
                )
            ),
        },
        {
            "stage": "End to end",
            "average": _format_milliseconds(
                latency.get(
                    "end_to_end_average"
                )
            ),
            "maximum": _format_milliseconds(
                latency.get(
                    "end_to_end_maximum"
                )
            ),
        },
    ]

    failed_checks = [
        {
            "group": "Reliability",
            "check": check_name,
        }
        for check_name, passed
        in reliability_checks["checks"].items()
        if not passed
    ]

    failed_checks.extend(
        {
            "group": "Deployment",
            "check": check_name,
        }
        for check_name, passed
        in deployment_checks["checks"].items()
        if not passed
    )

    quarantine_reasons = {
        str(reason): _integer(count)
        for reason, count
        in _mapping(
            bronze_storage.get(
                "quarantine_reason_counts"
            )
        ).items()
    }

    return {
        "title": (
            "Commerce Platform Operational Dashboard"
        ),
        "generated_at": generated_at,
        "status": (
            "healthy"
            if overall_valid
            else "attention_required"
        ),
        "healthy": overall_valid,
        "cards": cards,
        "services": service_rows,
        "reconciliation": (
            reconciliation_rows
        ),
        "latency": latency_rows,
        "failed_checks": failed_checks,
        "quarantine_reasons": (
            quarantine_reasons
        ),
        "dag": {
            "dag_id": str(
                dag.get("dag_id", "unknown")
            ),
            "task_count": _integer(
                dag.get("task_count")
            ),
            "import_errors": _integer(
                dag.get("import_errors")
            ),
        },
        "freshness": {
            "warehouse_data_age_seconds": (
                warehouse_age
            ),
            "pipeline_success_age_seconds": (
                _number(
                    freshness.get(
                        "pipeline_success_age_seconds"
                    )
                )
            ),
            "slo_seconds": freshness_slo,
        },
        "measured_events": _integer(
            latency.get("measured_events")
        ),
    }


def render_dashboard_html(
    model: dict[str, Any],
) -> str:
    title = escape(str(model["title"]))
    generated_at = escape(
        str(model["generated_at"])
    )
    status = escape(str(model["status"]))
    overall_class = (
        "healthy"
        if model.get("healthy")
        else "failed"
    )

    card_html = "\n".join(
        (
            '<article class="card">'
            f'<div class="indicator '
            f'{"healthy" if card["healthy"] else "failed"}">'
            "</div>"
            f'<p class="card-label">'
            f'{escape(str(card["label"]))}</p>'
            f'<p class="card-value">'
            f'{escape(str(card["value"]))}</p>'
            f'<p class="card-detail">'
            f'{escape(str(card["detail"]))}</p>'
            "</article>"
        )
        for card in model["cards"]
    )

    service_html = "\n".join(
        (
            "<tr>"
            f'<td>{escape(str(row["service"]))}</td>'
            f'<td><span class="badge '
            f'{"healthy" if row["healthy"] else "failed"}">'
            f'{escape(str(row["status"]))}'
            "</span></td>"
            "</tr>"
        )
        for row in model["services"]
    )

    reconciliation_html = "\n".join(
        (
            "<tr>"
            f'<td>{escape(str(row["table"]))}</td>'
            f'<td>{row["source_count"]:,}</td>'
            f'<td>{row["target_count"]:,}</td>'
            f'<td>{row["difference"]:,}</td>'
            f'<td><span class="badge '
            f'{"healthy" if row["reconciled"] else "failed"}">'
            f'{"Reconciled" if row["reconciled"] else "Difference"}'
            "</span></td>"
            "</tr>"
        )
        for row in model["reconciliation"]
    )

    latency_html = "\n".join(
        (
            "<tr>"
            f'<td>{escape(str(row["stage"]))}</td>'
            f'<td>{escape(str(row["average"]))}</td>'
            f'<td>{escape(str(row["maximum"]))}</td>'
            "</tr>"
        )
        for row in model["latency"]
    )

    if model["failed_checks"]:
        alert_html = "\n".join(
            (
                '<li class="failure-item">'
                f'{escape(str(item["group"]))}: '
                f'{escape(str(item["check"]))}'
                "</li>"
            )
            for item in model["failed_checks"]
        )
    else:
        alert_html = (
            '<li class="success-item">'
            "No active validation failures."
            "</li>"
        )

    reason_html = "\n".join(
        (
            "<tr>"
            f"<td>{escape(str(reason))}</td>"
            f"<td>{count:,}</td>"
            "</tr>"
        )
        for reason, count
        in sorted(
            model["quarantine_reasons"].items()
        )
    )

    if not reason_html:
        reason_html = (
            '<tr><td colspan="2">'
            "No quarantine reasons recorded."
            "</td></tr>"
        )

    dag = model["dag"]

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport"
        content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: dark;
      --background: #0b1120;
      --surface: #111a2e;
      --surface-alt: #17233b;
      --border: #293752;
      --text: #eef4ff;
      --muted: #9badc8;
      --healthy: #2dd4a3;
      --healthy-bg: #103d38;
      --failed: #ff6b7a;
      --failed-bg: #4a1f2a;
      --accent: #65a5ff;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      background:
        radial-gradient(circle at top right,
          #16264a 0, transparent 32rem),
        var(--background);
      color: var(--text);
      font-family:
        Inter, ui-sans-serif, system-ui,
        -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
      line-height: 1.5;
    }}

    main {{
      width: min(1200px, calc(100% - 2rem));
      margin: 0 auto;
      padding: 2rem 0 4rem;
    }}

    header {{
      display: flex;
      justify-content: space-between;
      gap: 2rem;
      align-items: flex-start;
      margin-bottom: 2rem;
    }}

    h1 {{
      margin: 0 0 0.4rem;
      font-size: clamp(1.7rem, 4vw, 2.8rem);
    }}

    h2 {{
      margin-top: 0;
      font-size: 1.2rem;
    }}

    .subtitle,
    .note {{
      color: var(--muted);
    }}

    .status {{
      white-space: nowrap;
      border-radius: 999px;
      padding: 0.55rem 0.85rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-size: 0.75rem;
    }}

    .status.healthy,
    .badge.healthy {{
      color: var(--healthy);
      background: var(--healthy-bg);
    }}

    .status.failed,
    .badge.failed {{
      color: var(--failed);
      background: var(--failed-bg);
    }}

    .cards {{
      display: grid;
      grid-template-columns:
        repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 1rem;
    }}

    .card,
    .panel {{
      border: 1px solid var(--border);
      background: linear-gradient(
        145deg,
        var(--surface),
        var(--surface-alt)
      );
      border-radius: 0.9rem;
      box-shadow: 0 18px 45px #0004;
    }}

    .card {{
      position: relative;
      padding: 1.1rem;
      overflow: hidden;
    }}

    .indicator {{
      position: absolute;
      inset: 0 auto 0 0;
      width: 0.3rem;
    }}

    .indicator.healthy {{
      background: var(--healthy);
    }}

    .indicator.failed {{
      background: var(--failed);
    }}

    .card-label,
    .card-detail {{
      margin: 0;
      color: var(--muted);
    }}

    .card-value {{
      margin: 0.35rem 0;
      font-size: 1.6rem;
      font-weight: 750;
    }}

    .card-detail {{
      font-size: 0.82rem;
    }}

    .grid {{
      display: grid;
      grid-template-columns:
        repeat(auto-fit, minmax(340px, 1fr));
      gap: 1rem;
      margin-top: 1rem;
    }}

    .panel {{
      padding: 1.1rem;
      overflow-x: auto;
    }}

    .wide {{
      grid-column: 1 / -1;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
    }}

    th,
    td {{
      padding: 0.7rem 0.55rem;
      border-bottom: 1px solid var(--border);
      text-align: left;
      white-space: nowrap;
    }}

    th {{
      color: var(--muted);
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}

    .badge {{
      display: inline-block;
      border-radius: 999px;
      padding: 0.2rem 0.55rem;
      font-size: 0.78rem;
      font-weight: 700;
    }}

    .failure-item {{
      color: var(--failed);
    }}

    .success-item {{
      color: var(--healthy);
    }}

    footer {{
      color: var(--muted);
      margin-top: 1.5rem;
      font-size: 0.8rem;
    }}

    @media (max-width: 650px) {{
      header {{
        display: block;
      }}

      .status {{
        display: inline-block;
        margin-top: 1rem;
      }}

      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>{title}</h1>
        <div class="subtitle">
          Generated at {generated_at}
        </div>
      </div>
      <div class="status {overall_class}">
        {status}
      </div>
    </header>

    <section class="cards">
      {card_html}
    </section>

    <section class="grid">
      <article class="panel">
        <h2>Service health</h2>
        <table>
          <thead>
            <tr>
              <th>Service</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {service_html}
          </tbody>
        </table>
      </article>

      <article class="panel">
        <h2>Pipeline contract</h2>
        <table>
          <tbody>
            <tr>
              <th>DAG</th>
              <td>{escape(str(dag["dag_id"]))}</td>
            </tr>
            <tr>
              <th>Tasks</th>
              <td>{dag["task_count"]:,}</td>
            </tr>
            <tr>
              <th>Import errors</th>
              <td>{dag["import_errors"]:,}</td>
            </tr>
            <tr>
              <th>Measured events</th>
              <td>{model["measured_events"]:,}</td>
            </tr>
          </tbody>
        </table>

        <h2>Active validation failures</h2>
        <ul>
          {alert_html}
        </ul>
      </article>

      <article class="panel wide">
        <h2>Current-state reconciliation</h2>
        <table>
          <thead>
            <tr>
              <th>Source table</th>
              <th>Source</th>
              <th>Warehouse state</th>
              <th>Difference</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {reconciliation_html}
          </tbody>
        </table>
      </article>

      <article class="panel">
        <h2>Accumulated latency</h2>
        <table>
          <thead>
            <tr>
              <th>Stage</th>
              <th>Average</th>
              <th>Maximum</th>
            </tr>
          </thead>
          <tbody>
            {latency_html}
          </tbody>
        </table>
        <p class="note">
          Historical accumulated values include periods
          when data intentionally remained at rest and are
          not steady-state production latency.
        </p>
      </article>

      <article class="panel">
        <h2>Quarantine reasons</h2>
        <table>
          <thead>
            <tr>
              <th>Reason</th>
              <th>Records</th>
            </tr>
          </thead>
          <tbody>
            {reason_html}
          </tbody>
        </table>
      </article>
    </section>

    <footer>
      Generated from live reliability and deployment
      validation results. No dashboard metrics are
      hard-coded.
    </footer>
  </main>
</body>
</html>
"""


__all__ = [
    "build_dashboard_model",
    "render_dashboard_html",
]
