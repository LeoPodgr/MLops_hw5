#!/usr/bin/env python3
"""Fast static validation; does not build images or start containers."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_DASHBOARDS = {
    "01-fraud-scoring.json",
    "02-scoring-writer.json",
    "03-system-metrics.json",
    "04-fraud-business-analysis.json",
}
REQUIRED_METRICS = [
    "fraud_scorer_transactions_processed_total",
    "fraud_scorer_processing_seconds",
    "fraud_scorer_score_distribution",
    "fraud_scorer_fraud_transactions_total",
    "fraud_scorer_input_validation_failures_total",
    "fraud_writer_scores_written_total",
    "fraud_writer_write_seconds",
    "fraud_writer_fraud_scores_written_total",
    "fraud_writer_score_distribution",
]


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    raise SystemExit(1)


def main() -> None:
    dashboard_dir = ROOT / "grafana" / "dashboards"
    found = {path.name for path in dashboard_dir.glob("*.json")}
    missing = REQUIRED_DASHBOARDS - found
    if missing:
        fail(f"missing dashboards: {sorted(missing)}")

    dashboards = {}
    for path in sorted(dashboard_dir.glob("*.json")):
        try:
            dashboards[path.name] = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            fail(f"invalid dashboard JSON {path.name}: {exc}")
        if not dashboards[path.name].get("panels"):
            fail(f"dashboard has no panels: {path.name}")

    all_text = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "services" / "src").rglob("*.py"))
    missing_metrics = [metric for metric in REQUIRED_METRICS if metric not in all_text]
    if missing_metrics:
        fail(f"metrics not found in application source: {missing_metrics}")

    model_pointer = (ROOT / "model_registry" / "latest.txt").read_text(encoding="utf-8").strip()
    model_path = ROOT / "model_registry" / f"{model_pointer}.json"
    if not model_path.exists():
        fail(f"latest model artifact missing: {model_path}")
    json.loads(model_path.read_text(encoding="utf-8"))

    alert_file = ROOT / "grafana" / "provisioning" / "alerting" / "alert-rules.yml"
    alert_text = alert_file.read_text(encoding="utf-8")
    if alert_text.count("uid:") < 2 or "Fraud share above 25 percent" not in alert_text or "Scoring stream stopped" not in alert_text:
        fail("expected two provisioned Grafana alert rules")

    print(f"OK: {len(dashboards)} dashboards have valid JSON and panels")
    print(f"OK: {len(REQUIRED_METRICS)} required/custom metric names are present in service code")
    print(f"OK: model pointer resolves to {model_pointer}.json")
    print("OK: two Grafana alert rules are present")
    print("NOTE: run 'docker compose config -q' separately to validate Compose with Docker installed")


if __name__ == "__main__":
    main()
