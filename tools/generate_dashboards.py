#!/usr/bin/env python3
"""Regenerate the checked-in Grafana dashboard JSON files."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "postgres", "uid": "postgres"}


def prom_panel(panel_id, title, expr, x, y, w=12, h=8, unit="short", legend="{{le}}", chart="timeseries", description=""):
    panel = {
        "id": panel_id,
        "title": title,
        "type": chart,
        "datasource": PROM,
        "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "palette-classic"},
                "custom": {"drawStyle": "line", "lineInterpolation": "smooth", "lineWidth": 2, "fillOpacity": 12, "showPoints": "never", "spanNulls": False},
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [{"datasource": PROM, "editorMode": "code", "expr": expr, "legendFormat": legend, "refId": "A", "range": True}],
    }
    return panel


def pg_panel(panel_id, title, sql, x, y, w=12, h=8, unit="short", chart="timeseries", description="", format="table"):
    return {
        "id": panel_id,
        "title": title,
        "type": chart,
        "datasource": PG,
        "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10, "showPoints": "never"},
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
            **({"orientation": "vertical", "xField": "score_bucket" if panel_id == 1 else ("cat_id" if panel_id == 3 else "merch"), "showValue": "always", "groupWidth": 0.7, "barWidth": 0.8, "stacking": {"mode": "none", "group": "A"}} if chart == "barchart" else {}),
        },
        "targets": [{"datasource": PG, "format": format, "rawSql": sql, "refId": "A", "editorMode": "code"}],
    }


def base_dashboard(title, uid, tags, panels, annotations=None, variables=None, description=""):
    annotation_list = [
        {
            "builtIn": 1,
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True,
            "hide": True,
            "iconColor": "rgba(0, 211, 255, 1)",
            "name": "Annotations & Alerts",
            "type": "dashboard",
        }
    ]
    if annotations:
        annotation_list.extend(annotations)
    return {
        "annotations": {"list": annotation_list},
        "description": description,
        "editable": False,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "id": None,
        "links": [],
        "liveNow": False,
        "panels": panels,
        "refresh": "5s",
        "schemaVersion": 39,
        "tags": tags,
        "templating": {"list": variables or []},
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
    }


def save(name, dashboard):
    (OUT / name).write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


OUT.mkdir(parents=True, exist_ok=True)

model_annotations = [
    {
        "datasource": PROM,
        "enable": True,
        "expr": "time() - fraud_scorer_model_version_loaded_timestamp_seconds < 30",
        "iconColor": "orange",
        "name": "Model version reload (30 seconds)",
        "tagKeys": "version",
        "textFormat": "Scorer loaded model {{version}}",
        "titleFormat": "Model reload",
    }
]

# Dashboard 1: online scoring / Kafka consumer side.
scoring_panels = [
    prom_panel(1, "Scoring throughput", "sum(rate(fraud_scorer_transactions_processed_total[1m]))", 0, 0, unit="ops", legend="scored transactions / sec", description="Successful scoring outputs per second."),
    prom_panel(2, "Fraud share", "100 * sum(rate(fraud_scorer_fraud_transactions_total[5m])) / clamp_min(sum(rate(fraud_scorer_transactions_processed_total[5m])), 0.001)", 12, 0, unit="percent", legend="fraud share", description="Rolling fraud-flag share, calculated from live counters."),
    prom_panel(3, "Scoring processing latency (p50 / p95 / p99)", "histogram_quantile(0.50, sum by (le) (rate(fraud_scorer_processing_seconds_bucket[5m])))", 0, 8, 8, 8, "s", "p50"),
    prom_panel(4, "Scoring score distribution over time", "sum by (le) (rate(fraud_scorer_score_distribution_bucket[2m]))", 8, 8, 16, 8, "ops", "score upper bound {{le}}", description="Rate of observations in each cumulative score histogram bucket."),
    prom_panel(5, "Input validation failures (extra project metric)", "sum by (reason) (increase(fraud_scorer_input_validation_failures_total[5m]))", 0, 16, 12, 8, "short", "{{reason}}", description="Additional data-quality metric not limited to the seminar's baseline monitoring: invalid transaction messages rejected before inference."),
    prom_panel(6, "Model versions loaded by the live scorer", "fraud_scorer_model_version_loaded_total", 12, 16, 12, 8, "short", "{{version}}", description="A new labeled series appears after latest.txt points to a new model artifact."),
]
scoring_panels[2]["targets"].extend([
    {"datasource": PROM, "editorMode": "code", "expr": "histogram_quantile(0.95, sum by (le) (rate(fraud_scorer_processing_seconds_bucket[5m])))", "legendFormat": "p95", "refId": "B", "range": True},
    {"datasource": PROM, "editorMode": "code", "expr": "histogram_quantile(0.99, sum by (le) (rate(fraud_scorer_processing_seconds_bucket[5m])))", "legendFormat": "p99", "refId": "C", "range": True},
])

scoring = base_dashboard(
    "Fraud Scoring — поток и модель",
    "fraud-scoring",
    ["fraud", "scoring", "prometheus"],
    scoring_panels,
    annotations=model_annotations,
    description="Online scoring throughput, latency, fraud share, score distribution, input quality and live model hot-reload events. Orange annotation marks a model version for approximately 30 seconds.",
)
save("01-fraud-scoring.json", scoring)

# Dashboard 2: writer side + SQL breakdown by model version.
writer_panels = [
    prom_panel(1, "Results written to PostgreSQL / sec", "sum(rate(fraud_writer_scores_written_total[1m]))", 0, 0, unit="ops", legend="rows/sec"),
    prom_panel(2, "Fraud results written / sec", "sum(rate(fraud_writer_fraud_scores_written_total[1m]))", 12, 0, unit="ops", legend="fraud rows/sec"),
    prom_panel(3, "PostgreSQL write latency (p95)", "histogram_quantile(0.95, sum by (le) (rate(fraud_writer_write_seconds_bucket[5m])))", 0, 8, 12, 8, "s", "p95"),
    prom_panel(4, "Stored score distribution over time", "sum by (le) (rate(fraud_writer_score_distribution_bucket[2m]))", 12, 8, 12, 8, "ops", "score upper bound {{le}}"),
    pg_panel(
        5,
        "Scores and fraud share by model version (PostgreSQL)",
        """SELECT model_version AS model_version,
       COUNT(*) AS scores_written,
       ROUND(100.0 * AVG(fraud_flag::numeric), 2) AS fraud_share_pct,
       MAX(created_at) AS last_score_at
FROM scores
WHERE $__timeFilter(created_at)
GROUP BY model_version
ORDER BY MAX(created_at) DESC""",
        0, 16, 24, 9, "short", "table", "Database view: number of persisted results and fraud share grouped by the actual model version which produced them.", "table"
    ),
]
writer = base_dashboard(
    "Scoring Writer — запись в PostgreSQL",
    "fraud-writer",
    ["fraud", "postgres", "writer"],
    writer_panels,
    description="Metrics from scoring_writer and a PostgreSQL view grouped by model version.",
)
save("02-scoring-writer.json", writer)

# Dashboard 3: Node Exporter.
system_panels = [
    prom_panel(1, "CPU busy", "100 * (1 - avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])))", 0, 0, 12, 8, "percent", "{{instance}}"),
    prom_panel(2, "Memory used", "100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)", 12, 0, 12, 8, "percent", "{{instance}}"),
    prom_panel(3, "Network receive / transmit", "sum by (instance) (rate(node_network_receive_bytes_total{device!~\"lo|veth.*|docker.*\"}[5m]))", 0, 8, 12, 8, "Bps", "RX {{instance}}"),
    prom_panel(4, "Disk used", "100 * (1 - node_filesystem_avail_bytes{fstype!~\"tmpfs|overlay|squashfs\", mountpoint!~\"/run.*|/var/lib/docker.*\"} / node_filesystem_size_bytes{fstype!~\"tmpfs|overlay|squashfs\", mountpoint!~\"/run.*|/var/lib/docker.*\"})", 12, 8, 12, 8, "percent", "{{mountpoint}}"),
]
# Add TX as a second series in the network panel.
system_panels[2]["targets"].append({"datasource": PROM, "editorMode": "code", "expr": "sum by (instance) (rate(node_network_transmit_bytes_total{device!~\"lo|veth.*|docker.*\"}[5m]))", "legendFormat": "TX {{instance}}", "refId": "B", "range": True})
system = base_dashboard(
    "System Metrics — Node Exporter",
    "fraud-system",
    ["system", "node-exporter"],
    system_panels,
    description="CPU, RAM, network throughput and filesystem utilization from Node Exporter.",
)
save("03-system-metrics.json", system)

# Dashboard 4: business analysis with PostgreSQL-backed state and merchant selectors.
variable_state = {
    "name": "state",
    "label": "US state",
    "type": "query",
    "datasource": PG,
    "definition": "SELECT DISTINCT us_state AS __text, us_state AS __value FROM scores ORDER BY 1",
    "query": "SELECT DISTINCT us_state AS __text, us_state AS __value FROM scores ORDER BY 1",
    "refresh": 2,
    "sort": 1,
    "multi": True,
    "includeAll": True,
    "current": {"selected": True, "text": "All", "value": "$__all"},
    "options": [],
    "hide": 0,
    "skipUrlSync": False,
}
variable_merch = {
    "name": "merch",
    "label": "Merchant",
    "type": "query",
    "datasource": PG,
    "definition": "SELECT DISTINCT merch AS __text, merch AS __value FROM scores ORDER BY 1",
    "query": "SELECT DISTINCT merch AS __text, merch AS __value FROM scores ORDER BY 1",
    "refresh": 2,
    "sort": 1,
    "multi": True,
    "includeAll": True,
    "current": {"selected": True, "text": "All", "value": "$__all"},
    "options": [],
    "hide": 0,
    "skipUrlSync": False,
}

business_panels = [
    pg_panel(
        1,
        "Score density (most recent 1,000 filtered transactions)",
        """WITH recent AS (
    SELECT score
    FROM scores
    WHERE us_state IN (${state:sqlstring})
      AND merch IN (${merch:sqlstring})
    ORDER BY created_at DESC
    LIMIT 1000
), buckets AS (
    SELECT width_bucket(score, 0.0, 1.000001, 20) AS bucket, COUNT(*) AS n
    FROM recent
    GROUP BY 1
)
SELECT (bucket::text || ' / 20') AS score_bucket,
       n::double precision / NULLIF(SUM(n) OVER (), 0) AS density
FROM buckets
ORDER BY bucket""",
        0, 0, 12, 9, "percentunit", "barchart", "Filtered density distribution of probability scores. The query uses only the latest 1,000 rows matching the selected state and merchant.", "table"
    ),
    pg_panel(
        2,
        "Filtered TPS (transactions per second)",
        """SELECT $__timeGroupAlias(created_at, '1m'), COUNT(*)::double precision / 60.0 AS tps
FROM scores
WHERE $__timeFilter(created_at)
  AND us_state IN (${state:sqlstring})
  AND merch IN (${merch:sqlstring})
GROUP BY 1
ORDER BY 1""",
        12, 0, 12, 9, "ops", "timeseries", "Minute counts are divided by 60 to display average transactions per second for the selected state and merchant filters.", "time_series"
    ),
    pg_panel(
        3,
        "Fraud share by product category — last 1,000 matching transactions",
        """WITH latest_1000 AS (
    SELECT cat_id, fraud_flag
    FROM scores
    WHERE us_state IN (${state:sqlstring})
      AND merch IN (${merch:sqlstring})
    ORDER BY created_at DESC
    LIMIT 1000
)
SELECT cat_id,
       ROUND(100.0 * AVG(fraud_flag::numeric), 2) AS fraud_share_pct,
       COUNT(*) AS transactions
FROM latest_1000
GROUP BY cat_id
ORDER BY fraud_share_pct DESC""",
        0, 9, 12, 9, "percent", "barchart", "Required category bar plot. The average fraud flag is calculated over the latest 1,000 filtered transactions.", "table"
    ),
    pg_panel(
        4,
        "Flagged transaction amount by merchant — last 24 hours",
        """SELECT merch,
       ROUND(SUM(transaction_amount * fraud_flag)::numeric, 2) AS flagged_amount
FROM scores
WHERE created_at >= NOW() - INTERVAL '24 hours'
  AND us_state IN (${state:sqlstring})
  AND merch IN (${merch:sqlstring})
GROUP BY merch
ORDER BY flagged_amount DESC
LIMIT 10""",
        12, 9, 12, 9, "currencyUSD", "barchart", "Additional business metric: nominal amount of transactions flagged as fraud, grouped by merchant for the last 24 hours.", "table"
    ),
]
business = base_dashboard(
    "Fraud Business Analysis — фильтры и категории",
    "fraud-business",
    ["fraud", "business", "postgres"],
    business_panels,
    variables=[variable_state, variable_merch],
    description="PostgreSQL business analysis. Filters apply to score density and filtered TPS; the category fraud-share bar plot uses the latest 1,000 matching transactions. Includes a merchant exposure chart.",
)
save("04-fraud-business-analysis.json", business)

print(f"Generated {len(list(OUT.glob('*.json')))} dashboards in {OUT}")
