from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from fraud_monitoring.common import connect_consumer, connect_producer, validate_transaction
from fraud_monitoring.model import FraudModel

logger = logging.getLogger("fraud-detector")

TRANSACTIONS_PROCESSED = Counter(
    "fraud_scorer_transactions_processed_total",
    "Number of transactions successfully scored and published",
)
FRAUD_TRANSACTIONS = Counter(
    "fraud_scorer_fraud_transactions_total",
    "Number of scored transactions flagged as fraud",
)
PROCESSING_TIME = Histogram(
    "fraud_scorer_processing_seconds",
    "End-to-end scoring and Kafka publish time per transaction",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
SCORE_DISTRIBUTION = Histogram(
    "fraud_scorer_score_distribution",
    "Distribution of fraud probability scores",
    buckets=(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0),
)
FRAUD_SHARE = Gauge(
    "fraud_scorer_fraud_share",
    "Cumulative share of scored transactions flagged as fraud (0..1)",
)
INPUT_VALIDATION_FAILURES = Counter(
    "fraud_scorer_input_validation_failures_total",
    "Transactions rejected before inference because of invalid input",
    ["reason"],
)
MODEL_VERSION_LOADED = Counter(
    "fraud_scorer_model_version_loaded_total",
    "Number of times a model version has been loaded by the live scorer",
    ["version"],
)
MODEL_VERSION_LOADED_AT = Gauge(
    "fraud_scorer_model_version_loaded_timestamp_seconds",
    "Unix timestamp when the scorer last loaded a model version",
    ["version"],
)


def main() -> None:
    model_dir = Path(os.getenv("MODEL_DIR", "/models"))
    metrics_port = int(os.getenv("SCORER_METRICS_PORT", "8000"))
    transactions_topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
    scoring_topic = os.getenv("SCORING_TOPIC", "scoring")
    group_id = os.getenv("KAFKA_GROUP_ID", "fraud-scorer-v1")

    start_http_server(metrics_port)
    logger.info("Prometheus metrics listening on :%s/metrics", metrics_port)
    model = FraudModel.load(model_dir)
    MODEL_VERSION_LOADED.labels(version=model.version).inc()
    MODEL_VERSION_LOADED_AT.labels(version=model.version).set(time.time())
    logger.info("Loaded model version=%s", model.version)

    consumer = connect_consumer(transactions_topic, group_id)
    producer = connect_producer()
    total_scored = 0
    total_fraud = 0

    for message in consumer:
        started = time.perf_counter()
        try:
            transaction = validate_transaction(message.value)
        except ValueError as exc:
            INPUT_VALIDATION_FAILURES.labels(reason=str(exc)).inc()
            consumer.commit()
            continue
        except Exception:
            logger.exception("Unexpected input validation error")
            INPUT_VALIDATION_FAILURES.labels(reason="unexpected_validation_error").inc()
            consumer.commit()
            continue

        try:
            # Check the pointer on each incoming record. Updating latest.txt is enough
            # to hot-reload a new artifact without restarting this container.
            try:
                candidate = FraudModel.load(model_dir)
                if candidate.version != model.version:
                    model = candidate
                    MODEL_VERSION_LOADED.labels(version=model.version).inc()
                    MODEL_VERSION_LOADED_AT.labels(version=model.version).set(time.time())
                    logger.info("Hot-loaded model version=%s", model.version)
            except (OSError, ValueError, json.JSONDecodeError):
                logger.exception("Cannot reload model pointer; continue with version=%s", model.version)

            score = model.predict_score(transaction)
            fraud_flag = int(score >= model.fraud_threshold)
            output = {
                **transaction,
                "score": round(score, 8),
                "fraud_flag": fraud_flag,
                "model_version": model.version,
            }
            producer.send(scoring_topic, value=output).get(timeout=10)

            total_scored += 1
            total_fraud += fraud_flag
            TRANSACTIONS_PROCESSED.inc()
            SCORE_DISTRIBUTION.observe(score)
            if fraud_flag:
                FRAUD_TRANSACTIONS.inc()
            FRAUD_SHARE.set(total_fraud / total_scored)
            PROCESSING_TIME.observe(time.perf_counter() - started)
            consumer.commit()
        except Exception:
            logger.exception("Failed to score/publish transaction_id=%s", transaction.get("transaction_id"))
            time.sleep(1)


if __name__ == "__main__":
    main()
