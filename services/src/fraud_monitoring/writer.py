from __future__ import annotations

import logging
import os
import time

import psycopg2
from prometheus_client import Counter, Histogram, start_http_server

from fraud_monitoring.common import connect_consumer, parse_transaction_time

logger = logging.getLogger("scoring-writer")

SCORES_WRITTEN = Counter(
    "fraud_writer_scores_written_total",
    "Number of scoring results committed to PostgreSQL",
)
FRAUD_SCORES_WRITTEN = Counter(
    "fraud_writer_fraud_scores_written_total",
    "Number of PostgreSQL rows with fraud_flag=1",
)
WRITE_TIME = Histogram(
    "fraud_writer_write_seconds",
    "Time to insert and commit one scoring result in PostgreSQL",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
SAVED_SCORE_DISTRIBUTION = Histogram(
    "fraud_writer_score_distribution",
    "Distribution of scores successfully stored in PostgreSQL",
    buckets=(0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0),
)

INSERT_SQL = """
INSERT INTO scores (
    transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id,
    transaction_amount, is_online, is_international, transaction_time
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
RETURNING id
"""


def connect_db():
    while True:
        try:
            connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "fraud_db"),
                user=os.getenv("POSTGRES_USER", "fraud_user"),
                password=os.getenv("POSTGRES_PASSWORD", "fraud_pass_change_me"),
                connect_timeout=5,
            )
            connection.autocommit = False
            logger.info("Connected to PostgreSQL")
            return connection
        except psycopg2.OperationalError:
            logger.exception("PostgreSQL is not ready; retrying in 3 seconds")
            time.sleep(3)


def main() -> None:
    metrics_port = int(os.getenv("WRITER_METRICS_PORT", "8001"))
    topic = os.getenv("SCORING_TOPIC", "scoring")
    group_id = os.getenv("KAFKA_GROUP_ID", "fraud-writer-v1")
    start_http_server(metrics_port)
    logger.info("Prometheus metrics listening on :%s/metrics", metrics_port)

    connection = connect_db()
    consumer = connect_consumer(topic, group_id)

    for message in consumer:
        payload = message.value
        started = time.perf_counter()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    INSERT_SQL,
                    (
                        str(payload["transaction_id"]),
                        float(payload["score"]),
                        int(payload["fraud_flag"]),
                        str(payload["model_version"]),
                        str(payload["us_state"]).upper(),
                        str(payload["merch"]),
                        str(payload["cat_id"]),
                        float(payload["amount"]),
                        bool(payload.get("is_online", False)),
                        bool(payload.get("is_international", False)),
                        parse_transaction_time(payload.get("transaction_time")),
                    ),
                )
                inserted = cursor.fetchone() is not None
            connection.commit()
            elapsed = time.perf_counter() - started
            if inserted:
                SCORES_WRITTEN.inc()
                SAVED_SCORE_DISTRIBUTION.observe(float(payload["score"]))
                if int(payload["fraud_flag"]):
                    FRAUD_SCORES_WRITTEN.inc()
                WRITE_TIME.observe(elapsed)
            consumer.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.exception("Lost PostgreSQL connection; reconnecting")
            try:
                connection.close()
            except Exception:
                pass
            connection = connect_db()
        except Exception:
            logger.exception("Failed to persist scoring result")
            connection.rollback()
            time.sleep(1)


if __name__ == "__main__":
    main()
