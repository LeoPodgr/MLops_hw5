from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def kafka_bootstrap_servers() -> list[str]:
    return [item.strip() for item in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",") if item.strip()]


def connect_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers(),
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
                acks="all",
                retries=10,
                max_in_flight_requests_per_connection=1,
                linger_ms=5,
            )
            logging.getLogger(__name__).info("Connected Kafka producer")
            return producer
        except NoBrokersAvailable:
            logging.getLogger(__name__).warning("Kafka is not ready; retrying in 2 seconds")
            time.sleep(2)


def _safe_json_deserializer(value: bytes) -> Any:
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"__invalid_json__": True}


def connect_consumer(topic: str, group_id: str) -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=kafka_bootstrap_servers(),
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset="latest",
                value_deserializer=_safe_json_deserializer,
                max_poll_records=100,
            )
            logging.getLogger(__name__).info("Connected Kafka consumer: topic=%s group=%s", topic, group_id)
            return consumer
        except NoBrokersAvailable:
            logging.getLogger(__name__).warning("Kafka is not ready; retrying in 2 seconds")
            time.sleep(2)


def validate_transaction(transaction: Any) -> dict[str, Any]:
    if isinstance(transaction, dict) and transaction.get("__invalid_json__"):
        raise ValueError("invalid_json")
    if not isinstance(transaction, dict):
        raise ValueError("not_an_object")
    required = ("transaction_id", "amount", "us_state", "merch", "cat_id")
    for field in required:
        if field not in transaction or transaction[field] is None:
            raise ValueError(f"missing_{field}")
    try:
        amount = float(transaction["amount"])
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_amount") from exc
    if amount < 0 or amount > 1_000_000:
        raise ValueError("amount_out_of_range")
    state = str(transaction["us_state"]).upper()
    if len(state) != 2 or not state.isalpha():
        raise ValueError("invalid_us_state")
    normalized = dict(transaction)
    normalized["transaction_id"] = str(transaction["transaction_id"])
    normalized["amount"] = amount
    normalized["us_state"] = state
    normalized["merch"] = str(transaction["merch"])
    normalized["cat_id"] = str(transaction["cat_id"])
    normalized["is_online"] = bool(transaction.get("is_online", False))
    normalized["is_international"] = bool(transaction.get("is_international", False))
    normalized["merchant_risk"] = min(max(float(transaction.get("merchant_risk", 0.2)), 0.0), 1.0)
    return normalized


def parse_transaction_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
