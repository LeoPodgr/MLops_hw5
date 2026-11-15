from __future__ import annotations

import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from fraud_monitoring.common import connect_producer

logger = logging.getLogger("transaction-simulator")

STATES = ["CA", "NY", "TX", "FL", "WA", "IL", "MA", "AZ", "CO", "GA", "NC", "NV"]
MERCHANTS = [
    ("online_market", 0.35),
    ("electronics_shop", 0.55),
    ("grocery_chain", 0.08),
    ("travel_booking", 0.72),
    ("pharmacy", 0.12),
    ("fashion_store", 0.25),
    ("digital_services", 0.48),
    ("fuel_station", 0.10),
    ("gaming_store", 0.62),
]
CATEGORIES = ["grocery", "electronics", "travel", "pharmacy", "fashion", "digital", "fuel", "gaming"]


def make_transaction() -> dict:
    merchant, merchant_risk = random.choice(MERCHANTS)
    amount = round(random.lognormvariate(4.15, 1.0), 2)
    amount = min(max(amount, 3.0), 2500.0)
    is_international = random.random() < 0.035
    is_online = random.random() < 0.42
    if is_international:
        amount = round(amount * random.uniform(1.1, 2.4), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "transaction_time": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "us_state": random.choice(STATES),
        "merch": merchant,
        "cat_id": random.choice(CATEGORIES),
        "is_online": is_online,
        "is_international": is_international,
        "merchant_risk": merchant_risk,
    }


def main() -> None:
    topic = os.getenv("TRANSACTIONS_TOPIC", "transactions")
    interval = max(float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "0.1")), 0.01)
    producer = connect_producer()
    sent = 0
    logger.info("Publishing synthetic transactions to topic=%s every %.3fs", topic, interval)
    while True:
        transaction = make_transaction()
        producer.send(topic, value=transaction).get(timeout=10)
        sent += 1
        if sent % 100 == 0:
            logger.info("Published %s transactions", sent)
        time.sleep(interval)


if __name__ == "__main__":
    main()
