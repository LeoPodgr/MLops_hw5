#!/usr/bin/env python3
"""Publish a batch of controllable test transactions into Kafka."""
from __future__ import annotations

import argparse
import random
import time
import uuid
from datetime import datetime, timezone

from fraud_monitoring.common import connect_producer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--amount", type=float, default=2500.0)
    parser.add_argument("--state", default="NY")
    parser.add_argument("--merchant", default="demo_high_risk_merchant")
    parser.add_argument("--category", default="electronics")
    parser.add_argument("--delay", type=float, default=0.005)
    parser.add_argument("--topic", default="transactions")
    args = parser.parse_args()
    if args.count < 1 or args.amount < 0 or len(args.state) != 2:
        parser.error("count must be positive, amount non-negative and state a two-letter code")

    producer = connect_producer()
    for _ in range(args.count):
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "transaction_time": datetime.now(timezone.utc).isoformat(),
            "amount": args.amount,
            "us_state": args.state.upper(),
            "merch": args.merchant,
            "cat_id": args.category,
            "is_online": True,
            "is_international": True,
            "merchant_risk": 1.0,
        }
        producer.send(args.topic, value=transaction)
        if args.delay > 0:
            time.sleep(args.delay)
    producer.flush(timeout=30)
    print(f"Published {args.count} demo transactions to {args.topic} (state={args.state.upper()}, merch={args.merchant}).")


if __name__ == "__main__":
    main()
