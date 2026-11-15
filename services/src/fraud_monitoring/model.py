from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FraudModel:
    version: str
    bias: float
    weights: dict[str, float]
    fraud_threshold: float

    @classmethod
    def load(cls, model_dir: str | Path) -> "FraudModel":
        directory = Path(model_dir)
        version_file = directory / "latest.txt"
        if not version_file.exists():
            raise FileNotFoundError(f"Model pointer does not exist: {version_file}")
        version = version_file.read_text(encoding="utf-8").strip()
        if not version or "/" in version or ".." in version:
            raise ValueError("Invalid model version in latest.txt")
        model_path = directory / f"{version}.json"
        payload = json.loads(model_path.read_text(encoding="utf-8"))
        if payload.get("version") != version:
            raise ValueError(f"Model version mismatch in {model_path}")
        weights = {str(key): float(value) for key, value in payload["weights"].items()}
        threshold = float(payload.get("fraud_threshold", os.getenv("FRAUD_THRESHOLD", "0.65")))
        if not 0 < threshold < 1:
            raise ValueError("fraud_threshold must be between 0 and 1")
        return cls(version=version, bias=float(payload["bias"]), weights=weights, fraud_threshold=threshold)

    def predict_score(self, transaction: dict[str, Any]) -> float:
        features = {
            "amount_scaled": min(float(transaction["amount"]) / 1000.0, 1000.0),
            "is_online": float(bool(transaction.get("is_online", False))),
            "is_international": float(bool(transaction.get("is_international", False))),
            "merchant_risk": float(transaction.get("merchant_risk", 0.2)),
        }
        logit = self.bias + sum(self.weights.get(name, 0.0) * value for name, value in features.items())
        # Numerically stable sigmoid.
        if logit >= 0:
            return 1.0 / (1.0 + math.exp(-min(logit, 700)))
        exp_value = math.exp(max(logit, -700))
        return exp_value / (1.0 + exp_value)
