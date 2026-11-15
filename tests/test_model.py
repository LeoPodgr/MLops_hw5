from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "src"))
from fraud_monitoring.model import FraudModel


class FraudModelTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.model_dir = Path(self.tempdir.name)
        payload = {
            "version": "test-v1",
            "bias": -4.0,
            "weights": {
                "amount_scaled": 3.0,
                "is_online": 1.0,
                "is_international": 1.8,
                "merchant_risk": 0.8,
            },
            "fraud_threshold": 0.65,
        }
        (self.model_dir / "test-v1.json").write_text(json.dumps(payload), encoding="utf-8")
        (self.model_dir / "latest.txt").write_text("test-v1\n", encoding="utf-8")

    def tearDown(self):
        self.tempdir.cleanup()

    def test_model_loads_from_pointer(self):
        model = FraudModel.load(self.model_dir)
        self.assertEqual(model.version, "test-v1")
        self.assertAlmostEqual(model.fraud_threshold, 0.65)

    def test_pointer_can_switch_to_a_new_model_version(self):
        payload = {
            "version": "test-v2",
            "bias": 2.0,
            "weights": {"amount_scaled": 3.0, "is_online": 1.0, "is_international": 1.8, "merchant_risk": 0.8},
            "fraud_threshold": 0.65,
        }
        (self.model_dir / "test-v2.json").write_text(json.dumps(payload), encoding="utf-8")
        (self.model_dir / "latest.txt").write_text("test-v2\n", encoding="utf-8")
        self.assertEqual(FraudModel.load(self.model_dir).version, "test-v2")

    def test_scores_are_probabilities(self):
        model = FraudModel.load(self.model_dir)
        transaction = {
            "amount": 100.0,
            "is_online": False,
            "is_international": False,
            "merchant_risk": 0.1,
        }
        score = model.predict_score(transaction)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_high_risk_transaction_scores_above_low_risk(self):
        model = FraudModel.load(self.model_dir)
        low = model.predict_score({"amount": 20, "is_online": False, "is_international": False, "merchant_risk": 0.05})
        high = model.predict_score({"amount": 2500, "is_online": True, "is_international": True, "merchant_risk": 1.0})
        self.assertGreater(high, low)
        self.assertGreater(high, 0.65)


if __name__ == "__main__":
    unittest.main()
