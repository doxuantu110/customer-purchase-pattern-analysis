"""
Regression tests for Star Schema validation logic in analysis/10_star_schema_modeling.py.

Run from repository root:
    python -m unittest analysis/test_star_schema_modeling.py -v
"""
import importlib.util
import os
import unittest

MODULE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "10_star_schema_modeling.py"
)
SPEC = importlib.util.spec_from_file_location("star_schema_modeling", MODULE_PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


def _passing_fk_checks():
    return {
        "CustomerKey_null_count": 0,
        "CustomerKey_invalid_count": 0,
        "ProductKey_null_count": 0,
        "ProductKey_invalid_count": 0,
        "DateKey_null_count": 0,
        "DateKey_invalid_count": 0,
        "TimeKey_null_count": 0,
        "TimeKey_invalid_count": 0,
        "fact_row_count_matches_source": True,
    }


class TestForeignKeyIntegrityEvaluation(unittest.TestCase):
    def test_all_fk_checks_pass_produces_no_failure(self):
        failures = MOD.evaluate_fk_integrity(_passing_fk_checks())
        self.assertEqual(failures, [])

    def test_nonzero_fk_null_count_produces_failure(self):
        checks = _passing_fk_checks()
        checks["CustomerKey_null_count"] = 3
        failures = MOD.evaluate_fk_integrity(checks)
        self.assertEqual(len(failures), 1)
        self.assertIn("CustomerKey_null_count=3", failures[0])

    def test_nonzero_fk_invalid_count_produces_failure(self):
        checks = _passing_fk_checks()
        checks["ProductKey_invalid_count"] = 2
        failures = MOD.evaluate_fk_integrity(checks)
        self.assertEqual(len(failures), 1)
        self.assertIn("ProductKey_invalid_count=2", failures[0])

    def test_fact_row_count_mismatch_produces_failure(self):
        checks = _passing_fk_checks()
        checks["fact_row_count_matches_source"] = False
        failures = MOD.evaluate_fk_integrity(checks)
        self.assertEqual(len(failures), 1)
        self.assertIn("fact_row_count_matches_source=False", failures[0])


class TestRevenueReconciliation(unittest.TestCase):
    def test_exact_decimal_match_despite_binary64_drift(self):
        # 0.1 + 0.2 != 0.3 in binary64, but Decimal sums match exactly.
        rec = MOD.build_revenue_reconciliation(
            tx_amounts=[0.1, 0.2],
            fact_amounts=[0.1, 0.2],
            seg_totals=[0.3],
            cs_totals=[0.3],
            ps_totals=[0.3],
            tx_count=2,
            fact_count=2,
            seg_purchases_sum=2,
            cs_purchases_sum=2,
            ps_purchases_sum=2,
        )
        self.assertEqual(rec["diff_source_vs_fact"], 0.0)
        self.assertTrue(rec["source_vs_fact_exact_match"])
        self.assertEqual(rec["diff_source_vs_customer_segments"], 0.0)
        self.assertTrue(rec["source_vs_customer_segments_exact_match"])
        self.assertTrue(rec["source_vs_customer_segments_within_tolerance"])
        self.assertTrue(rec["source_vs_customerstats_exact_match"])

    def test_real_rounding_difference_reports_not_exact_but_within_tolerance(self):
        rec = MOD.build_revenue_reconciliation(
            tx_amounts=[100.00],
            fact_amounts=[100.00],
            seg_totals=[100.01],
            cs_totals=[100.01],
            ps_totals=[100.00],
            tx_count=1,
            fact_count=1,
            seg_purchases_sum=1,
            cs_purchases_sum=1,
            ps_purchases_sum=1,
        )
        self.assertEqual(rec["diff_source_vs_customer_segments"], 0.01)
        self.assertFalse(rec["source_vs_customer_segments_exact_match"])
        self.assertTrue(rec["source_vs_customer_segments_within_tolerance"])

    def test_over_tolerance_difference_fails_tolerance(self):
        rec = MOD.build_revenue_reconciliation(
            tx_amounts=[100.00],
            fact_amounts=[100.05],
            seg_totals=[102.00],
            cs_totals=[102.00],
            ps_totals=[100.00],
            tx_count=1,
            fact_count=1,
            seg_purchases_sum=1,
            cs_purchases_sum=1,
            ps_purchases_sum=1,
        )
        self.assertFalse(rec["source_vs_fact_exact_match"])
        self.assertFalse(rec["source_vs_fact_within_tolerance"])
        self.assertFalse(rec["source_vs_customer_segments_exact_match"])
        self.assertFalse(rec["source_vs_customer_segments_within_tolerance"])


if __name__ == "__main__":
    unittest.main()
