"""
Star Schema Data Modeling Layer for Retail Customer Purchase Pattern Analysis.

Transforms cleaned purchase transactions, customer segmentation outputs, and
Hadoop MapReduce summaries into a pure Star Schema (1 Fact + 4 Dimensions)
ready for Power BI, along with a comprehensive JSON validation report.

Usage (from repository root):
    python analysis/10_star_schema_modeling.py
    python analysis/10_star_schema_modeling.py --out powerbi
"""
import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
import pandas as pd

TX_COLS = [
    "customer_id",
    "product_id",
    "amount",
    "timestamp",
    "category_code",
    "brand",
    "session_id",
]

CUSTOMERSTATS_COLS = [
    "customer_id",
    "total_spent",
    "num_purchases",
    "avg_amount",
    "first_purchase",
    "last_purchase",
]

PRODUCTSTATS_COLS = [
    "product_id",
    "revenue",
    "num_purchases",
    "avg_price",
    "first_sale",
    "last_sale",
]

DAY_NAMES_EN = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

PER_ROW_ROUNDING_TOL = 0.011
FACT_REVENUE_TOL = Decimal("0.01")
AGG_REVENUE_TOL = Decimal("1.0")
MAX_EXAMPLES = 20

FK_COUNT_KEYS = (
    "CustomerKey_null_count",
    "CustomerKey_invalid_count",
    "ProductKey_null_count",
    "ProductKey_invalid_count",
    "DateKey_null_count",
    "DateKey_invalid_count",
    "TimeKey_null_count",
    "TimeKey_invalid_count",
)


def sum_decimal(values):
    """Compute exact decimal sum to avoid IEEE-754 binary64 summation drift."""
    total = Decimal("0")
    for val in values:
        if pd.notna(val):
            total += Decimal(str(val))
    return total


def evaluate_fk_integrity(fk_checks):
    """Return critical failure messages for foreign key integrity checks."""
    failures = []
    for key in FK_COUNT_KEYS:
        val = fk_checks.get(key, 0)
        if isinstance(val, bool) or not isinstance(val, (int, np.integer)):
            failures.append(
                f"Foreign key integrity check has invalid type: {key}={val!r}"
            )
        elif val > 0:
            failures.append(f"Foreign key integrity error: {key}={val}")

    row_match = fk_checks.get("fact_row_count_matches_source")
    if row_match is not True:
        failures.append(
            f"Fact check failed: fact_row_count_matches_source={row_match}"
        )
    return failures


def build_revenue_reconciliation(
    tx_amounts,
    fact_amounts,
    seg_totals,
    cs_totals,
    ps_totals,
    tx_count,
    fact_count,
    seg_purchases_sum,
    cs_purchases_sum,
    ps_purchases_sum,
):
    """Reconcile revenue totals using exact Decimal arithmetic."""
    source_rev = sum_decimal(tx_amounts)
    fact_rev = sum_decimal(fact_amounts)
    seg_rev = sum_decimal(seg_totals)
    cs_rev = sum_decimal(cs_totals)
    ps_rev = sum_decimal(ps_totals)

    diff_source_fact = abs(source_rev - fact_rev)
    diff_source_seg = abs(source_rev - seg_rev)
    diff_source_cs = abs(source_rev - cs_rev)
    diff_seg_cs = abs(seg_rev - cs_rev)
    diff_source_ps = abs(source_rev - ps_rev)

    return {
        "source_transactions_revenue": float(source_rev),
        "fact_purchase_revenue": float(fact_rev),
        "customer_segments_monetary_total": float(seg_rev),
        "customerstats_revenue_total": float(cs_rev),
        "productstats_revenue_total": float(ps_rev),
        "diff_source_vs_fact": float(diff_source_fact),
        "source_vs_fact_exact_match": bool(diff_source_fact == Decimal("0")),
        "source_vs_fact_within_tolerance": bool(diff_source_fact <= FACT_REVENUE_TOL),
        "diff_source_vs_customer_segments": float(diff_source_seg),
        "source_vs_customer_segments_exact_match": bool(
            diff_source_seg == Decimal("0")
        ),
        "source_vs_customer_segments_within_tolerance": bool(
            diff_source_seg <= AGG_REVENUE_TOL
        ),
        "diff_source_vs_customerstats": float(diff_source_cs),
        "source_vs_customerstats_exact_match": bool(diff_source_cs == Decimal("0")),
        "source_vs_customerstats_within_tolerance": bool(
            diff_source_cs <= AGG_REVENUE_TOL
        ),
        "diff_customer_segments_vs_customerstats": float(diff_seg_cs),
        "customer_segments_vs_customerstats_exact_match": bool(
            diff_seg_cs == Decimal("0")
        ),
        "diff_source_vs_productstats": float(diff_source_ps),
        "source_vs_productstats_exact_match": bool(diff_source_ps == Decimal("0")),
        "tolerances": {
            "source_vs_fact": float(FACT_REVENUE_TOL),
            "aggregate_two_decimal_rounding": float(AGG_REVENUE_TOL),
        },
        "total_purchase_rows": {
            "source_transactions": int(tx_count),
            "fact_purchase": int(fact_count),
            "sum_customer_segments_num_purchases": int(seg_purchases_sum),
            "sum_customerstats_num_purchases": int(cs_purchases_sum),
            "sum_productstats_num_purchases": int(ps_purchases_sum),
        },
    }


def parse_args():
    ap = argparse.ArgumentParser(
        description="Build Star Schema CSVs (1 Fact + 4 Dimensions) for Power BI."
    )
    ap.add_argument(
        "--transactions",
        default="data/clean/transactions.csv",
        help="Path to headerless transactions.csv",
    )
    ap.add_argument(
        "--segments",
        default="output/dashboard/customer_segments.csv",
        help="Path to customer_segments.csv",
    )
    ap.add_argument(
        "--customerstats",
        default="data/local_out/yarn_customer_r2",
        help="Directory containing Hadoop customerstats part-* TSV files",
    )
    ap.add_argument(
        "--productstats",
        default="data/local_out/full_product",
        help="Directory containing Hadoop productstats part-* TSV files",
    )
    ap.add_argument(
        "--out",
        default="powerbi",
        help="Output directory for Star Schema CSVs and validation JSON",
    )
    return ap.parse_args()


def read_hadoop_parts(path, names, dtypes):
    if not os.path.isdir(path):
        raise FileNotFoundError(f"Hadoop output directory not found: {path}")
    files = sorted(glob.glob(os.path.join(path, "part-*")))
    if not files:
        raise FileNotFoundError(f"No part-* files found in {path}")
    frames = [
        pd.read_csv(f, sep="\t", header=None, names=names, dtype=dtypes)
        for f in files
    ]
    return pd.concat(frames, ignore_index=True), files


def resolve_product_attribute(tx_df, attr_col, max_examples=MAX_EXAMPLES):
    """
    Deterministically resolve one attribute value per product_id and report
    all products with conflicting source values.

    Policy:
      1. If a product_id has both non-'unknown' and 'unknown' values, prefer
         non-'unknown' candidates.
      2. Among remaining candidates, pick the most frequent (mode) value.
      3. Break ties deterministically by latest UTC timestamp, then
         alphabetical order of the attribute value.
    """
    # Count occurrences and latest timestamp per (product_id, attr_value)
    grouped = (
        tx_df.groupby(["product_id", attr_col], sort=False)
        .agg(freq=(attr_col, "size"), latest_ts=("timestamp", "max"))
        .reset_index()
    )

    n_distinct = grouped.groupby("product_id", sort=False)[attr_col].size()
    conflict_ids = set(n_distinct[n_distinct > 1].index)

    # Mark whether candidate is 'unknown' (False < True so non-unknown sorts first)
    grouped["is_unknown"] = grouped[attr_col] == "unknown"

    # Sort so that the winning row per product_id appears first:
    # 1. product_id asc
    # 2. is_unknown asc (non-unknown first)
    # 3. freq desc (highest frequency first)
    # 4. latest_ts desc (most recent UTC timestamp first)
    # 5. attr_col asc (alphabetical tie-break)
    # For descending sort on strings (latest_ts), use rank or two-pass stable sort.
    grouped = grouped.sort_values(by=[attr_col], ascending=[True], kind="mergesort")
    grouped = grouped.sort_values(by=["latest_ts"], ascending=[False], kind="mergesort")
    grouped = grouped.sort_values(by=["freq"], ascending=[False], kind="mergesort")
    grouped = grouped.sort_values(by=["is_unknown"], ascending=[True], kind="mergesort")
    grouped = grouped.sort_values(by=["product_id"], ascending=[True], kind="mergesort")

    winners = grouped.drop_duplicates(subset=["product_id"], keep="first")[
        ["product_id", attr_col]
    ].set_index("product_id")[attr_col]

    examples = []
    if conflict_ids:
        sample_pids = sorted(conflict_ids)[:max_examples]
        conflict_rows = grouped[grouped["product_id"].isin(sample_pids)]
        for pid in sample_pids:
            sub = conflict_rows[conflict_rows["product_id"] == pid]
            value_counts = {
                str(r[attr_col]): int(r["freq"]) for _, r in sub.iterrows()
            }
            selected = str(winners.loc[pid])
            has_unknown = "unknown" in value_counts
            has_known = any(k != "unknown" for k in value_counts)
            if has_unknown and has_known:
                reason = "prefer_non_unknown_then_mode_then_latest_ts_then_alpha"
            else:
                reason = "mode_then_latest_ts_then_alpha"
            examples.append(
                {
                    "product_id": str(pid),
                    "selected_value": selected,
                    "conflicting_values": value_counts,
                    "resolution_reason": reason,
                }
            )

    return winners, conflict_ids, examples


def build_dim_customer(seg_df):
    """Build DimCustomer with deterministic CustomerKey sorted by customer_id."""
    dim = seg_df[
        [
            "customer_id",
            "recency_days",
            "num_purchases",
            "total_spent",
            "avg_amount",
            "segment_id",
            "segment",
        ]
    ].copy()
    dim = dim.rename(
        columns={"num_purchases": "frequency", "total_spent": "monetary"}
    )
    dim = dim.sort_values("customer_id", ascending=True, kind="mergesort").reset_index(
        drop=True
    )
    dim.insert(0, "CustomerKey", np.arange(1, len(dim) + 1, dtype=np.int64))
    return dim


def build_dim_product(tx_df):
    """Build DimProduct with deterministic ProductKey and conflict report."""
    cat_winners, cat_conflicts, cat_examples = resolve_product_attribute(
        tx_df, "category_code"
    )
    brand_winners, brand_conflicts, brand_examples = resolve_product_attribute(
        tx_df, "brand"
    )

    product_ids = sorted(tx_df["product_id"].unique())
    dim = pd.DataFrame(
        {
            "ProductKey": np.arange(1, len(product_ids) + 1, dtype=np.int64),
            "product_id": product_ids,
            "category_code": [str(cat_winners.loc[pid]) for pid in product_ids],
            "brand": [str(brand_winners.loc[pid]) for pid in product_ids],
        }
    )

    conflict_report = {
        "policy": (
            "Keep exactly one row per product_id. When conflicting values exist, "
            "prefer non-'unknown' values if present, then choose the most frequent "
            "(mode) value, breaking ties deterministically by latest UTC timestamp "
            "and then alphabetical order."
        ),
        "category_conflict_count": len(cat_conflicts),
        "brand_conflict_count": len(brand_conflicts),
        "total_products_with_any_conflict": len(cat_conflicts | brand_conflicts),
        "category_conflict_examples": cat_examples,
        "brand_conflict_examples": brand_examples,
    }
    return dim, conflict_report


def build_dim_date(ts_utc_valid):
    """Build DimDate (1 row per UTC calendar date present in purchase dataset)."""
    unique_dates = pd.Series(ts_utc_valid.dt.normalize().unique()).sort_values()
    rows = []
    for dt in unique_dates:
        dow = int(dt.dayofweek) + 1  # ISO: 1=Monday .. 7=Sunday
        rows.append(
            {
                "DateKey": int(dt.strftime("%Y%m%d")),
                "full_date": dt.strftime("%Y-%m-%d"),
                "day": int(dt.day),
                "month": int(dt.month),
                "year": int(dt.year),
                "day_of_week": dow,
                "day_name": DAY_NAMES_EN[dow],
            }
        )
    return pd.DataFrame(rows)


def build_dim_time():
    """Build DimTime with exactly 24 rows (UTC hours 0..23)."""
    rows = [
        {
            "TimeKey": h,
            "hour": h,
            "time_period": f"{h:02d}:00-{h:02d}:59",
        }
        for h in range(24)
    ]
    return pd.DataFrame(rows)


def build_fact_purchase(tx_df, ts_utc, dim_customer, dim_product):
    """
    Build FactPurchase preserving the exact source purchase-event grain.
    Uses left lookups so no source row is silently dropped or duplicated.
    """
    cust_map = (
        dim_customer.drop_duplicates(subset=["customer_id"], keep="first")
        .set_index("customer_id")["CustomerKey"]
    )
    prod_map = (
        dim_product.drop_duplicates(subset=["product_id"], keep="first")
        .set_index("product_id")["ProductKey"]
    )

    customer_keys = tx_df["customer_id"].map(cust_map)
    product_keys = tx_df["product_id"].map(prod_map)

    date_keys = (
        ts_utc.dt.year * 10000 + ts_utc.dt.month * 100 + ts_utc.dt.day
    ).astype("Int64")
    time_keys = ts_utc.dt.hour.astype("Int64")

    fact = pd.DataFrame(
        {
            "PurchaseKey": np.arange(1, len(tx_df) + 1, dtype=np.int64),
            "CustomerKey": customer_keys.astype("Int64"),
            "ProductKey": product_keys.astype("Int64"),
            "DateKey": date_keys,
            "TimeKey": time_keys,
            "amount": tx_df["amount"].to_numpy(),
            "session_id": tx_df["session_id"].to_numpy(),
        }
    )
    return fact


def reconcile_customers(seg_df, cs_df, tx_df):
    """
    Reconcile customer_segments.csv vs Hadoop customerstats by customer_id,
    plus avg_amount = total_spent / num_purchases and transactions aggregation.
    """
    m = seg_df[
        ["customer_id", "num_purchases", "total_spent", "avg_amount"]
    ].merge(
        cs_df[["customer_id", "num_purchases", "total_spent", "avg_amount"]],
        on="customer_id",
        how="outer",
        suffixes=("_seg", "_cs"),
        indicator=True,
    )

    only_seg = m[m["_merge"] == "left_only"]["customer_id"].tolist()
    only_cs = m[m["_merge"] == "right_only"]["customer_id"].tolist()
    both = m[m["_merge"] == "both"].copy()

    # Frequency comparison
    freq_diff = (both["num_purchases_seg"] - both["num_purchases_cs"]).abs()
    freq_mismatches = both[freq_diff > 0]

    # Monetary comparison
    mon_diff = (both["total_spent_seg"] - both["total_spent_cs"]).abs()
    mon_exact_mismatches = both[mon_diff > 0]
    mon_tol_mismatches = both[mon_diff > PER_ROW_ROUNDING_TOL]

    # avg_amount vs customerstats
    avg_cs_diff = (both["avg_amount_seg"] - both["avg_amount_cs"]).abs()
    avg_cs_exact_mismatches = both[avg_cs_diff > 0]
    avg_cs_tol_mismatches = both[avg_cs_diff > PER_ROW_ROUNDING_TOL]

    # avg_amount == total_spent / num_purchases inside customer_segments
    computed_avg = seg_df["total_spent"] / seg_df["num_purchases"].replace(0, np.nan)
    avg_formula_diff = (seg_df["avg_amount"] - computed_avg).abs()
    avg_formula_exact_diff = seg_df[avg_formula_diff > 1e-9]
    avg_formula_tol_fail = seg_df[avg_formula_diff > PER_ROW_ROUNDING_TOL]

    # Compare transactions.csv aggregated by customer_id vs customer_segments
    tx_cust = (
        tx_df.groupby("customer_id", sort=False)
        .agg(tx_freq=("amount", "size"), tx_monetary=("amount", "sum"))
        .reset_index()
    )
    mt = seg_df[["customer_id", "num_purchases", "total_spent"]].merge(
        tx_cust, on="customer_id", how="outer", indicator=True
    )
    only_seg_vs_tx = mt[mt["_merge"] == "left_only"]["customer_id"].tolist()
    only_tx_vs_seg = mt[mt["_merge"] == "right_only"]["customer_id"].tolist()
    both_tx = mt[mt["_merge"] == "both"]
    tx_freq_mismatch = int((both_tx["num_purchases"] != both_tx["tx_freq"]).sum())
    tx_mon_diff = (both_tx["total_spent"] - both_tx["tx_monetary"]).abs()
    tx_mon_tol_mismatch = int((tx_mon_diff > PER_ROW_ROUNDING_TOL).sum())

    def _examples(df_sub, cols):
        out = []
        for _, r in df_sub.head(10).iterrows():
            row_dict = {}
            for c in cols:
                val = r[c]
                if pd.isna(val):
                    row_dict[c] = None
                elif isinstance(val, (np.integer, int)):
                    row_dict[c] = int(val)
                elif isinstance(val, (np.floating, float)):
                    row_dict[c] = float(val)
                else:
                    row_dict[c] = str(val)
            out.append(row_dict)
        return out

    return {
        "customer_segments_vs_customerstats": {
            "matched_customers": int(len(both)),
            "only_in_customer_segments_count": len(only_seg),
            "only_in_customer_segments_examples": only_seg[:10],
            "only_in_customerstats_count": len(only_cs),
            "only_in_customerstats_examples": only_cs[:10],
            "frequency_mismatch_count": int(len(freq_mismatches)),
            "frequency_max_abs_diff": int(freq_diff.max()) if len(both) else 0,
            "frequency_mismatch_examples": _examples(
                freq_mismatches,
                ["customer_id", "num_purchases_seg", "num_purchases_cs"],
            ),
            "monetary_exact_diff_count": int(len(mon_exact_mismatches)),
            "monetary_over_tolerance_count": int(len(mon_tol_mismatches)),
            "monetary_tolerance": PER_ROW_ROUNDING_TOL,
            "monetary_max_abs_diff": round(float(mon_diff.max()), 6) if len(both) else 0.0,
            "monetary_over_tolerance_examples": _examples(
                mon_tol_mismatches,
                ["customer_id", "total_spent_seg", "total_spent_cs"],
            ),
            "avg_amount_exact_diff_count": int(len(avg_cs_exact_mismatches)),
            "avg_amount_over_tolerance_count": int(len(avg_cs_tol_mismatches)),
            "avg_amount_tolerance": PER_ROW_ROUNDING_TOL,
            "avg_amount_max_abs_diff": round(float(avg_cs_diff.max()), 6) if len(both) else 0.0,
            "avg_amount_over_tolerance_examples": _examples(
                avg_cs_tol_mismatches,
                ["customer_id", "avg_amount_seg", "avg_amount_cs"],
            ),
        },
        "avg_amount_formula_check": {
            "tolerance": PER_ROW_ROUNDING_TOL,
            "rows_with_rounding_diff": int(len(avg_formula_exact_diff)),
            "rows_over_tolerance": int(len(avg_formula_tol_fail)),
            "max_abs_diff": round(float(avg_formula_diff.max()), 6) if len(seg_df) else 0.0,
            "over_tolerance_examples": _examples(
                avg_formula_tol_fail,
                ["customer_id", "num_purchases", "total_spent", "avg_amount"],
            ),
        },
        "customer_segments_vs_transactions": {
            "only_in_customer_segments_count": len(only_seg_vs_tx),
            "only_in_customer_segments_examples": only_seg_vs_tx[:10],
            "only_in_transactions_count": len(only_tx_vs_seg),
            "only_in_transactions_examples": only_tx_vs_seg[:10],
            "frequency_mismatch_count": tx_freq_mismatch,
            "monetary_over_tolerance_count": tx_mon_tol_mismatch,
            "monetary_max_abs_diff": round(float(tx_mon_diff.max()), 6) if len(both_tx) else 0.0,
        },
    }


def reconcile_products(tx_df, ps_df, conflict_report):
    """Compare transactions.csv product IDs & aggregates against Hadoop productstats."""
    tx_prod = (
        tx_df.groupby("product_id", sort=False)
        .agg(tx_purchases=("amount", "size"), tx_revenue=("amount", "sum"))
        .reset_index()
    )
    m = tx_prod.merge(
        ps_df[["product_id", "num_purchases", "revenue"]],
        on="product_id",
        how="outer",
        indicator=True,
    )
    only_tx = m[m["_merge"] == "left_only"]["product_id"].tolist()
    only_ps = m[m["_merge"] == "right_only"]["product_id"].tolist()
    both = m[m["_merge"] == "both"]

    purch_mismatch = int((both["tx_purchases"] != both["num_purchases"]).sum())
    rev_diff = (both["tx_revenue"] - both["revenue"]).abs()
    rev_over_tol = int((rev_diff > PER_ROW_ROUNDING_TOL).sum())

    return {
        "transactions_distinct_products": int(len(tx_prod)),
        "productstats_rows": int(len(ps_df)),
        "matched_products": int(len(both)),
        "in_transactions_missing_from_productstats_count": len(only_tx),
        "in_transactions_missing_from_productstats_examples": only_tx[:10],
        "in_productstats_missing_from_transactions_count": len(only_ps),
        "in_productstats_missing_from_transactions_examples": only_ps[:10],
        "purchase_count_mismatch_count": purch_mismatch,
        "revenue_over_tolerance_count": rev_over_tol,
        "revenue_max_abs_diff": round(float(rev_diff.max()), 6) if len(both) else 0.0,
        "attribute_conflicts": conflict_report,
    }


def main():
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)

    # 1. Read inputs with explicit schemas and types
    tx = pd.read_csv(
        args.transactions,
        header=None,
        names=TX_COLS,
        dtype={
            "customer_id": str,
            "product_id": str,
            "amount": float,
            "timestamp": str,
            "category_code": str,
            "brand": str,
            "session_id": str,
        },
        keep_default_na=False,
    )
    # Normalize any unexpected blank strings cleanly without dropping rows
    for col in ("category_code", "brand", "session_id"):
        tx[col] = tx[col].replace("", "unknown")

    seg = pd.read_csv(
        args.segments,
        encoding="utf-8-sig",
        dtype={"customer_id": str, "segment": str},
    )
    cs, cs_files = read_hadoop_parts(
        args.customerstats, CUSTOMERSTATS_COLS, {"customer_id": str}
    )
    ps, ps_files = read_hadoop_parts(
        args.productstats, PRODUCTSTATS_COLS, {"product_id": str}
    )

    # 2. Parse timestamps strictly in UTC
    ts_utc = pd.to_datetime(
        tx["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce", utc=True
    )
    ts_parse_failures = int(ts_utc.isna().sum())

    # 3. Build Dimension tables
    dim_customer = build_dim_customer(seg)
    dim_product, conflict_report = build_dim_product(tx)
    dim_date = build_dim_date(ts_utc.dropna())
    dim_time = build_dim_time()

    # 4. Build Fact table
    fact_purchase = build_fact_purchase(tx, ts_utc, dim_customer, dim_product)

    # 5. Dimension uniqueness checks
    dim_uniqueness = {
        "DimCustomer_CustomerKey_unique": bool(dim_customer["CustomerKey"].is_unique),
        "DimCustomer_customer_id_unique": bool(dim_customer["customer_id"].is_unique),
        "DimCustomer_duplicate_customer_id_count": int(
            dim_customer["customer_id"].duplicated().sum()
        ),
        "DimProduct_ProductKey_unique": bool(dim_product["ProductKey"].is_unique),
        "DimProduct_product_id_unique": bool(dim_product["product_id"].is_unique),
        "DimProduct_duplicate_product_id_count": int(
            dim_product["product_id"].duplicated().sum()
        ),
        "DimDate_DateKey_unique": bool(dim_date["DateKey"].is_unique),
        "DimTime_TimeKey_unique": bool(dim_time["TimeKey"].is_unique),
        "DimTime_exact_24_rows": bool(len(dim_time) == 24),
    }

    # 6. Foreign key integrity checks
    valid_ck = set(dim_customer["CustomerKey"])
    valid_pk = set(dim_product["ProductKey"])
    valid_dk = set(dim_date["DateKey"])
    valid_tk = set(dim_time["TimeKey"])

    fk_checks = {
        "CustomerKey_null_count": int(fact_purchase["CustomerKey"].isna().sum()),
        "CustomerKey_invalid_count": int(
            (~fact_purchase["CustomerKey"].dropna().isin(valid_ck)).sum()
        ),
        "ProductKey_null_count": int(fact_purchase["ProductKey"].isna().sum()),
        "ProductKey_invalid_count": int(
            (~fact_purchase["ProductKey"].dropna().isin(valid_pk)).sum()
        ),
        "DateKey_null_count": int(fact_purchase["DateKey"].isna().sum()),
        "DateKey_invalid_count": int(
            (~fact_purchase["DateKey"].dropna().isin(valid_dk)).sum()
        ),
        "TimeKey_null_count": int(fact_purchase["TimeKey"].isna().sum()),
        "TimeKey_invalid_count": int(
            (~fact_purchase["TimeKey"].dropna().isin(valid_tk)).sum()
        ),
        "fact_row_count_matches_source": bool(len(fact_purchase) == len(tx)),
    }

    # 7. Revenue reconciliation (exact Decimal sums to avoid binary64 drift)
    revenue_reconciliation = build_revenue_reconciliation(
        tx_amounts=tx["amount"],
        fact_amounts=fact_purchase["amount"],
        seg_totals=seg["total_spent"],
        cs_totals=cs["total_spent"],
        ps_totals=ps["revenue"],
        tx_count=len(tx),
        fact_count=len(fact_purchase),
        seg_purchases_sum=seg["num_purchases"].sum(),
        cs_purchases_sum=cs["num_purchases"].sum(),
        ps_purchases_sum=ps["num_purchases"].sum(),
    )
    diff_source_fact = revenue_reconciliation["diff_source_vs_fact"]
    diff_source_seg = revenue_reconciliation["diff_source_vs_customer_segments"]
    diff_source_cs = revenue_reconciliation["diff_source_vs_customerstats"]
    diff_seg_cs = revenue_reconciliation["diff_customer_segments_vs_customerstats"]

    # 8. Customer and product reconciliations
    customer_reconciliation = reconcile_customers(seg, cs, tx)
    product_reconciliation = reconcile_products(tx, ps, conflict_report)

    # 9. Determine overall validation status (PASS / WARNING / FAIL)
    critical_failures = []
    warnings = []

    if ts_parse_failures > 0:
        critical_failures.append(
            f"Timestamp parsing failed for {ts_parse_failures} rows."
        )
    for k, ok in dim_uniqueness.items():
        if isinstance(ok, bool) and not ok:
            critical_failures.append(f"Dimension check failed: {k}")
    critical_failures.extend(evaluate_fk_integrity(fk_checks))
    if not revenue_reconciliation["source_vs_fact_within_tolerance"]:
        critical_failures.append(
            f"Source vs FactPurchase revenue diff {diff_source_fact:.6f} exceeds {FACT_REVENUE_TOL}"
        )
    if not revenue_reconciliation["source_vs_customer_segments_within_tolerance"]:
        critical_failures.append(
            f"Source vs customer_segments revenue diff {diff_source_seg:.6f} exceeds {AGG_REVENUE_TOL}"
        )
    if not revenue_reconciliation["source_vs_customerstats_within_tolerance"]:
        critical_failures.append(
            f"Source vs customerstats revenue diff {diff_source_cs:.6f} exceeds {AGG_REVENUE_TOL}"
        )

    c_vs_cs = customer_reconciliation["customer_segments_vs_customerstats"]
    if c_vs_cs["only_in_customer_segments_count"] > 0:
        critical_failures.append(
            f"{c_vs_cs['only_in_customer_segments_count']} customer_ids in customer_segments missing from customerstats"
        )
    if c_vs_cs["only_in_customerstats_count"] > 0:
        critical_failures.append(
            f"{c_vs_cs['only_in_customerstats_count']} customer_ids in customerstats missing from customer_segments"
        )
    if c_vs_cs["frequency_mismatch_count"] > 0:
        critical_failures.append(
            f"{c_vs_cs['frequency_mismatch_count']} customers have frequency mismatches between customer_segments and customerstats"
        )
    if c_vs_cs["monetary_over_tolerance_count"] > 0:
        critical_failures.append(
            f"{c_vs_cs['monetary_over_tolerance_count']} customers have monetary diffs > {PER_ROW_ROUNDING_TOL}"
        )
    if c_vs_cs["avg_amount_over_tolerance_count"] > 0:
        critical_failures.append(
            f"{c_vs_cs['avg_amount_over_tolerance_count']} customers have avg_amount diffs > {PER_ROW_ROUNDING_TOL}"
        )
    if customer_reconciliation["avg_amount_formula_check"]["rows_over_tolerance"] > 0:
        critical_failures.append(
            f"{customer_reconciliation['avg_amount_formula_check']['rows_over_tolerance']} customers fail avg_amount = total_spent / num_purchases"
        )

    c_vs_tx = customer_reconciliation["customer_segments_vs_transactions"]
    if (
        c_vs_tx["only_in_customer_segments_count"] > 0
        or c_vs_tx["only_in_transactions_count"] > 0
        or c_vs_tx["frequency_mismatch_count"] > 0
        or c_vs_tx["monetary_over_tolerance_count"] > 0
    ):
        critical_failures.append(
            "Customer reconciliation against transactions.csv failed."
        )

    if (
        product_reconciliation["in_transactions_missing_from_productstats_count"] > 0
        or product_reconciliation["in_productstats_missing_from_transactions_count"] > 0
        or product_reconciliation["purchase_count_mismatch_count"] > 0
        or product_reconciliation["revenue_over_tolerance_count"] > 0
    ):
        critical_failures.append(
            "Product reconciliation against Hadoop productstats failed."
        )

    # Non-critical warnings
    if conflict_report["total_products_with_any_conflict"] > 0:
        warnings.append(
            f"{conflict_report['total_products_with_any_conflict']} products have conflicting "
            f"category_code ({conflict_report['category_conflict_count']}) or "
            f"brand ({conflict_report['brand_conflict_count']}) values in transactions.csv; "
            f"resolved using deterministic policy."
        )
    if diff_source_seg > 0 or diff_source_cs > 0 or diff_seg_cs > 0:
        warnings.append(
            f"Minor floating-point/two-decimal rounding differences detected across revenue totals "
            f"(source vs segments={diff_source_seg:.6f}, source vs customerstats={diff_source_cs:.6f}, "
            f"segments vs customerstats={diff_seg_cs:.6f}), all within tolerance."
        )
    if customer_reconciliation["avg_amount_formula_check"]["rows_with_rounding_diff"] > 0:
        warnings.append(
            f"{customer_reconciliation['avg_amount_formula_check']['rows_with_rounding_diff']} "
            f"customers have minor two-decimal rounding differences between avg_amount and "
            f"total_spent / num_purchases (max diff "
            f"{customer_reconciliation['avg_amount_formula_check']['max_abs_diff']:.6f} <= {PER_ROW_ROUNDING_TOL})."
        )

    if critical_failures:
        overall_status = "FAIL"
    elif warnings:
        overall_status = "WARNING"
    else:
        overall_status = "PASS"

    # 10. Write Star Schema CSVs (utf-8-sig for Power BI / Excel Vietnamese support)
    outputs = {
        "FactPurchase.csv": fact_purchase,
        "DimCustomer.csv": dim_customer,
        "DimProduct.csv": dim_product,
        "DimDate.csv": dim_date,
        "DimTime.csv": dim_time,
    }
    for filename, df_out in outputs.items():
        out_path = os.path.join(args.out, filename)
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")

    # 11. Write validation report JSON
    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_status": overall_status,
        "critical_failures": critical_failures,
        "warnings": warnings,
        "inputs": {
            "transactions": args.transactions,
            "customer_segments": args.segments,
            "customerstats_dir": args.customerstats,
            "customerstats_files": [os.path.basename(f) for f in cs_files],
            "productstats_dir": args.productstats,
            "productstats_files": [os.path.basename(f) for f in ps_files],
        },
        "source_row_counts": {
            "transactions": int(len(tx)),
            "customer_segments": int(len(seg)),
            "customerstats": int(len(cs)),
            "productstats": int(len(ps)),
        },
        "output_row_counts": {
            "FactPurchase": int(len(fact_purchase)),
            "DimCustomer": int(len(dim_customer)),
            "DimProduct": int(len(dim_product)),
            "DimDate": int(len(dim_date)),
            "DimTime": int(len(dim_time)),
        },
        "timestamp_validation": {
            "timezone": "UTC",
            "parse_failures": ts_parse_failures,
            "min_timestamp_utc": str(ts_utc.min()),
            "max_timestamp_utc": str(ts_utc.max()),
        },
        "dimension_uniqueness": dim_uniqueness,
        "foreign_key_integrity": fk_checks,
        "revenue_reconciliation": revenue_reconciliation,
        "customer_reconciliation": customer_reconciliation,
        "product_reconciliation": product_reconciliation,
    }

    report_path = os.path.join(args.out, "modeling_validation.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Star Schema outputs written to: {args.out}/")
    for filename, df_out in outputs.items():
        print(f"  {filename:<24} {len(df_out):>10,} rows")
    print(f"  {'modeling_validation.json':<24} status={overall_status}")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(f"  - {w}")
    if critical_failures:
        print("\nCritical Failures:")
        for c in critical_failures:
            print(f"  - {c}")
        sys.exit(1)


if __name__ == "__main__":
    main()
