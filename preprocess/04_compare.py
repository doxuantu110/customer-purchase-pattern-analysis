"""
Ngày 5 - So sánh output job customerstats với số liệu đối chiếu.

  python preprocess/04_compare.py <thư mục output MapReduce> <ground_truth.csv | events csv không header>

Tham số thứ hai:
  - data/clean/ground_truth.csv  (toàn bộ dữ liệu)
  - data/clean/sample_events.csv (tính lại bằng pandas từ chính file events đó, cho bản mẫu)
"""
import glob
import os
import sys

import pandas as pd

if len(sys.argv) != 3:
    sys.exit(__doc__)
mr_dir, ref_path = sys.argv[1], sys.argv[2]

cols = ["customer_id", "total_spent", "num_purchases", "avg_amount", "first_purchase", "last_purchase"]
parts = sorted(glob.glob(os.path.join(mr_dir, "part-*")))
if not parts:
    sys.exit(f"Không thấy file part-* trong {mr_dir}")
mr = pd.concat([pd.read_csv(f, sep="\t", header=None, names=cols, dtype={"customer_id": str})
                for f in parts], ignore_index=True)

if os.path.basename(ref_path) == "ground_truth.csv":
    ref = pd.read_csv(ref_path, dtype={"customer_id": str})
else:
    ev = pd.read_csv(ref_path, header=None, dtype=str,
                     names=["timestamp", "event_type", "user_id", "product_id", "category_id",
                            "category_code", "brand", "price", "user_session"])
    ev = ev[ev["event_type"] == "purchase"].copy()
    ev["price"] = ev["price"].astype(float)
    ref = ev.groupby("user_id").agg(total_spent=("price", "sum"), num_purchases=("price", "count"),
                                    avg_amount=("price", "mean"), first_purchase=("timestamp", "min"),
                                    last_purchase=("timestamp", "max")).reset_index() \
            .rename(columns={"user_id": "customer_id"})
    ref[["total_spent", "avg_amount"]] = ref[["total_spent", "avg_amount"]].round(2)

m = mr.merge(ref, on="customer_id", how="outer", suffixes=("_mr", "_ref"), indicator=True)
only_mr = int((m["_merge"] == "left_only").sum())
only_ref = int((m["_merge"] == "right_only").sum())
both = m[m["_merge"] == "both"]

bad_count = int((both["num_purchases_mr"] != both["num_purchases_ref"]).sum())
bad_first = int((both["first_purchase_mr"] != both["first_purchase_ref"]).sum())
bad_last = int((both["last_purchase_mr"] != both["last_purchase_ref"]).sum())
d_total = float((both["total_spent_mr"] - both["total_spent_ref"]).abs().max()) if len(both) else 0.0
d_avg = float((both["avg_amount_mr"] - both["avg_amount_ref"]).abs().max()) if len(both) else 0.0

print(f"Dòng MapReduce: {len(mr):,} | dòng đối chiếu: {len(ref):,}")
print(f"Chỉ có ở MapReduce: {only_mr:,} | chỉ có ở đối chiếu: {only_ref:,}")
print(f"Sai num_purchases: {bad_count:,} | sai first: {bad_first:,} | sai last: {bad_last:,}")
print(f"Lệch lớn nhất total_spent: {d_total:.4f} | avg_amount: {d_avg:.4f} (cho phép <= 0.011 do làm tròn)")
ok = (only_mr == only_ref == bad_count == bad_first == bad_last == 0 and d_total <= 0.011 and d_avg <= 0.011)
print("KẾT QUẢ:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
