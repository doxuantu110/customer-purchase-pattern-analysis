"""
Ngày 7 - So sánh trực tiếp output của hai job (ví dụ Java và Streaming), cả hai cùng định dạng 6 cột.

  python preprocess/06_compare_outputs.py data/local_out/yarn_customer_r2 data/local_out/stream_customer
"""
import glob
import os
import sys

import pandas as pd

if len(sys.argv) != 3:
    sys.exit(__doc__)
cols = ["customer_id", "total_spent", "num_purchases", "avg_amount", "first_purchase", "last_purchase"]


def load(d):
    files = sorted(glob.glob(os.path.join(d, "part-*")))
    if not files:
        sys.exit(f"Không thấy file part-* trong {d}")
    return pd.concat([pd.read_csv(f, sep="\t", header=None, names=cols, dtype={"customer_id": str})
                      for f in files], ignore_index=True)


a, b = load(sys.argv[1]), load(sys.argv[2])
m = a.merge(b, on="customer_id", how="outer", suffixes=("_a", "_b"), indicator=True)
both = m[m["_merge"] == "both"]
exact = int(((both["total_spent_a"] == both["total_spent_b"]) & (both["avg_amount_a"] == both["avg_amount_b"])
             & (both["num_purchases_a"] == both["num_purchases_b"])
             & (both["first_purchase_a"] == both["first_purchase_b"])
             & (both["last_purchase_a"] == both["last_purchase_b"])).sum())
print(f"Dòng A: {len(a):,} | dòng B: {len(b):,} | chỉ có ở A: {int((m['_merge']=='left_only').sum()):,} "
      f"| chỉ có ở B: {int((m['_merge']=='right_only').sum()):,}")
print(f"Giống hệt từng ký tự: {exact:,}/{len(both):,} khách")
print(f"Lệch lớn nhất total_spent: {(both['total_spent_a']-both['total_spent_b']).abs().max():.4f} | "
      f"avg_amount: {(both['avg_amount_a']-both['avg_amount_b']).abs().max():.4f}")
bad = int(((both["num_purchases_a"] != both["num_purchases_b"]) | (both["first_purchase_a"] != both["first_purchase_b"])
           | (both["last_purchase_a"] != both["last_purchase_b"])).sum())
print(f"Sai num_purchases/first/last: {bad:,}")
ok = (len(a) == len(b) == len(both) and bad == 0
      and (both["total_spent_a"] - both["total_spent_b"]).abs().max() <= 0.011
      and (both["avg_amount_a"] - both["avg_amount_b"]).abs().max() <= 0.011)
print("KẾT QUẢ:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
