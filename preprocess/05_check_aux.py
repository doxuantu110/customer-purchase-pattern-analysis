"""
Ngày 5 - Kiểm tra nhanh hai job phụ.

  python preprocess/05_check_aux.py <output timepattern> <output productstats> data/clean/ground_truth.csv

Kiểm tra: tổng số giao dịch của hai job bằng tổng num_purchases ở ground truth,
tổng doanh thu theo sản phẩm bằng tổng total_spent ở ground truth.
"""
import glob
import os
import sys

import pandas as pd

if len(sys.argv) != 4:
    sys.exit(__doc__)
tp_dir, pr_dir, gt_path = sys.argv[1:]


def read(d, names, dtypes=None):
    files = sorted(glob.glob(os.path.join(d, "part-*")))
    if not files:
        sys.exit(f"Không thấy file part-* trong {d}")
    return pd.concat([pd.read_csv(f, sep="\t", header=None, names=names, dtype=dtypes) for f in files],
                     ignore_index=True)


tp = read(tp_dir, ["dow", "hour", "count"])
pr = read(pr_dir, ["product_id", "revenue", "num_purchases", "avg_price", "first_sale", "last_sale"],
          {"product_id": str})
gt = pd.read_csv(gt_path, dtype={"customer_id": str})

print(f"timepattern: {len(tp)} ô (tối đa 168), tổng giao dịch {tp['count'].sum():,}")
print(f"productstats: {len(pr):,} sản phẩm, tổng lượt mua {pr['num_purchases'].sum():,}")
print(f"ground truth: tổng lượt mua {gt['num_purchases'].sum():,}")
d_rev = abs(pr["revenue"].sum() - gt["total_spent"].sum())
print(f"Lệch tổng doanh thu (sản phẩm so với khách): {d_rev:.2f}")
ok = (tp["count"].sum() == gt["num_purchases"].sum() == pr["num_purchases"].sum()
      and len(tp) <= 168 and d_rev <= max(1.0, 0.0001 * gt["total_spent"].sum()))
print("KẾT QUẢ:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
