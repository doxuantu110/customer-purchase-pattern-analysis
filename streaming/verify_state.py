"""
Ngày 8 - Kiểm tra trạng thái cộng dồn bằng cách tính lại từ chính các file lô đã xử lý.

  python streaming/verify_state.py            (mặc định data/stream)
"""
import glob
import os
import sys

import pandas as pd

import merge_state

base = sys.argv[1] if len(sys.argv) > 1 else "data/stream"
state_path = os.path.join(base, "state.csv")
processed = merge_state.load_processed(state_path)
files = [p for p in sorted(glob.glob(os.path.join(base, "incoming", "batch_*.csv")))
         if os.path.splitext(os.path.basename(p))[0] in processed]
if not files:
    sys.exit("Chưa có lô nào được xử lý.")
cols = ["timestamp", "event_type", "user_id", "product_id", "category_id", "category_code", "brand", "price", "session"]
ev = pd.concat([pd.read_csv(p, header=None, names=cols, dtype={"user_id": str}) for p in files], ignore_index=True)
ref = ev.groupby("user_id").agg(total=("price", "sum"), n=("price", "count"), first=("timestamp", "min"),
                                last=("timestamp", "max")).reset_index()
st = pd.read_csv(state_path, dtype={"customer_id": str})
m = ref.merge(st, left_on="user_id", right_on="customer_id", how="outer", indicator=True)
both = m[m["_merge"] == "both"]
only = int((m["_merge"] != "both").sum())
bad_n = int((both["n"] != both["num_purchases"]).sum())
bad_t = int(((both["first"] != both["first_purchase"]) | (both["last"] != both["last_purchase"])).sum())
d = float((both["total"] - both["total_spent"]).abs().max()) if len(both) else 0.0
print("%d lô, %d giao dịch, %d khách (tính lại) | %d khách (trạng thái)" % (len(files), len(ev), len(ref), len(st)))
print("Khách thiếu/thừa: %d | sai số lượt: %d | sai thời gian: %d | lệch tổng lớn nhất: %.4f" % (only, bad_n, bad_t, d))
ok = only == 0 and bad_n == 0 and bad_t == 0 and d <= 0.011
print("KẾT QUẢ:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
