#!/usr/bin/env python3
"""Tạo các bảng tổng hợp luồng từ các lô đã xử lý, để Power BI đọc từ Blob.

Chạy từ gốc repo (hook gọi tự động sau mỗi lô):
    python stream_aggregates.py batch_0003
Đầu vào : data/stream/incoming/batch_*.csv (9 cột như events_clean.csv, không tiêu đề), tính đến lô đã cho
Đầu ra  : data/stream/export/
    stream_timepattern.csv  dow, dow_label, hour, transactions, revenue   (đủ 7 x 24 ô)
    stream_products.csv     product_id, category_code, brand, transactions, revenue
    stream_meta.csv         last_batch, purchases, updated_utc
    state.csv               bản sao trạng thái cộng dồn theo khách (nếu có)
"""
import argparse
import csv
import glob
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone

ap = argparse.ArgumentParser()
ap.add_argument("batch", nargs="?", default=os.environ.get("BATCH", ""))
ap.add_argument("--tsv", help="đọc kết quả job Hadoop Streaming (insights.tsv) thay vì tự tính từ các lô")
ap.add_argument("--out", default=os.path.join("data/stream", "export"))
args = ap.parse_args()
BASE = "data/stream"
upto = args.batch
out = args.out
os.makedirs(out, exist_ok=True)

files = sorted(glob.glob(os.path.join(BASE, "incoming", "batch_*.csv")))
if upto:
    files = [f for f in files if os.path.splitext(os.path.basename(f))[0] <= upto]

LABELS = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
tp = defaultdict(lambda: [0, 0.0])
pr = defaultdict(lambda: [0, 0.0])
n = 0
if args.tsv:
    # H|dow|hour \t count \t revenue   hoặc   P|pid|cat|brand \t count \t revenue
    with open(args.tsv, encoding="utf-8") as fh:
        for line in fh:
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) != 3:
                continue
            key, c, s = parts[0].split("|"), int(parts[1]), float(parts[2])
            if key[0] == "H":
                tp[(int(key[1]), int(key[2]))] = [c, s]
                n += c
            elif key[0] == "P":
                pr[(key[1], key[2], key[3])] = [c, s]
    files = []
for f in files:
    with open(f, newline="", encoding="utf-8") as fh:
        for r in csv.reader(fh):
            # 0 timestamp, 1 event_type, 2 user_id, 3 product_id, 4 category_id, 5 category_code, 6 brand, 7 price
            if len(r) < 9 or r[1] != "purchase":
                continue
            try:
                t = datetime.strptime(r[0][:19], "%Y-%m-%d %H:%M:%S")
                price = float(r[7])
            except ValueError:
                continue
            k = (t.isoweekday(), t.hour)
            tp[k][0] += 1
            tp[k][1] += price
            k2 = (r[3], r[5] or "unknown", r[6] or "unknown")
            pr[k2][0] += 1
            pr[k2][1] += price
            n += 1

with open(os.path.join(out, "stream_timepattern.csv"), "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["dow", "dow_label", "hour", "transactions", "revenue"])
    for d in range(1, 8):
        for h in range(24):
            c, s = tp.get((d, h), (0, 0.0))
            w.writerow([d, LABELS[d - 1], h, c, round(s, 2)])

with open(os.path.join(out, "stream_products.csv"), "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["product_id", "category_code", "brand", "transactions", "revenue"])
    for (pid, cat, brand), (c, s) in sorted(pr.items(), key=lambda kv: -kv[1][1]):
        w.writerow([pid, cat, brand, c, round(s, 2)])

with open(os.path.join(out, "stream_meta.csv"), "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(["last_batch", "purchases", "updated_utc"])
    w.writerow([upto or (os.path.splitext(os.path.basename(files[-1]))[0] if files else ""), n,
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")])

state = os.path.join(BASE, "state.csv")
if os.path.exists(state):
    shutil.copy(state, os.path.join(out, "state.csv"))
src = "Hadoop Streaming" if args.tsv else "Python (dự phòng)"
print(f"[ETL] insights ({src}): {n} lượt mua, {len(pr)} sản phẩm")
if tp:
    (d, h), (c, s) = max(tp.items(), key=lambda kv: kv[1][0])
    print(f"[ETL]   giờ cao điểm: {LABELS[d - 1]} {h}h ({c} lượt, {s:,.2f})")
for (pid, cat, brand), (c, s) in sorted(pr.items(), key=lambda kv: -kv[1][1])[:3]:
    print(f"[ETL]   top sản phẩm {pid} ({brand}, {cat}): {c} lượt, {s:,.2f}")
