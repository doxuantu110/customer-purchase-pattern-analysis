"""EDA cho dữ liệu REES46 (clickstream, nhiều GB) bằng cách đọc theo khối.

Chạy từ gốc repo:
    python preprocess/01_eda_chunked.py                      # toàn bộ data/2019-Oct.csv
    python preprocess/01_eda_chunked.py data/sample_1000000.csv   # chạy thử trên mẫu
Đầu ra: docs/eda_report.md
Bộ nhớ dùng: vài trăm MB (chỉ đọc cột cần thiết, mỗi khối 1 triệu dòng).
"""
import sys
import time
from collections import Counter
from pathlib import Path

import pandas as pd

SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/2019-Oct.csv")
CHUNK = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000
OUT = Path("docs/eda_report.md")

if not SRC.exists():
    raise SystemExit(f"Không thấy {SRC}")

COLS = ["event_time", "event_type", "product_id", "category_code",
        "brand", "price", "user_id", "user_session"]

rows = 0
events = Counter()
missing = Counter()
users, buyers = set(), set()
products_bought = set()
sessions_bought = set()
dup_in_chunk = 0
bad_price = 0                       # purchase có price <= 0
price_sum, price_min, price_max, n_purchase = 0.0, float("inf"), float("-inf"), 0
tmin, tmax = None, None
daily_events, daily_purchases = Counter(), Counter()
cat_purchases = Counter()
brand_purchases = Counter()

t0 = time.time()
reader = pd.read_csv(SRC, usecols=COLS, chunksize=CHUNK,
                     dtype={"event_type": "category", "category_code": "string",
                            "brand": "string", "user_session": "string"})
for i, ch in enumerate(reader, 1):
    rows += len(ch)
    for c in COLS:
        missing[c] += int(ch[c].isna().sum())
    events.update(ch["event_type"].value_counts().to_dict())
    dup_in_chunk += int(ch.duplicated().sum())

    t = pd.to_datetime(ch["event_time"], utc=True, errors="coerce")
    if t.notna().any():
        lo, hi = t.min(), t.max()
        tmin = lo if tmin is None or lo < tmin else tmin
        tmax = hi if tmax is None or hi > tmax else tmax
    day = t.dt.strftime("%Y-%m-%d")
    daily_events.update(day.value_counts().to_dict())

    users.update(ch["user_id"].dropna().unique().tolist())

    p = ch[ch["event_type"] == "purchase"]
    if len(p):
        buyers.update(p["user_id"].dropna().unique().tolist())
        products_bought.update(p["product_id"].dropna().unique().tolist())
        sessions_bought.update(p["user_session"].dropna().unique().tolist())
        daily_purchases.update(day[p.index].value_counts().to_dict())
        cat_purchases.update(p["category_code"].dropna().value_counts().to_dict())
        brand_purchases.update(p["brand"].dropna().value_counts().to_dict())
        pr = p["price"].dropna()
        bad_price += int((pr <= 0).sum())
        price_sum += float(pr.sum()); n_purchase += len(pr)
        price_min = min(price_min, float(pr.min())); price_max = max(price_max, float(pr.max()))
    print(f"  khối {i}: tổng {rows:,} dòng ({time.time() - t0:.0f}s)", flush=True)

pct = lambda x: f"{x / rows * 100:.2f}%" if rows else "-"
L = []; add = L.append
add(f"# Báo cáo EDA - REES46 ({SRC.name})\n")
add("## 1. Tổng quan")
add(f"- Số dòng: **{rows:,}** | Kích thước file: **{SRC.stat().st_size / 1e9:.2f} GB**")
add(f"- Khoảng thời gian: {tmin} -> {tmax}")
add(f"- Số user_id: **{len(users):,}** | Số user có mua: **{len(buyers):,}**")
add(f"- Số sản phẩm đã được mua: **{len(products_bought):,}** | Số session có mua: **{len(sessions_bought):,}**\n")

add("## 2. Phân bố loại sự kiện")
add("| event_type | Số dòng | Tỷ lệ |"); add("|---|---:|---:|")
for k, v in events.most_common():
    add(f"| {k} | {v:,} | {pct(v)} |")
add("")

add("## 3. Vấn đề chất lượng dữ liệu (xử lý ở Ngày 3)")
add("| Vấn đề | Số dòng | Tỷ lệ |"); add("|---|---:|---:|")
for c in COLS:
    add(f"| Thiếu `{c}` | {missing[c]:,} | {pct(missing[c])} |")
add(f"| Trùng lặp (chỉ đếm trong từng khối, ước lượng) | {dup_in_chunk:,} | {pct(dup_in_chunk)} |")
add(f"| Sự kiện purchase có price <= 0 | {bad_price:,} | - |")
add("")

add("## 4. Thống kê giá của sự kiện purchase")
if n_purchase:
    add(f"- Số purchase: {n_purchase:,}; giá nhỏ nhất {price_min:.2f}, lớn nhất {price_max:.2f}, "
        f"trung bình {price_sum / n_purchase:.2f}; tổng doanh thu {price_sum:,.0f}\n")

add("## 5. Top 10 category_code theo số lượt mua")
add("| category_code | Lượt mua |"); add("|---|---:|")
for k, v in cat_purchases.most_common(10):
    add(f"| {k} | {v:,} |")
add("")
add("## 6. Top 10 thương hiệu theo số lượt mua")
add("| brand | Lượt mua |"); add("|---|---:|")
for k, v in brand_purchases.most_common(10):
    add(f"| {k} | {v:,} |")
add("")

add("## 7. Số sự kiện và số lượt mua theo ngày")
add("| Ngày | Sự kiện | Lượt mua |"); add("|---|---:|---:|")
for d in sorted(daily_events):
    add(f"| {d} | {daily_events[d]:,} | {daily_purchases.get(d, 0):,} |")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(L), encoding="utf-8")
print("\n".join(L[:30]))
print(f"\n>>> Đã ghi {OUT} (mất {time.time() - t0:.0f}s)")
