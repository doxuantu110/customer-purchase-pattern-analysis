"""
Ngày 3 - Tiền xử lý REES46 (2019-Oct.csv) theo khối.

Đầu ra (không header, không có dấu phẩy trong giá trị):
  data/clean/events_clean.csv   mọi sự kiện đã làm sạch (dữ liệu cỡ GB cho MapReduce)
      timestamp,event_type,user_id,product_id,category_id,category_code,brand,price,user_session
  data/clean/transactions.csv   chỉ purchase, đã bỏ trùng toàn cục (dùng cho RFM, K-means, ground truth)
      customer_id,product_id,amount,timestamp,category_code,brand,session_id
  data/clean/sample_events.csv  ~10.000 dòng events để test nhanh
  data/clean/sample.csv         ~1.000 dòng transactions để test nhanh
  docs/cleaning_stats.md        bảng thống kê trước/sau làm sạch

Chạy từ gốc repo:
  python preprocess/02_preprocess.py data/sample_1000000.csv   # thử trên mẫu
  python preprocess/02_preprocess.py                           # toàn bộ 2019-Oct.csv
"""
import os
import sys
import time
from collections import Counter

import pandas as pd

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/2019-Oct.csv"
OUT_DIR = "data/clean"
DOCS = "docs"
CHUNK = 1_000_000
COLS = ["event_time", "event_type", "product_id", "category_id",
        "category_code", "brand", "price", "user_id", "user_session"]
EVENT_ORDER = ["timestamp", "event_type", "user_id", "product_id", "category_id",
               "category_code", "brand", "price", "user_session"]
VALID_EVENTS = {"view", "cart", "purchase"}

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(DOCS, exist_ok=True)

events_path = os.path.join(OUT_DIR, "events_clean.csv")
if os.path.exists(events_path):
    os.remove(events_path)

stats = Counter()             # tổng theo lý do
by_type_in = Counter()        # số dòng đầu vào theo event_type
by_type_out = Counter()       # số dòng đầu ra theo event_type
price_le0_by_type = Counter()
purchases = []
sample_events = None
t0 = time.time()

for i, chunk in enumerate(pd.read_csv(SRC, usecols=COLS, dtype=str, chunksize=CHUNK)):
    stats["rows_in"] += len(chunk)
    by_type_in.update(chunk["event_type"].fillna("(missing)").value_counts().to_dict())

    # chuẩn hóa khoảng trắng
    for c in COLS:
        chunk[c] = chunk[c].str.strip()
    chunk = chunk.replace("", pd.NA)

    # 1. thiếu trường khóa
    key = chunk[["event_time", "event_type", "user_id", "product_id", "price"]].isna().any(axis=1)
    stats["missing_key"] += int(key.sum())
    chunk = chunk[~key]

    # 2. event_type lạ
    bad_ev = ~chunk["event_type"].isin(VALID_EVENTS)
    stats["invalid_event_type"] += int(bad_ev.sum())
    chunk = chunk[~bad_ev]

    # 3. giá
    price = pd.to_numeric(chunk["price"], errors="coerce")
    bad_price = price.isna()
    stats["price_not_numeric"] += int(bad_price.sum())
    chunk, price = chunk[~bad_price].copy(), price[~bad_price]
    le0 = price <= 0
    stats["price_le_0"] += int(le0.sum())
    price_le0_by_type.update(chunk.loc[le0, "event_type"].value_counts().to_dict())
    chunk, price = chunk[~le0].copy(), price[~le0]
    chunk["price"] = price.round(2).map("{:.2f}".format)

    # 4. thời gian: "2019-10-01 00:00:04 UTC" -> "2019-10-01 00:00:04"
    ts = pd.to_datetime(chunk["event_time"].str.replace(" UTC", "", regex=False),
                        errors="coerce")
    bad_ts = ts.isna()
    stats["bad_timestamp"] += int(bad_ts.sum())
    chunk = chunk[~bad_ts].copy()
    chunk["timestamp"] = ts[~bad_ts].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 5. điền thiếu, đồng nhất định dạng, bỏ dấu phẩy
    for c in ("category_code", "brand"):
        stats[f"{c}_missing_filled"] += int(chunk[c].isna().sum())
        chunk[c] = (chunk[c].fillna("unknown").str.lower()
                    .str.replace(",", " ", regex=False))
    chunk["category_id"] = chunk["category_id"].fillna("unknown")
    chunk["user_session"] = chunk["user_session"].fillna("unknown")

    # 6. trùng lặp trong khối
    chunk = chunk[EVENT_ORDER]
    before = len(chunk)
    chunk = chunk.drop_duplicates()
    stats["duplicate_in_chunk"] += before - len(chunk)

    by_type_out.update(chunk["event_type"].value_counts().to_dict())
    stats["events_out"] += len(chunk)
    chunk.to_csv(events_path, mode="a", header=False, index=False)

    if sample_events is None:
        sample_events = chunk.head(10_000)
    purchases.append(chunk[chunk["event_type"] == "purchase"])
    print(f"khối {i+1}: {stats['rows_in']:,} dòng đã đọc, {stats['events_out']:,} giữ lại "
          f"({time.time()-t0:.0f}s)", flush=True)

# ---- transactions: chỉ purchase, bỏ trùng toàn cục ----
tx = pd.concat(purchases, ignore_index=True)
before = len(tx)
tx = tx.drop_duplicates()
stats["duplicate_purchase_global"] = before - len(tx)
tx = tx.sort_values("timestamp", kind="stable")
tx_out = tx.rename(columns={"user_id": "customer_id", "price": "amount",
                            "user_session": "session_id"})[
    ["customer_id", "product_id", "amount", "timestamp", "category_code", "brand", "session_id"]]
tx_out.to_csv(os.path.join(OUT_DIR, "transactions.csv"), header=False, index=False)
tx_out.sample(n=min(1000, len(tx_out)), random_state=42).sort_values("timestamp") \
    .to_csv(os.path.join(OUT_DIR, "sample.csv"), header=False, index=False)
sample_events.to_csv(os.path.join(OUT_DIR, "sample_events.csv"), header=False, index=False)

# ---- báo cáo ----
rows_in = stats["rows_in"]
lines = ["# Thống kê làm sạch (Ngày 3)", "", f"Nguồn: `{SRC}`", "",
         "| Bước | Số dòng | % so với đầu vào |", "| --- | ---: | ---: |",
         f"| Dòng đầu vào | {rows_in:,} | 100% |"]
for k, label in [("missing_key", "Loại: thiếu user_id/product_id/event_time/event_type/price"),
                 ("invalid_event_type", "Loại: event_type không hợp lệ"),
                 ("price_not_numeric", "Loại: price không phải số"),
                 ("price_le_0", "Loại: price <= 0"),
                 ("bad_timestamp", "Loại: thời gian không đọc được"),
                 ("duplicate_in_chunk", "Loại: trùng lặp (trong từng khối 1 triệu dòng)")]:
    lines.append(f"| {label} | {stats[k]:,} | {stats[k]/rows_in:.4%} |")
lines.append(f"| **events_clean.csv** | **{stats['events_out']:,}** | {stats['events_out']/rows_in:.2%} |")
lines += ["", "Giá trị thiếu được điền `unknown` (không loại dòng):", "",
          f"- category_code: {stats['category_code_missing_filled']:,} dòng "
          f"({stats['category_code_missing_filled']/rows_in:.2%})",
          f"- brand: {stats['brand_missing_filled']:,} dòng ({stats['brand_missing_filled']/rows_in:.2%})",
          "", "price <= 0 theo loại sự kiện: " + (", ".join(f"{k}={v:,}" for k, v in price_le0_by_type.items()) or "không có"),
          "", "## Theo loại sự kiện", "", "| event_type | Trước | Sau |", "| --- | ---: | ---: |"]
for et in ["view", "cart", "purchase"]:
    lines.append(f"| {et} | {by_type_in.get(et,0):,} | {by_type_out.get(et,0):,} |")
lines += ["", "## transactions.csv (chỉ purchase)", "",
          f"- Purchase sau làm sạch trong events: {before:,}",
          f"- Trùng lặp toàn cục bị loại: {stats['duplicate_purchase_global']:,}",
          f"- **transactions.csv: {len(tx_out):,} dòng**, {tx_out['customer_id'].nunique():,} khách, "
          f"{tx_out['product_id'].nunique():,} sản phẩm",
          f"- Khoảng thời gian: {tx_out['timestamp'].min()} đến {tx_out['timestamp'].max()}"]
with open(os.path.join(DOCS, "cleaning_stats.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print("Xong. Báo cáo: docs/cleaning_stats.md")
