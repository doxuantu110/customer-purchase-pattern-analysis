"""
Ngày 3 - Tạo số liệu đối chiếu (ground truth) từ file đã làm sạch.

  data/clean/ground_truth.csv              theo khách: customer_id,total_spent,num_purchases,avg_amount,first_purchase,last_purchase
  data/clean/ground_truth_event_counts.csv theo event_type và ngày: số sự kiện (đối chiếu job đếm trên events_clean.csv)

Chạy từ gốc repo: python preprocess/03_ground_truth.py
"""
import pandas as pd

TX = "data/clean/transactions.csv"
EV = "data/clean/events_clean.csv"

tx = pd.read_csv(TX, header=None, dtype={0: str, 1: str, 6: str},
                 names=["customer_id", "product_id", "amount", "timestamp",
                        "category_code", "brand", "session_id"])
gt = tx.groupby("customer_id").agg(
    total_spent=("amount", "sum"),
    num_purchases=("amount", "count"),
    avg_amount=("amount", "mean"),
    first_purchase=("timestamp", "min"),
    last_purchase=("timestamp", "max"),
).reset_index()
gt[["total_spent", "avg_amount"]] = gt[["total_spent", "avg_amount"]].round(2)
gt.to_csv("data/clean/ground_truth.csv", index=False)
print(f"ground_truth.csv: {len(gt):,} khách, tổng chi tiêu {gt['total_spent'].sum():,.2f}")

counts = {}
for chunk in pd.read_csv(EV, header=None, usecols=[0, 1], names=["timestamp", "event_type"],
                         chunksize=2_000_000):
    key = list(zip(chunk["timestamp"].str[:10], chunk["event_type"]))
    for k, v in pd.Series(key).value_counts().items():
        counts[k] = counts.get(k, 0) + v
out = pd.DataFrame([(d, e, n) for (d, e), n in sorted(counts.items())],
                   columns=["date", "event_type", "count"])
out.to_csv("data/clean/ground_truth_event_counts.csv", index=False)
print(f"ground_truth_event_counts.csv: tổng {out['count'].sum():,} sự kiện")
