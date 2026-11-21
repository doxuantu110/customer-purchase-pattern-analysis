"""
Ngày 9 - Đặc trưng RFM từ output MapReduce và phân cụm K-means.

Bước 1 (chọn K):   python analysis/09_rfm_kmeans.py
Bước 2 (chốt K):   python analysis/09_rfm_kmeans.py --k 4

Đầu vào mặc định: data/local_out/yarn_customer_r2 (thư mục part-r-*, 6 cột TAB) hoặc một file CSV có header
(ví dụ data/clean/ground_truth.csv).
Đầu ra:
  docs/img/day9_chon_k.png            biểu đồ Elbow + Silhouette (bước 1)
  output/k_selection.csv              số liệu inertia/silhouette/Davies-Bouldin theo K (bước 1)
  output/customer_clusters.csv        customer_id,recency_days,frequency,monetary,cluster (bước 2)
  output/cluster_profile.csv          trung bình/trung vị R,F,M và số khách mỗi cụm (bước 2)
  models/kmeans_rfm.joblib            scaler + KMeans + ngày tham chiếu + ánh xạ nhãn (bước 2)
"""
import argparse
import glob
import os

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import davies_bouldin_score, silhouette_score
from sklearn.preprocessing import StandardScaler

ap = argparse.ArgumentParser()
ap.add_argument("--input", default="data/local_out/yarn_customer_r2")
ap.add_argument("--k", type=int, default=None, help="số cụm để chốt; bỏ trống = chạy phân tích chọn K")
ap.add_argument("--kmin", type=int, default=2)
ap.add_argument("--kmax", type=int, default=10)
ap.add_argument("--sample", type=int, default=20000, help="cỡ mẫu cho silhouette (tính đủ 347k điểm rất chậm)")
ap.add_argument("--seed", type=int, default=42)
args = ap.parse_args()

COLS = ["customer_id", "total_spent", "num_purchases", "avg_amount", "first_purchase", "last_purchase"]


def load(path):
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "part-*")))
        if not files:
            raise SystemExit("Không thấy file part-* trong " + path)
        return pd.concat([pd.read_csv(f, sep="\t", header=None, names=COLS, dtype={"customer_id": str})
                          for f in files], ignore_index=True)
    return pd.read_csv(path, dtype={"customer_id": str})


df = load(args.input)
df["last_purchase"] = pd.to_datetime(df["last_purchase"])
ref_date = df["last_purchase"].max().normalize() + pd.Timedelta(days=1)       # ngày cuối dataset + 1
df["recency_days"] = (ref_date - df["last_purchase"].dt.normalize()).dt.days   # >= 1
rfm = df.rename(columns={"num_purchases": "frequency", "total_spent": "monetary"})[
    ["customer_id", "recency_days", "frequency", "monetary"]].copy()
print("Khách: %d | ngày tham chiếu: %s" % (len(rfm), ref_date.date()))
print(rfm[["recency_days", "frequency", "monetary"]].describe(percentiles=[.5, .9, .99]).round(2).to_string())
print("Tỷ lệ khách mua đúng 1 lần: %.1f%%" % (100 * (rfm["frequency"] == 1).mean()))

# log1p để giảm lệch phân phối rồi chuẩn hóa
X_raw = np.log1p(rfm[["recency_days", "frequency", "monetary"]].to_numpy(dtype=float))
scaler = StandardScaler().fit(X_raw)
X = scaler.transform(X_raw)

if args.k is None:
    os.makedirs("docs/img", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    rows = []
    for k in range(args.kmin, args.kmax + 1):
        km = KMeans(n_clusters=k, n_init=10, random_state=args.seed).fit(X)
        sil = silhouette_score(X, km.labels_, sample_size=min(args.sample, len(X)), random_state=args.seed)
        dbi = davies_bouldin_score(X, km.labels_)
        rows.append({"k": k, "inertia": km.inertia_, "silhouette": sil, "davies_bouldin": dbi})
        print("K=%2d  inertia=%12.1f  silhouette=%.4f  davies_bouldin=%.4f" % (k, km.inertia_, sil, dbi), flush=True)
    res = pd.DataFrame(rows)
    res.to_csv("output/k_selection.csv", index=False)

    cand = res[(res["k"] >= 3) & (res["k"] <= 6)]
    best = int(cand.loc[cand["silhouette"].idxmax(), "k"])
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    ax[0].plot(res["k"], res["inertia"], "o-")
    ax[0].set(title="Elbow (inertia)", xlabel="K", ylabel="inertia")
    ax[1].plot(res["k"], res["silhouette"], "o-", color="tab:green")
    ax[1].axvline(best, ls="--", color="gray")
    ax[1].set(title="Silhouette (mẫu %d điểm)" % min(args.sample, len(X)), xlabel="K", ylabel="silhouette")
    fig.tight_layout()
    fig.savefig("docs/img/day9_chon_k.png", dpi=150)
    print("\nSilhouette cao nhất trong K=3..6 là K=%d. Xem docs/img/day9_chon_k.png (điểm gấp khúc của Elbow) rồi chạy:" % best)
    print("  python analysis/09_rfm_kmeans.py --k <số bạn chọn>")
else:
    km = KMeans(n_clusters=args.k, n_init=20, random_state=args.seed).fit(X)
    # đánh số lại nhãn theo trung bình Monetary giảm dần: cụm 0 = chi tiêu cao nhất
    order = rfm.assign(raw=km.labels_).groupby("raw")["monetary"].mean().sort_values(ascending=False).index
    label_map = {int(old): new for new, old in enumerate(order)}
    rfm["cluster"] = [label_map[int(l)] for l in km.labels_]

    os.makedirs("output", exist_ok=True)
    os.makedirs("models", exist_ok=True)
    rfm.to_csv("output/customer_clusters.csv", index=False)
    prof = rfm.groupby("cluster").agg(
        customers=("customer_id", "count"),
        recency_mean=("recency_days", "mean"), recency_median=("recency_days", "median"),
        frequency_mean=("frequency", "mean"), frequency_median=("frequency", "median"),
        monetary_mean=("monetary", "mean"), monetary_median=("monetary", "median"),
        monetary_total=("monetary", "sum")).reset_index()
    prof["pct_customers"] = 100 * prof["customers"] / len(rfm)
    prof["pct_revenue"] = 100 * prof["monetary_total"] / prof["monetary_total"].sum()
    prof.round(2).to_csv("output/cluster_profile.csv", index=False)
    joblib.dump({"scaler": scaler, "kmeans": km, "ref_date": ref_date, "label_map": label_map,
                 "features": ["recency_days", "frequency", "monetary"], "transform": "log1p+StandardScaler",
                 "k": args.k, "seed": args.seed}, "models/kmeans_rfm.joblib")
    sil = silhouette_score(X, km.labels_, sample_size=min(args.sample, len(X)), random_state=args.seed)
    print("\nK=%d | silhouette (mẫu) = %.4f | inertia = %.1f" % (args.k, sil, km.inertia_))
    print(prof.round(1).to_string(index=False))
    print("\nĐã lưu: output/customer_clusters.csv, output/cluster_profile.csv, models/kmeans_rfm.joblib")
