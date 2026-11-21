"""
Ngày 9 - Vẽ lại biểu đồ chọn K (không chạy lại K-means; đọc output/k_selection.csv).

  python analysis/09c_plot_k.py --k 4
  python analysis/09c_plot_k.py --k 4 --compare docs/cluster_profile_k3.csv docs/cluster_profile_k4.csv

Đầu ra:
  docs/img/day9_chon_k_v2.png         4 biểu đồ: Elbow, mức giảm inertia, Silhouette, Davies-Bouldin (đánh dấu K đã chọn)
  docs/img/day9_so_sanh_k3_k4.png     % khách và % doanh thu từng cụm ở hai phương án (nếu có --compare)
Biểu đồ giữ nguyên toàn bộ số liệu (không ẩn chỉ số nào); K được đánh dấu là K *đã chọn*, không phải K tốt nhất theo một chỉ số.
"""
import argparse
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--k", type=int, required=True, help="K đã chọn")
ap.add_argument("--input", default="output/k_selection.csv")
ap.add_argument("--compare", nargs=2, metavar=("PROFILE_A", "PROFILE_B"),
                help="hai file cluster_profile.csv để so sánh, ví dụ K=3 và K=4")
args = ap.parse_args()
os.makedirs("docs/img", exist_ok=True)

res = pd.read_csv(args.input)
ks = res["k"].to_numpy()
RED = "tab:red"


def mark(ax):
    ax.axvline(args.k, ls="--", color=RED, alpha=.7)
    row = res[res["k"] == args.k].iloc[0]
    return row


fig, ax = plt.subplots(2, 2, figsize=(12, 8))

# 1. Elbow
a = ax[0, 0]
a.plot(ks, res["inertia"], "o-")
row = mark(a)
a.plot([args.k], [row["inertia"]], "o", color=RED, ms=10)
a.set(title="Elbow (inertia): không có khuỷu rõ rệt", xlabel="K", ylabel="inertia")

# 2. Mức giảm inertia khi tăng K
a = ax[0, 1]
drop = 100 * (res["inertia"].iloc[:-1].to_numpy() - res["inertia"].iloc[1:].to_numpy()) / res["inertia"].iloc[:-1].to_numpy()
a.bar(ks[1:], drop, color=["tab:red" if k == args.k else "tab:blue" for k in ks[1:]])
for k, d in zip(ks[1:], drop):
    a.text(k, d + .3, "%.0f%%" % d, ha="center", fontsize=8)
a.set(title="Inertia giảm bao nhiêu % so với K-1", xlabel="K", ylabel="% giảm")

# 3. Silhouette
a = ax[1, 0]
a.plot(ks, res["silhouette"], "o-", color="tab:green")
a.axhspan(0.26, 0.50, color="gray", alpha=.12)
a.text(ks[-1], 0.485, "cấu trúc yếu (0,26–0,50)", ha="right", va="top", fontsize=8, color="dimgray")
row = mark(a)
a.plot([args.k], [row["silhouette"]], "o", color=RED, ms=10)
for k in (3, args.k):
    r = res[res["k"] == k]
    if len(r):
        a.annotate("K=%d: %.3f" % (k, r.iloc[0]["silhouette"]), (k, r.iloc[0]["silhouette"]),
                   textcoords="offset points", xytext=(8, 8), fontsize=9)
a.set(title="Silhouette (cao hơn = tách tốt hơn)", xlabel="K", ylabel="silhouette")

# 4. Davies-Bouldin
a = ax[1, 1]
a.plot(ks, res["davies_bouldin"], "o-", color="tab:purple")
row = mark(a)
a.plot([args.k], [row["davies_bouldin"]], "o", color=RED, ms=10)
for k in (3, args.k):
    r = res[res["k"] == k]
    if len(r):
        a.annotate("K=%d: %.3f" % (k, r.iloc[0]["davies_bouldin"]), (k, r.iloc[0]["davies_bouldin"]),
                   textcoords="offset points", xytext=(8, 8), fontsize=9)
a.set(title="Davies-Bouldin (thấp hơn = tốt hơn)", xlabel="K", ylabel="Davies-Bouldin")

fig.suptitle("Chọn số cụm: các chỉ số không phân định rõ; K=%d (đường đỏ) được chọn theo ý nghĩa kinh doanh" % args.k,
             fontsize=12)
fig.tight_layout(rect=[0, 0, 1, .96])
fig.savefig("docs/img/day9_chon_k_v2.png", dpi=150)
print("Đã lưu docs/img/day9_chon_k_v2.png")

if args.compare:
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8), sharey=True)
    for a, path in zip(axes, args.compare):
        p = pd.read_csv(path).sort_values("cluster")
        x = np.arange(len(p))
        w = .38
        b1 = a.bar(x - w / 2, p["pct_customers"], w, label="% khách", color="tab:blue")
        b2 = a.bar(x + w / 2, p["pct_revenue"], w, label="% doanh thu", color="tab:orange")
        for bars in (b1, b2):
            for r in bars:
                a.text(r.get_x() + r.get_width() / 2, r.get_height() + .8, "%.1f" % r.get_height(),
                       ha="center", fontsize=8)
        a.set_xticks(x)
        a.set_xticklabels(["Cụm %d\nR=%.0f F=%.1f\nM=%.0f" % (c, r_, f_, m_) for c, r_, f_, m_ in
                           zip(p["cluster"], p["recency_mean"], p["frequency_mean"], p["monetary_mean"])], fontsize=8)
        a.set_title("K=%d" % len(p))
        a.set_ylabel("%")
        a.legend()
    fig.suptitle("Mỗi cụm chiếm bao nhiêu % khách và % doanh thu (R: ngày từ lần mua cuối, F: số lượt, M: chi tiêu TB)",
                 fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, .94])
    fig.savefig("docs/img/day9_so_sanh_k3_k4.png", dpi=150)
    print("Đã lưu docs/img/day9_so_sanh_k3_k4.png")