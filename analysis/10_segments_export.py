"""
Ngày 10 - Đặt tên phân khúc, biểu đồ PCA, bảng đặc tả, và xuất bộ CSV cho dashboard.

  python analysis/10_segments_export.py

Đọc : output/customer_clusters.csv, models/kmeans_rfm.joblib (Ngày 9, K=4),
      data/local_out/yarn_customer_r2 (output MapReduce theo khách), data/local_out/full_time, full_product,
      data/clean/transactions.csv
Ghi : output/dashboard/*.csv, docs/segment_spec.md, docs/img/day10_pca_scatter.png, docs/img/day10_profile.png
Tên và gợi ý hành động mặc định dành cho K=4 (nhãn đánh số theo chi tiêu TB giảm dần). Đọc bảng hồ sơ in ra
và sửa NAMES/ACTIONS (hoặc dùng --names "Tên 0;Tên 1;...") nếu kết quả của bạn khác.
"""
import argparse
import glob
import os
import textwrap

import joblib
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

ap = argparse.ArgumentParser()
ap.add_argument("--clusters", default="output/customer_clusters.csv")
ap.add_argument("--customers", default="data/local_out/yarn_customer_r2")
ap.add_argument("--transactions", default="data/clean/transactions.csv")
ap.add_argument("--time", default="data/local_out/full_time")
ap.add_argument("--product", default="data/local_out/full_product")
ap.add_argument("--model", default="models/kmeans_rfm.joblib")
ap.add_argument("--out", default="output/dashboard")
ap.add_argument("--topn", type=int, default=100)
ap.add_argument("--names", default=None, help="tên các cụm 0..K-1 ngăn cách bằng dấu ;")
args = ap.parse_args()

NAMES = {4: ["Khách giá trị cao", "Khách giỏ hàng lớn, chưa quay lại", "Khách mua gần đây, chi tiêu trung bình",
             "Khách mua lẻ, giá trị thấp"]}
DESCR = {4: ["Mua thường xuyên, chi tiêu rất cao và mua gần đây; nhóm nhỏ nhưng tạo ra phần lớn doanh thu.",
             "Phần lớn chỉ mua một đến hai lần với giỏ hàng khá lớn, đã gần ba tuần chưa quay lại.",
             "Vừa mua trong những ngày gần nhất với chi tiêu quanh mức trung bình; dữ liệu một tháng chưa cho biết họ là khách mới hay khách cũ.",
             "Mua một lần với giá trị rất thấp và đã lâu chưa quay lại."]}
ACTIONS = {4: [
    ["Chương trình thành viên VIP: tích điểm, ưu đãi sớm khi có hàng mới.",
     "Chăm sóc riêng (hỗ trợ ưu tiên, giao hàng nhanh) để giữ chân.",
     "Gợi ý sản phẩm cao cấp và phụ kiện đi kèm (cross-sell); theo dõi dấu hiệu giảm tần suất mua."],
    ["Email/thông báo nhắc nhở kèm voucher có thời hạn để kéo họ quay lại.",
     "Gợi ý sản phẩm cùng danh mục hoặc phụ kiện của món đã mua.",
     "Ưu đãi riêng cho đơn thứ hai nhằm biến khách mua một lần thành khách mua lại."],
    ["Gửi tin cảm ơn và hướng dẫn sử dụng sản phẩm vừa mua để cải thiện trải nghiệm.",
     "Gợi ý phụ kiện hoặc sản phẩm bổ sung trong 1-2 tuần sau khi mua, kèm miễn phí vận chuyển cho đơn kế tiếp.",
     "Thu thập đánh giá và phản hồi; theo dõi tỷ lệ quay lại sau 14-30 ngày để kiểm chứng giả thuyết."],
    ["Kênh tự động, chi phí thấp: gợi ý combo hoặc mua kèm để tăng giá trị đơn.",
     "Khuyến mãi theo ngưỡng (mua thêm để được giảm) thay vì giảm giá sâu.",
     "Không đầu tư chăm sóc cá nhân hóa; đo hiệu quả bằng thử nghiệm A/B trước khi mở rộng."]]}
DOW = {1: "Thứ 2", 2: "Thứ 3", 3: "Thứ 4", 4: "Thứ 5", 5: "Thứ 6", 6: "Thứ 7", 7: "Chủ nhật"}
MR_COLS = ["customer_id", "total_spent", "num_purchases", "avg_amount", "first_purchase", "last_purchase"]

os.makedirs(args.out, exist_ok=True)
os.makedirs("docs/img", exist_ok=True)


def read_parts(path, names, dtype=None):
    files = sorted(glob.glob(os.path.join(path, "part-*")))
    if not files:
        raise SystemExit("Không thấy file part-* trong " + path)
    return pd.concat([pd.read_csv(f, sep="\t", header=None, names=names, dtype=dtype) for f in files],
                     ignore_index=True)


def save(df, name):
    p = os.path.join(args.out, name)
    df.to_csv(p, index=False, encoding="utf-8-sig")           # utf-8-sig để Excel/Power BI đọc đúng tiếng Việt
    print("  %-32s %9d dòng" % (name, len(df)))


# ---------- 1. Ghép nhãn, đặt tên ----------
cl = pd.read_csv(args.clusters, dtype={"customer_id": str})
k = int(cl["cluster"].nunique())
if args.names:
    names = [n.strip() for n in args.names.split(";")]
elif k in NAMES:
    names = NAMES[k]
else:
    names = ["Cụm %d" % i for i in range(k)]
    print("CẢNH BÁO: chưa có tên mặc định cho K=%d, dùng tên tạm; hãy đặt --names." % k)
if len(names) != k:
    raise SystemExit("Số tên (%d) khác số cụm (%d)." % (len(names), k))
name_of = dict(enumerate(names))

mr = read_parts(args.customers, MR_COLS, {"customer_id": str})
seg = cl.merge(mr[["customer_id", "avg_amount", "first_purchase", "last_purchase"]], on="customer_id", how="left")
if seg["first_purchase"].isna().any():
    raise SystemExit("Có khách trong customer_clusters.csv không có trong output MapReduce.")
seg["segment_id"] = seg["cluster"]
seg["segment"] = seg["cluster"].map(name_of)

summ = seg.groupby(["segment_id", "segment"]).agg(
    customers=("customer_id", "count"), purchases=("frequency", "sum"), revenue=("monetary", "sum"),
    recency_mean=("recency_days", "mean"), frequency_mean=("frequency", "mean"),
    monetary_mean=("monetary", "mean"), monetary_median=("monetary", "median")).reset_index()
summ["pct_customers"] = 100 * summ["customers"] / summ["customers"].sum()
summ["pct_revenue"] = 100 * summ["revenue"] / summ["revenue"].sum()
summ["avg_order_value"] = summ["revenue"] / summ["purchases"]
print("\nHồ sơ phân khúc (kiểm tra tên có khớp hành vi không):")
print(summ.round(1).to_string(index=False))
if k == 4 and not (summ.loc[2, "recency_mean"] < summ.loc[1, "recency_mean"]
                   and summ.loc[2, "recency_mean"] < summ.loc[3, "recency_mean"]):
    print("CẢNH BÁO: cụm 2 không phải cụm có Recency thấp nhất, tên mặc định có thể không còn đúng.")

# ---------- 2. PCA và biểu đồ hồ sơ ----------
mdl = joblib.load(args.model)
if int(mdl.get("k", k)) != k:
    print("CẢNH BÁO: mô hình lưu K=%s nhưng nhãn có %d cụm; chạy lại 09_rfm_kmeans.py --k %d." % (mdl.get("k"), k, k))
X = mdl["scaler"].transform(np.log1p(seg[["recency_days", "frequency", "monetary"]].to_numpy(float)))
pca = PCA(n_components=2, random_state=42).fit(X)
Z = pca.transform(X)
ev = 100 * pca.explained_variance_ratio_
rng = np.random.default_rng(42)
idx = rng.choice(len(seg), size=min(30000, len(seg)), replace=False)
colors = plt.get_cmap("tab10")
fig, a = plt.subplots(figsize=(9, 6.5))
for c in range(k):
    m = idx[seg["cluster"].to_numpy()[idx] == c]
    a.scatter(Z[m, 0], Z[m, 1], s=4, alpha=.35, color=colors(c), label=name_of[c])
    cen = Z[(seg["cluster"] == c).to_numpy()].mean(axis=0)
    a.scatter(*cen, marker="X", s=160, color=colors(c), edgecolor="black", zorder=5)
a.set(xlabel="PC1 (%.1f%% phương sai)" % ev[0], ylabel="PC2 (%.1f%% phương sai)" % ev[1],
      title="PCA 2D của R, F, M (log1p + chuẩn hóa); mẫu %d khách, X = tâm cụm" % len(idx))
lg = a.legend(markerscale=4, fontsize=9)
fig.tight_layout()
fig.savefig("docs/img/day10_pca_scatter.png", dpi=150)
plt.close(fig)
print("\nPCA giải thích %.1f%% phương sai (PC1 %.1f%%, PC2 %.1f%%)" % (ev.sum(), ev[0], ev[1]))

zmean = pd.DataFrame(X, columns=["Recency", "Frequency", "Monetary"]).groupby(seg["cluster"].to_numpy()).mean()
fig, a = plt.subplots(figsize=(9, 4.8))
w = .26
for j, col in enumerate(zmean.columns):
    a.bar(np.arange(k) + (j - 1) * w, zmean[col], w, label=col)
a.axhline(0, color="black", lw=.8)
a.set_xticks(range(k))
a.set_xticklabels([textwrap.fill(n, 18) for n in names], fontsize=8)
a.set(ylabel="trung bình đã chuẩn hóa (z-score)",
      title="Hồ sơ từng phân khúc (Recency cao = lâu chưa mua)")
a.legend()
fig.tight_layout()
fig.savefig("docs/img/day10_profile.png", dpi=150)
plt.close(fig)

# ---------- 3. Giao dịch ghép nhãn ----------
tx = pd.read_csv(args.transactions, header=None,
                 names=["customer_id", "product_id", "amount", "timestamp", "category_code", "brand", "session_id"],
                 dtype={"customer_id": str, "product_id": str, "category_code": str, "brand": str, "session_id": str})
ts = pd.to_datetime(tx["timestamp"])
tx["date"] = ts.dt.strftime("%Y-%m-%d")
tx["dow"] = ts.dt.dayofweek + 1
tx["hour"] = ts.dt.hour
tx = tx.merge(seg[["customer_id", "segment_id", "segment"]], on="customer_id", how="left")
unmatched = int(tx["segment_id"].isna().sum())
tx["category_top"] = tx["category_code"].fillna("unknown").str.split(".").str[0]
tx["brand"] = tx["brand"].fillna("unknown")

# ---------- 4. Xuất CSV ----------
print("\nGhi vào %s:" % args.out)
save(seg[["customer_id", "segment_id", "segment", "recency_days", "frequency", "monetary", "avg_amount",
          "first_purchase", "last_purchase"]].rename(columns={"frequency": "num_purchases", "monetary": "total_spent"}),
     "customer_segments.csv")
save(summ.round(2), "segment_summary.csv")

tp = read_parts(args.time, ["dow", "hour", "purchases"])
tp["dow_name"] = tp["dow"].map(DOW)
save(tp[["dow", "dow_name", "hour", "purchases"]].sort_values(["dow", "hour"]), "time_pattern.csv")
tps = tx.groupby(["segment_id", "segment", "dow", "hour"]).size().reset_index(name="purchases")
tps["dow_name"] = tps["dow"].map(DOW)
save(tps, "time_pattern_by_segment.csv")

rd = tx.groupby("date").agg(revenue=("amount", "sum"), purchases=("amount", "count"),
                            customers=("customer_id", "nunique")).reset_index()
save(rd.round(2), "revenue_daily.csv")
rds = tx.groupby(["date", "segment_id", "segment"]).agg(revenue=("amount", "sum"),
                                                          purchases=("amount", "count")).reset_index()
save(rds.round(2), "revenue_daily_by_segment.csv")

rc = tx.groupby(["category_top", "segment_id", "segment"]).agg(
    revenue=("amount", "sum"), purchases=("amount", "count"), customers=("customer_id", "nunique")).reset_index()
save(rc.round(2), "revenue_by_category_segment.csv")
rb = tx.groupby("brand").agg(revenue=("amount", "sum"), purchases=("amount", "count"),
                             customers=("customer_id", "nunique")).reset_index() \
       .sort_values("revenue", ascending=False).head(30)
save(rb.round(2), "revenue_by_brand_top30.csv")

pr = read_parts(args.product, ["product_id", "revenue", "num_purchases", "avg_price", "first_sale", "last_sale"],
                {"product_id": str})
pr["rank_purchases"] = pr["num_purchases"].rank(ascending=False, method="min").astype(int)
top = pr.sort_values("revenue", ascending=False).head(args.topn).copy()
top["rank_revenue"] = range(1, len(top) + 1)
sub = tx[tx["product_id"].isin(top["product_id"])]
mode = sub.groupby("product_id")[["category_code", "brand"]].agg(
    lambda s: s.dropna().mode().iat[0] if s.notna().any() else "unknown").reset_index()
top = top.merge(mode, on="product_id", how="left")
save(top[["rank_revenue", "rank_purchases", "product_id", "category_code", "brand", "revenue", "num_purchases",
          "avg_price"]], "top_products.csv")

# ---------- 5. Kiểm tra nhất quán ----------
print("\nKiểm tra:")
ok = True
chk = tp.merge(tx.groupby(["dow", "hour"]).size().reset_index(name="n"), on=["dow", "hour"], how="outer")
bad = int((chk["purchases"] != chk["n"]).sum())
print("  time_pattern (MapReduce) so với tính lại từ giao dịch: %d ô lệch" % bad)
ok &= bad == 0
d = abs(tx["amount"].sum() - seg["monetary"].sum())
print("  tổng doanh thu giao dịch so với tổng chi tiêu theo khách: lệch %.2f" % d)
ok &= d < 1
print("  giao dịch không ghép được nhãn: %d" % unmatched)
ok &= unmatched == 0
d2 = abs(tx[tx["product_id"].isin(top["product_id"])]["amount"].sum() - top["revenue"].sum())
print("  doanh thu top %d sản phẩm (MapReduce) so với giao dịch: lệch %.2f" % (len(top), d2))
ok &= d2 < 1
print("KẾT QUẢ:", "PASS" if ok else "FAIL")

# ---------- 6. Bảng đặc tả phân khúc ----------
L = ["# Đặc tả phân khúc khách hàng (K=%d)" % k, "",
     "Nguồn: RFM của %s khách có mua hàng trong tháng 10/2019 (REES46); R tính từ ngày tham chiếu %s." %
     (format(len(seg), ","), str(mdl.get("ref_date", ""))[:10]), "",
     "| Nhóm | Tên | Số khách | % khách | % doanh thu | Recency TB (ngày) | Frequency TB | Monetary TB |",
     "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
for _, r in summ.iterrows():
    L.append("| %d | %s | %s | %.1f | %.1f | %.1f | %.1f | %s |" % (
        r["segment_id"], r["segment"], format(int(r["customers"]), ","), r["pct_customers"], r["pct_revenue"],
        r["recency_mean"], r["frequency_mean"], format(round(r["monetary_mean"]), ",")))
L += ["", "Lưu ý: dữ liệu chỉ gồm một tháng nên Recency tối đa 31 ngày; 'lâu chưa quay lại' nghĩa là chưa mua lại "
      "trong khoảng đó, không phải khách đã rời bỏ. Các gợi ý dưới đây là giả thuyết cần kiểm chứng bằng thử nghiệm A/B.", ""]
for c in range(k):
    r = summ[summ["segment_id"] == c].iloc[0]
    L.append("## Nhóm %d - %s" % (c, names[c]))
    L.append("")
    if k in DESCR:
        L.append(DESCR[k][c])
        L.append("")
    L.append("%s khách (%.1f%%), %.1f%% doanh thu; giá trị đơn trung bình %.0f." % (
        format(int(r["customers"]), ","), r["pct_customers"], r["pct_revenue"], r["avg_order_value"]))
    L.append("")
    if k in ACTIONS:
        L.append("Gợi ý hành động:")
        for a_ in ACTIONS[k][c]:
            L.append("- " + a_)
        L.append("")
with open("docs/segment_spec.md", "w", encoding="utf-8") as f:
    f.write("\n".join(L) + "\n")
print("\nĐã lưu docs/segment_spec.md, docs/img/day10_pca_scatter.png, docs/img/day10_profile.png")