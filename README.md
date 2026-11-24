# Phân tích hành vi mua hàng của khách hàng bán lẻ bằng Hadoop trên Microsoft Azure

Đồ án môn Điện toán đám mây. Tác giả: **Đỗ Xuân Tú, 23521698**. Giảng viên: **Th.S Hà Lê Hoài Trung**.

Đồ án phân tích dữ liệu giao dịch bán lẻ trực tuyến (REES46, tháng 10/2019, 5,6 GB, hơn 42 triệu sự kiện) bằng
MapReduce (Java), Hadoop Streaming (Python) và mô phỏng micro-batch, sau đó phân khúc khách hàng bằng RFM và K-means
và trực quan hóa bằng Power BI. Hadoop chạy trên cụm Docker (2 DataNode, 2 NodeManager) ở máy cá nhân và trên máy ảo
Microsoft Azure; dữ liệu và kết quả lưu ở Azure Blob Storage.

## 1. Kết quả chính

| Hạng mục | Kết quả |
|---|---|
| Dữ liệu | 42.448.764 sự kiện, giữ lại 42.349.874 sau làm sạch (99,77%); 742.773 lượt mua; 347.118 khách; 42.241 sản phẩm; 01/10 đến 31/10/2019 |
| Tính đúng đắn | Job Java, Streaming Python và pandas cho cùng kết quả cho 347.118 khách (chỉ lệch làm tròn `avg_amount` tối đa 0,01) |
| Hiệu năng (cụm Docker, 43 map task) | `customerstats` 2 reducer khoảng 2m29s; Streaming khoảng 2m36s; chạy local (169 map task) khoảng 9 phút |
| Micro-batch (Azure VM) | Độ trễ trung bình 25,8s (khoảng cách lô 30s), 25,7s (60s), 17,3s (30s, `--uber`); mỗi cấu hình chạy một lần |
| Phân khúc (K=4) | 10,9% khách tạo 51,4% doanh thu; 62,1% khách chỉ mua một lần |
| Chi phí Azure | **[điền tổng chi phí, số giờ dùng VM]** |

## 2. Kiến trúc

```
Kaggle (2019-Oct.csv, 5,6 GB)
   │  tải thủ công/CLI
   ▼
Azure Blob: raw/ ──► preprocess (pandas, theo khối) ──► events_clean.csv, transactions.csv ──► Blob: clean/
                                                              │
                                                              ▼
                                           HDFS (cụm Hadoop: Docker trên laptop / Docker trên Azure VM)
                                              ├─ MapReduce Java: customerstats, timepattern, productstats
                                              └─ Hadoop Streaming Python: mapper.py / reducer.py
                                                              │
        micro-batch: generate_stream ► runner ► Streaming ► merge_state (trạng thái cộng dồn) ► latency_log
                                                              │
                                                              ▼
                              output MapReduce ──► RFM + K-means (K=4) ──► CSV dashboard ──► Power BI (4 trang)
                                                                                     └──► Blob: output/
```

## 3. Cấu trúc thư mục

Đối chiếu với `git ls-files` của bạn và đổi tên cho khớp nếu khác.

```
preprocess/        Tiền xử lý, EDA, số liệu đối chiếu (ground truth), các script so khớp (00 đến 06)
retail-analysis/   Project Maven: Mapper, Combiner, Reducer, Driver, 12 unit test (cổng vào RetailJobs)
streaming/         Mapper/Reducer Python, mô phỏng micro-batch (generate_stream, stream_runner, merge_state, verify_state)
analysis/          RFM, K-means, chọn K, đặt tên phân khúc, xuất CSV cho dashboard
dashboard/         retail_dashboard.pbix, powerbi_queries.pq.txt, powerbi_measures.dax.txt
hadoop-docker/     Cụm Hadoop 3.3.6 bằng Docker Compose
tools/             check_reproduction.py (kiểm tra kết quả tái lập)
models/            kmeans_rfm.joblib (bộ chuẩn hóa, mô hình, nhãn)
output/            Kết quả nhỏ: k_selection.csv, cluster_profile.csv, dashboard/*.csv
docs/              Nhật ký (log.md), thống kê làm sạch, đặc tả phân khúc, ảnh minh chứng, bảng thời gian
data/              (không đưa lên Git) dữ liệu thô, dữ liệu sạch, kết quả MapReduce
```

## 4. Môi trường

| Thành phần | Phiên bản đã dùng |
|---|---|
| Hadoop | 3.3.6 (image `apache/hadoop:3.3.6`, Java 8 trong container) |
| Build Java | JDK 17 trên máy, biên dịch ra bytecode Java 8 (`maven.compiler.release=8`); Maven |
| Python | 3.10 trên máy; Python 2.7 trong container Hadoop (mapper/reducer viết tương thích cả hai) |
| Công cụ | Docker và Docker Compose, Git, Azure CLI, AzCopy, Power BI Desktop |
| Azure | **[điền: loại subscription, vùng, kích cỡ VM, hệ điều hành VM]** |

Máy cần ít nhất 30 đến 40 GB đĩa trống và Docker được cấp khoảng 6 đến 8 GB RAM, 4 CPU.
Thư viện Python: `pip install -r requirements.txt`.

## 5. Dữ liệu

- Nguồn: [REES46 eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store),
  file `2019-Oct.csv` (cần tài khoản Kaggle). Đặt vào `data/2019-Oct.csv`.
- Cột gốc: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`.
- Dữ liệu sạch (`data/clean/`): `events_clean.csv` (9 cột, không header) và `transactions.csv` (chỉ `purchase`;
  `customer_id, product_id, amount, timestamp, category_code, brand, session_id`).
- Lưu ý: mỗi dòng `purchase` là một sản phẩm. `frequency` trong RFM là số lượt mua sản phẩm, chưa gộp theo phiên.

## 6. Chạy lại từ đầu

Chạy từ gốc repo. Lệnh `hdfs`, `hadoop`, `yarn` chạy trong container namenode.

### Bước 1. Tiền xử lý (máy cá nhân)

```powershell
python preprocess/00_make_sample.py                # tạo mẫu 1 triệu dòng để thử
python preprocess/01_eda_chunked.py                # EDA -> docs/eda_report.md
python preprocess/02_preprocess.py                 # -> data/clean/events_clean.csv, transactions.csv, docs/cleaning_stats.md
python preprocess/03_ground_truth.py               # -> ground_truth.csv, ground_truth_event_counts.csv
```

Kết quả mong đợi: `events_clean.csv` 42.349.874 dòng, `transactions.csv` 742.773 dòng, `ground_truth.csv` 347.118 dòng.

### Bước 2. Bật cụm Hadoop và nạp dữ liệu

```powershell
cd hadoop-docker
docker compose up -d --scale datanode=2 --scale nodemanager=2
docker compose exec namenode bash -c "hdfs dfsadmin -report | grep 'Live datanodes'"    # phải thấy (2)
docker compose exec namenode bash
```

Trong container:

```bash
hdfs dfs -mkdir -p /retail/clean
hdfs dfs -put /data/clean/events_clean.csv /retail/clean/
hdfs dfs -ls -h /retail/clean          # khoảng 5,3 GB, 43 block
```

Luôn dùng `--scale datanode=2 --scale nodemanager=2` và đợi đủ 2 DataNode trước khi nạp. Lưu ý: dữ liệu HDFS có thể mất
khi cụm bị dựng lại, khi đó nạp lại.

### Bước 3. Build JAR và chép vào container

```powershell
cd retail-analysis
mvn clean package                      # 12 test phải pass
cd ..\hadoop-docker
docker compose cp ../retail-analysis/target/retail-analysis.jar namenode:/tmp/
```

### Bước 4. Chạy các job MapReduce Java (trong container)

```bash
export HADOOP_HOME=/opt/hadoop
export MR_ENV="HADOOP_MAPRED_HOME=$HADOOP_HOME"
OPTS="-D yarn.app.mapreduce.am.env=$MR_ENV -D mapreduce.map.env=$MR_ENV -D mapreduce.reduce.env=$MR_ENV"
for r in 1 2 4; do
  hdfs dfs -rm -r -f /retail/out/customer_r$r
  time hadoop jar /tmp/retail-analysis.jar customerstats $OPTS /retail/clean/events_clean.csv /retail/out/customer_r$r $r
done
hdfs dfs -rm -r -f /retail/out/time /retail/out/product
hadoop jar /tmp/retail-analysis.jar timepattern  $OPTS /retail/clean/events_clean.csv /retail/out/time 2
hadoop jar /tmp/retail-analysis.jar productstats $OPTS /retail/clean/events_clean.csv /retail/out/product 2
mkdir -p /data/local_out
hdfs dfs -get /retail/out/customer_r2 /data/local_out/yarn_customer_r2
hdfs dfs -get /retail/out/time        /data/local_out/full_time
hdfs dfs -get /retail/out/product     /data/local_out/full_product
```

Các tham số `-D ...env=...` là bắt buộc trên cụm này (nếu thiếu, YARN báo không tìm thấy `MRAppMaster`).
Counters mong đợi: `PURCHASE_RECORDS=742773`, `SKIPPED_NON_PURCHASE=41607101`, Reduce output 347.118 (customerstats),
168 (timepattern), 42.241 (productstats).

Kiểm chứng (máy cá nhân):

```powershell
python preprocess/04_compare.py data/local_out/yarn_customer_r2 data/clean/ground_truth.csv     # PASS
python preprocess/05_check_aux.py data/local_out/full_time data/local_out/full_product data/clean/ground_truth.csv   # PASS
```

### Bước 5. Hadoop Streaming (Python)

```powershell
docker compose cp ../streaming/mapper.py  namenode:/tmp/
docker compose cp ../streaming/reducer.py namenode:/tmp/
```

Trong container (dùng lại biến `HADOOP_HOME`, `MR_ENV`, `OPTS` ở bước 4, đặt lại nếu mở shell mới; đổi `python` thành `python3` nếu container chỉ có Python 3):

```bash
hdfs dfs -rm -r -f /retail/out/stream_customer
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar $OPTS -D mapreduce.job.reduces=2 \
  -files /tmp/mapper.py,/tmp/reducer.py -mapper "python mapper.py" -reducer "python reducer.py" \
  -input /retail/clean/events_clean.csv -output /retail/out/stream_customer
hdfs dfs -get /retail/out/stream_customer /data/local_out/stream_customer
```

```powershell
python preprocess/04_compare.py data/local_out/stream_customer data/clean/ground_truth.csv         # PASS
python preprocess/06_compare_outputs.py data/local_out/yarn_customer_r2 data/local_out/stream_customer   # PASS
```

### Bước 6. Mô phỏng micro-batch

Chạy `--reset` khi runner đang tắt. Mở hai cửa sổ:

```powershell
python streaming/generate_stream.py --reset --batches 10 --interval 30 --min 100 --max 500
python streaming/stream_runner.py --mode cluster --max-batches 10        # thêm --uber để thử uber mode
python streaming/verify_state.py                                         # PASS
```

Dùng `--mode local` để thử không cần cụm. Độ trễ ghi ở `data/stream/latency_log.csv`.

### Bước 7. RFM, K-means và phân khúc

```powershell
python analysis/09_rfm_kmeans.py                       # chọn K -> docs/img/day9_chon_k.png, output/k_selection.csv
python analysis/09_rfm_kmeans.py --k 4                 # lưu nhãn cụm và mô hình
python analysis/09c_plot_k.py --k 4 --compare docs/cluster_profile_k3.csv docs/cluster_profile_k4.csv
python analysis/10_segments_export.py                  # đặt tên, PCA, đặc tả, CSV cho dashboard
```

(`docs/cluster_profile_k3.csv` có từ lần chạy `--k 3`; chạy `--k 3` trước rồi chép `output/cluster_profile.csv` nếu thiếu.)

### Bước 8. Power BI

Mở `dashboard/retail_dashboard.pbix`. Nếu đường dẫn khác máy, vào **Transform data → Manage Parameters**, đổi `DataFolder`
thành thư mục `output/dashboard` của bạn rồi **Refresh**. Nếu tạo lại báo cáo, dùng `powerbi_queries.pq.txt` và
`powerbi_measures.dax.txt`.

### Bước 9. Kiểm tra tổng thể

```powershell
python tools/check_reproduction.py
```

Mỗi dòng phải là `OK` (hoặc `SKIP` cho bước chưa chạy). `WARN` ở kích thước cụm K-means chấp nhận được nếu bạn dùng
phiên bản scikit-learn khác.

## 7. Chạy trên Azure

**[Điền theo thực tế]**: tên Resource Group, Storage Account (container `raw`, `clean`, `output`), kích cỡ và vùng VM,
cách cài Docker trên VM, cách lấy dữ liệu từ Blob bằng AzCopy (SAS), và cách dừng VM sau khi dùng. Nêu lý do dùng máy ảo
thay cho HDInsight (quota, loại subscription). Các bước 2 đến 6 phía trên chạy nguyên trên VM; kết quả được tải lên
`output/azure_run/`.

| Job (Azure VM) | Thời gian |
|---|---|
| customerstats, 1 / 2 / 4 reducer | **[điền]** |
| timepattern, productstats | **[điền]** |
| Streaming Python | **[điền]** |
| Micro-batch: khoảng cách 30s, 60s, 30s `--uber` | 25,8s / 25,7s / 17,3s (độ trễ trung bình) |

## 8. Dashboard Power BI

| Trang | Nội dung |
|---|---|
| Tổng quan | Thẻ KPI, hai biểu đồ tròn (tỷ trọng khách và doanh thu), doanh thu theo ngày |
| Phân khúc khách hàng | Định vị phân khúc, phân bố số lần mua, top khách, phân bố Recency, bảng hồ sơ |
| Hành vi theo thời gian | Heatmap thứ × giờ, giao dịch theo giờ và theo thứ, cơ cấu doanh thu theo ngày |
| Sản phẩm và danh mục | Top sản phẩm, doanh thu theo danh mục, top thương hiệu, scatter sản phẩm |

Ảnh chụp nằm ở `docs/img/`. Giờ trong dữ liệu là UTC.

## 9. Hạn chế

- Dữ liệu chỉ một tháng: Recency tối đa 31 ngày, không kết luận được khách "rời bỏ". Nhãn phân khúc dựa vào lần mua cuối nên
  doanh thu theo ngày của từng phân khúc không phải xu hướng thật của nhóm đó.
- Mỗi dòng `purchase` là một sản phẩm; `frequency` và giá trị trung bình mỗi lượt mua chưa phải số đơn và giá trị đơn.
- Streaming trong đồ án là micro-batch trên Hadoop (độ trễ khoảng 17 đến 26 giây trên Azure VM), không phải xử lý luồng đúng nghĩa.
- Cấu trúc cụm yếu (Silhouette K=4 là 0,307, tính trên mẫu 20.000 điểm); K=4 được chọn theo ý nghĩa kinh doanh,
  vì K=3 có Silhouette cao hơn (0,357) nhưng gộp hai nhóm mua một lần có giá trị khác nhau.
- Hadoop chạy trên máy ảo Azure thay cho HDInsight. **[điền lý do]**
- Mỗi cấu hình đo thời gian chỉ chạy một đến hai lần, nên chênh lệch nhỏ nằm trong mức nhiễu.

## 10. Sự cố thường gặp

| Triệu chứng | Nguyên nhân và cách xử lý |
|---|---|
| `0 datanode(s) running` khi `hdfs dfs -put` | DataNode chưa lên; dùng `--scale datanode=2 --scale nodemanager=2`, đợi `Live datanodes (2)` |
| `Could not find or load main class ...MRAppMaster` | Thiếu các tham số `-D ...env=HADOOP_MAPRED_HOME=...` |
| `File /tmp/mapper.py does not exist` | Chưa `docker compose cp` hai file Python vào container |
| `--reset` báo `PermissionError` | Runner đang chạy và giữ file; dừng runner rồi reset |
| `/retail/clean` không còn sau khi dựng lại cụm | HDFS không lưu bền khi dựng lại cụm; nạp lại dữ liệu |
| Số liệu Power BI lệch nhiều lần | Thiếu culture `en-US` khi ép kiểu; xem `powerbi_queries.pq.txt` |

## 11. Nguồn và giấy phép

Dữ liệu: REES46 (Kaggle), theo điều khoản của tác giả tập dữ liệu. Mã nguồn: **[điền giấy phép hoặc "dùng cho mục đích học tập"]**.