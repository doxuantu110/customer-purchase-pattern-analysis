## Ngày 3
- Tiền xử lý REES46: X dòng vào, Y dòng ra events_clean (Z%), W purchase trong transactions.csv
- Quy tắc: bỏ thiếu khóa, price <= 0, trùng; điền unknown cho brand/category_code
- Số liệu làm sạch: docs/cleaning_stats.md; ground truth: data/clean/ground_truth*.csv
- Đã upload lên Azure clean/ và nạp HDFS /retail/clean (events_clean: N block)

## Ngày 4
- Project Maven retail-analysis (Hadoop x.y.z, Java N), job CustomerStats: Mapper/Combiner/Reducer/Driver
- Mapper chỉ xử lý purchase; counter INVALID_RECORDS, SKIPPED_NON_PURCHASE, PURCHASE_RECORDS
- Value chung: sum,count,min_ts,max_ts; avg tính ở Reducer
- mvn clean package thành công, tạo retail-analysis.jar



## Ngày 6
Môi trường: cluster Docker (Hadoop 3.3.6, 2 DataNode, 2 NodeManager), không dùng HDInsight.
Job: customerstats trên events_clean.csv (5,28 GiB, 43 block, 43 map task).

| Số reducer | Thời gian (Elapsed) | Ghi chú |
| --- | --- | --- |
| 1 | 2m25s, 2m41s | 1 file output, 22.796.538 byte |
| 2 | 2m29s | 2 file output |
| 4 | 2m50s, 3m02s | 4 file output, mỗi file ~5,7 MB |

Nhận xét: shuffle chỉ ~31,6 MiB nên số reducer ít ảnh hưởng; 4 reducer có xu hướng chậm hơn do overhead.
Kết quả 2 reducer: PASS so với ground_truth.csv (347.118 khách).
Lỗi đã gặp: MRAppMaster thiếu HADOOP_MAPRED_HOME, xử lý bằng -D yarn.app.mapreduce.am.env/mapreduce.map.env/mapreduce.reduce.env.

## Ngày 7 - Hadoop Streaming (Python)
- Viết streaming/mapper.py, streaming/reducer.py (tương thích Python 2.7 và 3); counters nhóm `retail` gửi qua stderr.
- Python trong container: [điền kết quả `python --version`]
- Kiểm thử cục bộ bằng streaming/run_local.py: mẫu và toàn bộ file đều PASS so với ground truth.
- Chạy trên YARN (2 reducer, 43 map task): real 2m36s; PURCHASE_RECORDS=742.773, SKIPPED_NON_PURCHASE=41.607.101, Map input=42.349.874, Reduce output=347.118; không có Combiner (Combine records=0, Reduce input=742.773); shuffle 28.796.687 byte.
- Đối chiếu ground truth (04_compare.py): PASS, 347.118 khách.
- So sánh với job Java (06_compare_outputs.py): PASS; 335.222/347.118 khách giống hệt từng ký tự, phần còn lại chỉ lệch avg_amount 0,01 (khác cách làm tròn Java/Python); không sai số lượt hay thời gian.

| Job | Thời gian | Combiner | Reduce input records |
| --- | --- | --- | --- |
| Java, 2 reducer | Elapsed 2m29s | Có (742.773 xuống 541.436, đo ở 1 và 4 reducer) | 541.436 |
| Streaming Python, 2 reducer | real 2m36s | Không | 742.773 |

- Lưu ý: hai lần đo cách nhau một lần dựng lại cụm nên không hoàn toàn cùng điều kiện; không kết luận Python nhanh hơn Java.
- Sự cố: (1) quên chép mapper.py/reducer.py vào container (File /tmp/mapper.py does not exist) -> docker compose cp; (2) bật lại cụm không có --scale và chạy job quá sớm khi DataNode chưa đăng ký; (3) HDFS bị đặt lại sau khi khởi động lại NameNode (nghi do NameNode định dạng lại khi container khởi động; chưa xác nhận) -> nạp lại events_clean.csv và chạy lại.

## Ngày 8 - Mô phỏng micro-batch
- Thiết kế: generate_stream.py (sinh lô 100-500 giao dịch vào data/stream/incoming) -> stream_runner.py (job Streaming cho từng lô) -> merge_state.py (cộng dồn state.csv) -> verify_state.py (tính lại bằng pandas) + latency_log.csv.
- Chế độ local (khoảng cách lô 5s, 8 lô, 1.410 giao dịch): job 0,2-0,3s; độ trễ trung bình 5,5s, lớn nhất 17,6s (do runner khởi động muộn, hàng đợi tự xả sau 4 lô). verify_state.py: PASS (1.140 khách, lệch tổng 0,0000).
- Chế độ cluster (YARN, khoảng cách lô 30s, 10 lô, 2.120 khách, doanh thu tích lũy 805.322,26): job trung bình 30,0s (trung vị 30,1s, lớn nhất 32,1s); chờ 61-65s; độ trễ trung bình 93,3s, lớn nhất 95,5s.
- Phân tích: thời gian job (khoảng 30s) bằng khoảng cách lô nên hệ thống bão hòa, tồn đọng khoảng 2 lô từ đầu. Độ trễ thật của một lô khoảng 30s (độ trễ trừ thời gian chờ). Thời gian job gần như không phụ thuộc kích thước lô (112 giao dịch: 29,0s; 477 giao dịch: 27,3s) nên chủ yếu là chi phí khởi động YARN.
- Kết luận: micro-batch kiểu MapReduce có độ trễ tối thiểu cỡ chục giây do chi phí khởi động job; muốn dưới vài giây cần Spark Streaming/Flink (ngoài phạm vi đồ án).
- Chưa làm: chạy lại cluster với khoảng cách 60s và --uber (hai DataNode bị dừng); chưa chạy verify_state.py cho lần chạy cluster (state bị ghi đè bởi lần chạy local). Chỉ ghi vào báo cáo những gì đã đo.
- Sự cố: --reset báo PermissionError vì runner đang giữ file mở (dừng runner trước khi reset); runner in log cũ khi lỗi (đã sửa: xóa log cũ trước mỗi lô); cụm không có DataNode (docker compose up -d --scale datanode=2 --scale nodemanager=2, đợi Live datanodes (2)).

## Ngày 9 - RFM và K-means
- Nguồn: output job Java (data/local_out/yarn_customer_r2), 347.118 khách; ngày tham chiếu 2019-11-01 (ngày cuối dataset + 1).
- Thống kê: Recency 1-31 ngày (trung vị 14); Frequency trung vị 1, p99 = 15, max 321; Monetary trung vị 246,52, p99 = 6.655,90, max 265.569,52. 62,1% khách mua đúng 1 lần.
- Tiền xử lý: log1p rồi StandardScaler. K-means K=2..10, n_init=10, random_state=42; silhouette tính trên mẫu 20.000 điểm (cố định bằng random_state).

| K | Inertia | Silhouette | Davies-Bouldin |
| --- | ---: | ---: | ---: |
| 2 | 671.879 | 0,404 | 1,119 |
| 3 | 494.985 | 0,357 | 1,052 |
| 4 | 384.214 | 0,307 | 1,011 |
| 5 | 326.883 | 0,303 | 1,009 |
| 6 | 290.587 | 0,297 | 1,042 |
| 7 | 260.524 | 0,297 | 1,013 |
| 8 | 235.072 | 0,290 | 1,011 |
| 9 | 216.105 | 0,303 | 1,005 |
| 10 | 199.149 | 0,300 | 0,980 |

- Nhận xét: Elbow không có khuỷu rõ; Silhouette giảm mạnh từ K=3 sang K=4 rồi đi ngang khoảng 0,30; cấu trúc cụm yếu ở mọi K (dữ liệu là một dải liên tục).
- Chọn K=4 thay vì K=3 (Silhouette cao nhất trong 3-6): K=4 tách nhóm khách mua một lần của K=3 thành nhóm giỏ hàng lớn (34,5% khách, 33,3% doanh thu, chi TB 639) và nhóm giá trị thấp (34,0% khách, 4,8% doanh thu, chi TB 93); Davies-Bouldin của K=4 cũng tốt hơn K=3. Hồ sơ cụm: docs/cluster_profile_k3.csv và docs/cluster_profile_k4.csv.

| Cụm (K=4) | % khách | % doanh thu | Recency TB | Frequency TB | Monetary TB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 10,9 | 51,4 | 8,6 | 8,1 | 3.122 |
| 1 | 34,5 | 33,3 | 18,5 | 1,7 | 639 |
| 2 | 20,6 | 10,6 | 4,1 | 1,5 | 340 |
| 3 | 34,0 | 4,8 | 18,9 | 1,1 | 93 |

- Giới hạn: dữ liệu chỉ một tháng (Recency tối đa 31 ngày) nên không kết luận khách "ngủ đông"; cảnh báo joblib về số lõi CPU trên Windows là vô hại.
- Lưu: output/customer_clusters.csv, output/cluster_profile.csv, models/kmeans_rfm.joblib (nhãn đánh số lại theo Monetary giảm dần).