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

