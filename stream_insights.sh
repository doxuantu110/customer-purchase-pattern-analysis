#!/usr/bin/env bash
# Chạy job Hadoop Streaming (insights_mapper.py / insights_reducer.py) trên YARN
# cho các lô đã xử lý (từ lô đầu đến $BATCH). Kết quả: data/stream/insights.tsv
# Chạy từ gốc repo. Python trong container: PY=python (mặc định) hoặc PY=python3.
set -euo pipefail
PY=${PY:-python}
INPUTS=""
for f in data/stream/incoming/batch_*.csv; do
  n=$(basename "$f" .csv)
  if [[ -z "${BATCH:-}" || ! "$n" > "$BATCH" ]]; then INPUTS="$INPUTS -input /retail/stream/incoming/$n.csv"; fi
done
[ -n "$INPUTS" ] || { echo "[INSIGHTS] chưa có lô nào"; exit 1; }
(cd hadoop-docker && docker compose cp ../insights_mapper.py namenode:/tmp/ >/dev/null && docker compose cp ../insights_reducer.py namenode:/tmp/ >/dev/null)
(cd hadoop-docker && docker compose exec -T namenode bash -c "
hdfs dfs -rm -r -f /retail/stream/insights >/dev/null
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -D mapreduce.job.reduces=1 -D mapreduce.job.ubertask.enable=true \
  -files /tmp/insights_mapper.py,/tmp/insights_reducer.py \
  -mapper '$PY insights_mapper.py' -reducer '$PY insights_reducer.py' \
  $INPUTS -output /retail/stream/insights > /tmp/insights.log 2>&1 \
  && hdfs dfs -cat '/retail/stream/insights/part-*' > /data/stream/insights.tsv") \
  || { echo "[INSIGHTS] job Streaming lỗi"; exit 1; }
echo "[INSIGHTS] Hadoop Streaming xong: $(wc -l < data/stream/insights.tsv) khóa tổng hợp"
