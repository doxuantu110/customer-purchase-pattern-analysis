#!/usr/bin/env bash
# ETL tự động (demo): nguồn giao dịch đến -> phát hiện lô mới -> job Streaming trên YARN
#                     -> cộng dồn -> bảng tổng hợp -> đẩy lên Azure Blob (output/stream) -> Power BI
# Cần cùng thư mục gốc repo: etl_demo.sh, etl_hook.sh, push_pbi.sh, stream_aggregates.py
# Chuẩn bị (mỗi phiên SSH):
#   cd ~/customer-purchase-pattern-analysis && source .venv/bin/activate && source ~/demo_env.sh
# Chạy:  bash etl_demo.sh [số lô] [giây giữa các lô]    (demo đẹp hơn: MINROWS=2000 MAXROWS=5000 bash etl_demo.sh 4 25)
set -euo pipefail
N=${1:-5}; GAP=${2:-30}
: "${BLOB_URL:?hãy export BLOB_URL}"; : "${SAS:?hãy export SAS}"
export BLOB_URL SAS
rm -rf data/stream; mkdir -p logs

python streaming/stream_runner.py --mode cluster --max-batches "$N" --uber \
  --on-batch 'bash etl_hook.sh' 2>&1 | tee logs/etl_runner.log &
RUNNER=$!

sleep 20    # chờ runner kết nối cụm và chép mapper/reducer
python streaming/generate_stream.py --src data/transactions.csv --batches "$N" --interval "$GAP" --min "${MINROWS:-100}" --max "${MAXROWS:-500}" 2>&1 | tee logs/etl_generator.log
wait "$RUNNER"
echo "[ETL] hoàn tất $N lô"