#!/usr/bin/env bash
# ETL tự động (demo): nguồn giao dịch đến -> phát hiện lô mới -> job Streaming trên YARN
#                     -> cộng dồn trạng thái -> đẩy kết quả lên Azure Blob (output/stream)
# Chuẩn bị (mỗi phiên SSH):
#   cd ~/customer-purchase-pattern-analysis && source .venv/bin/activate
#   export BLOB_URL="https://<ten-storage>.blob.core.windows.net"
#   read -rs SAS; export SAS          # dán SAS có quyền ghi vào container output
# Chạy:  bash etl_demo.sh [số lô] [giây giữa các lô]
set -euo pipefail
N=${1:-5}; GAP=${2:-30}
: "${BLOB_URL:?hãy export BLOB_URL}"; : "${SAS:?hãy export SAS}"
rm -rf data/stream; mkdir -p logs

python streaming/stream_runner.py --mode cluster --max-batches "$N" --uber \
  --on-batch 'echo "[ETL] lô $BATCH ($ROWS dòng) xong, doanh thu cộng dồn $REVENUE"; azcopy copy data/stream/state.csv "$BLOB_URL/output/stream/state.csv?$SAS" --overwrite=true --log-level=NONE >/dev/null && echo "[ETL] đã đẩy state.csv lên Blob output/stream"' \
  2>&1 | tee logs/etl_runner.log &
RUNNER=$!

sleep 20    # chờ runner kết nối cụm và chép mapper/reducer
python streaming/generate_stream.py --src data/transactions.csv --batches "$N" --interval "$GAP" 2>&1 | tee logs/etl_generator.log
wait "$RUNNER"
echo "[ETL] hoàn tất $N lô"
