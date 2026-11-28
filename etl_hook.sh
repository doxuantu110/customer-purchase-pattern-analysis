#!/usr/bin/env bash
# Hook chạy sau mỗi lô (gọi bởi stream_runner.py --on-batch). Dùng biến BATCH, ROWS, REVENUE.
# Việc làm: tạo bảng tổng hợp luồng -> đẩy lên Azure Blob (output/stream) -> (tuỳ chọn) đẩy số liệu lên Power BI.
echo "[ETL] lô ${BATCH:-?} (${ROWS:-?} dòng) xong, doanh thu cộng dồn ${REVENUE:-?}"
if bash stream_insights.sh; then
  python stream_aggregates.py "${BATCH:-}" --tsv data/stream/insights.tsv
else
  echo "[ETL] job insights lỗi, dùng tính toán dự phòng bằng Python"
  python stream_aggregates.py "${BATCH:-}"
fi
if azcopy copy "data/stream/export/*" "$BLOB_URL/output/stream/?$SAS" --overwrite=true --log-level=NONE >/dev/null; then
  echo "[ETL] đã đẩy 4 file (state, timepattern, products, meta) lên Blob output/stream"
else
  echo "[ETL] LỖI: không đẩy được lên Blob (kiểm tra BLOB_URL, SAS còn hạn và có quyền ghi)"
fi
[ -n "${PBI_PUSH_URL:-}" ] && bash push_pbi.sh
exit 0
