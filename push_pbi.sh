#!/usr/bin/env bash
# Đẩy một dòng số liệu của lô vừa xử lý lên dataset streaming của Power BI (Push URL).
# Được gọi từ hook --on-batch, dùng các biến BATCH, ROWS, REVENUE do runner cung cấp.
# Cần: export PBI_PUSH_URL="<Push URL của streaming dataset>"
# Thử không gửi thật: DRY_RUN=1 BATCH=batch_0001 ROWS=427 REVENUE=119014.9 PBI_PUSH_URL=x bash push_pbi.sh
set -euo pipefail
: "${PBI_PUSH_URL:?hãy export PBI_PUSH_URL}"
JSON=$(printf '[{"batch":"%s","rows":%s,"revenue":%s,"time":"%s"}]' \
  "${BATCH:-?}" "${ROWS:-0}" "${REVENUE:-0}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
if [ "${DRY_RUN:-0}" = "1" ]; then echo "$JSON"; exit 0; fi
curl -s -o /dev/null -w "[PBI] HTTP %{http_code}\n" -X POST -H "Content-Type: application/json" -d "$JSON" "$PBI_PUSH_URL"
