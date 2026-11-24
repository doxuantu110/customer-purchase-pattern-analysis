"""
Đẩy một dòng số liệu của lô vừa xử lý lên Power BI streaming dataset (loại API) qua Push URL.

Dùng làm hook của runner:
  export POWERBI_PUSH_URL='https://api.powerbi.com/beta/.../rows?...key=...'   # KHÔNG đưa vào Git
  python streaming/stream_runner.py --mode cluster --on-batch "python streaming/push_powerbi.py"

Đọc các biến môi trường do runner đặt: BATCH, ROWS, JOB_S, QUEUE_WAIT_S, LATENCY_S, CUSTOMERS, REVENUE.
Schema của streaming dataset cần tạo trong Power BI Service (đúng tên và kiểu):
  timestamp (DateTime), batch (Text), rows (Number), job_s (Number), queue_wait_s (Number),
  latency_s (Number), customers (Number), revenue (Number)
Chạy thử không cần Power BI:  python streaming/push_powerbi.py --dry-run
"""
import datetime
import json
import os
import sys
import urllib.error
import urllib.request

dry = "--dry-run" in sys.argv


def num(name, default=0.0):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


row = {
    "timestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    "batch": os.environ.get("BATCH", "demo"),
    "rows": num("ROWS"),
    "job_s": num("JOB_S"),
    "queue_wait_s": num("QUEUE_WAIT_S"),
    "latency_s": num("LATENCY_S"),
    "customers": num("CUSTOMERS"),
    "revenue": num("REVENUE"),
}
payload = json.dumps([row]).encode("utf-8")

if dry:
    print("DRY RUN, sẽ gửi:", payload.decode())
    sys.exit(0)

url = os.environ.get("POWERBI_PUSH_URL")
if not url:
    sys.exit("Thiếu biến môi trường POWERBI_PUSH_URL")
req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
try:
    with urllib.request.urlopen(req, timeout=20) as r:
        print("Đã đẩy %s lên Power BI (HTTP %d)" % (row["batch"], r.status))
except urllib.error.HTTPError as e:
    sys.exit("Power BI trả HTTP %d: %s" % (e.code, e.read().decode(errors="replace")[:200]))
except Exception as e:
    sys.exit("Không đẩy được: %s" % e)
