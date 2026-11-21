"""
Ngày 8 - Sinh luồng giao dịch đến liên tục (mô phỏng): cứ mỗi <interval> giây ghi một file nhỏ vào data/stream/incoming/.

Nguồn: data/clean/transactions.csv (purchase, đã sắp theo thời gian) -> mỗi lô là một đoạn giao dịch liên tiếp.
Mỗi dòng ghi ra ở định dạng 9 cột như events_clean.csv nên dùng lại nguyên mapper.py/reducer.py.

  python streaming/generate_stream.py --reset --batches 10 --interval 20
"""
import argparse
import csv
import itertools
import os
import random
import shutil
import time

ap = argparse.ArgumentParser()
ap.add_argument("--src", default="data/clean/transactions.csv")
ap.add_argument("--base", default="data/stream", help="thư mục gốc: incoming/, results/, state...")
ap.add_argument("--batches", type=int, default=10)
ap.add_argument("--interval", type=float, default=20, help="giây giữa hai lô")
ap.add_argument("--min", type=int, default=100, dest="nmin")
ap.add_argument("--max", type=int, default=500, dest="nmax")
ap.add_argument("--start-row", type=int, default=0, help="bỏ qua bao nhiêu giao dịch đầu")
ap.add_argument("--seed", type=int, default=42)
ap.add_argument("--reset", action="store_true", help="xóa incoming/, results/, state và log cũ trước khi chạy")
args = ap.parse_args()

incoming = os.path.join(args.base, "incoming")
if args.reset and os.path.isdir(args.base):
    try:
        for sub in ("incoming", "results"):
            shutil.rmtree(os.path.join(args.base, sub), ignore_errors=True)
        for name in ("state.csv", "state.meta.json", "latency_log.csv", "generator_log.csv"):
            p = os.path.join(args.base, name)
            if os.path.exists(p):
                os.remove(p)
    except PermissionError as e:
        raise SystemExit("Không xóa được file cũ (%s).\nstream_runner.py đang chạy và giữ file mở: "
                         "nhấn Ctrl+C để dừng runner, chạy lại generator với --reset, rồi mới bật runner." % e)
os.makedirs(incoming, exist_ok=True)

random.seed(args.seed)
log_path = os.path.join(args.base, "generator_log.csv")
new_log = not os.path.exists(log_path)
existing = len([n for n in os.listdir(incoming) if n.startswith("batch_") and n.endswith(".csv")])

with open(args.src, newline="", encoding="utf-8") as src, open(log_path, "a", newline="", encoding="utf-8") as log:
    reader = csv.reader(src)
    next(itertools.islice(reader, args.start_row, args.start_row), None)
    lw = csv.writer(log)
    if new_log:
        lw.writerow(["batch", "created_epoch", "rows", "first_ts", "last_ts"])
    for i in range(existing + 1, existing + args.batches + 1):
        n = random.randint(args.nmin, args.nmax)
        rows = list(itertools.islice(reader, n))
        if not rows:
            print("Hết dữ liệu nguồn.")
            break
        name = "batch_%04d" % i
        tmp = os.path.join(incoming, name + ".csv.tmp")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            for cid, pid, amount, ts, cat, brand, sess in rows:
                w.writerow([ts, "purchase", cid, pid, "unknown", cat, brand, amount, sess])
        os.replace(tmp, os.path.join(incoming, name + ".csv"))   # đổi tên nguyên tử: runner không đọc file dở
        now = time.time()
        lw.writerow([name, "%.3f" % now, len(rows), rows[0][3], rows[-1][3]])
        log.flush()
        print("%s  +%d giao dịch  (%s -> %s)" % (time.strftime("%H:%M:%S"), len(rows), rows[0][3], rows[-1][3]), flush=True)
        if i < existing + args.batches:
            time.sleep(args.interval)
print("Generator xong.")