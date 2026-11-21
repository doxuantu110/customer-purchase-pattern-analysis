"""
Ngày 8 - Cộng dồn kết quả mỗi lô vào trạng thái tổng theo khách (total, count, first, last).

Dùng như module (stream_runner.py) hoặc dòng lệnh:
  python streaming/merge_state.py data/stream/state.csv data/stream/results/batch_0001.out [...]

Mỗi lô chỉ được cộng một lần (danh sách lô đã xử lý lưu ở <state>.meta.json thay cho đuôi .csv).
"""
import csv
import json
import os
import sys

HEADER = ["customer_id", "total_spent", "num_purchases", "avg_amount", "first_purchase", "last_purchase"]


def meta_path(state_path):
    return os.path.splitext(state_path)[0] + ".meta.json"


def load_state(path):
    state = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                state[r["customer_id"]] = {"total": float(r["total_spent"]), "count": int(r["num_purchases"]),
                                           "first": r["first_purchase"], "last": r["last_purchase"]}
    return state


def load_processed(path):
    p = meta_path(path)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return set(json.load(f).get("processed", []))
    return set()


def merge_batch(state, out_path):
    """Cộng một file output của lô (6 cột, phân tách bằng TAB) vào state. Trả về số khách trong lô."""
    n = 0
    with open(out_path, encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\r\n").split("\t")
            if len(p) != 6:
                continue
            cid, total, count, _avg, first, last = p
            s = state.get(cid)
            if s is None:
                state[cid] = {"total": float(total), "count": int(count), "first": first, "last": last}
            else:
                s["total"] += float(total)
                s["count"] += int(count)
                s["first"] = min(s["first"], first)
                s["last"] = max(s["last"], last)
            n += 1
    return n


def save_state(state, path, processed):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        for cid in sorted(state):
            s = state[cid]
            w.writerow([cid, "%.2f" % s["total"], s["count"], "%.2f" % (s["total"] / s["count"]),
                        s["first"], s["last"]])
    os.replace(tmp, path)
    with open(meta_path(path), "w", encoding="utf-8") as f:
        json.dump({"processed": sorted(processed)}, f)


def merge_files(state_path, out_paths):
    state = load_state(state_path)
    processed = load_processed(state_path)
    for p in out_paths:
        name = os.path.splitext(os.path.basename(p))[0]
        if name in processed:
            print("Bỏ qua (đã cộng): " + name)
            continue
        merge_batch(state, p)
        processed.add(name)
    save_state(state, state_path, processed)
    return state


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    st = merge_files(sys.argv[1], sys.argv[2:])
    print("Trạng thái: %d khách, tổng doanh thu %.2f" % (len(st), sum(s["total"] for s in st.values())))
