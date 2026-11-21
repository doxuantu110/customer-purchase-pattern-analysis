"""
Ngày 8 - Bộ chạy micro-batch: phát hiện file mới trong incoming/, chạy job Streaming cho riêng file đó,
cộng dồn kết quả vào trạng thái tổng và ghi log độ trễ.

  python streaming/stream_runner.py --mode local   --max-batches 10     # thử trên máy (mapper|sort|reducer)
  python streaming/stream_runner.py --mode cluster --max-batches 10     # chạy job Streaming trên YARN (Docker)

Chạy từ gốc repo. Mở generate_stream.py ở cửa sổ khác (chạy generator trước, runner sau vài giây).
Độ trễ = lúc có kết quả cộng dồn - lúc file xuất hiện (mtime của file lô).
"""
import argparse
import csv
import glob
import os
import statistics
import subprocess
import sys
import time

import merge_state

HERE = os.path.dirname(os.path.abspath(__file__))
ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["local", "cluster"], default="local")
ap.add_argument("--base", default="data/stream")
ap.add_argument("--max-batches", type=int, default=10)
ap.add_argument("--idle-timeout", type=float, default=180, help="dừng nếu quá số giây này không có lô mới")
ap.add_argument("--poll", type=float, default=1.0)
ap.add_argument("--compose-dir", default="hadoop-docker")
ap.add_argument("--python", default="python", help="lệnh Python trong container (python hoặc python3)")
ap.add_argument("--hadoop-home", default="/opt/hadoop")
ap.add_argument("--streaming-version", default="3.3.6")
ap.add_argument("--reducers", type=int, default=1)
ap.add_argument("--uber", action="store_true",
                help="bật uber mode: chạy map/reduce ngay trong ApplicationMaster, bớt thời gian khởi động container")
args = ap.parse_args()

incoming = os.path.join(args.base, "incoming")
results = os.path.join(args.base, "results")
state_path = os.path.join(args.base, "state.csv")
lat_path = os.path.join(args.base, "latency_log.csv")
os.makedirs(incoming, exist_ok=True)
os.makedirs(results, exist_ok=True)


def run_local(name):
    out = os.path.join(results, name + ".out")
    with open(os.path.join(incoming, name + ".csv"), "rb") as f:
        mapped, _ = subprocess.Popen([sys.executable, os.path.join(HERE, "mapper.py")], stdin=f,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    reduced, _ = subprocess.Popen([sys.executable, os.path.join(HERE, "reducer.py")], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE).communicate(b"".join(sorted(mapped.splitlines(keepends=True))))
    with open(out, "wb") as f:
        f.write(reduced)
    return out


def docker(cmd):
    return subprocess.run(["docker", "compose", "exec", "-T", "namenode", "bash", "-c", cmd],
                          cwd=args.compose_dir, capture_output=True, text=True)


def setup_cluster():
    for s in ("mapper.py", "reducer.py"):
        r = subprocess.run(["docker", "compose", "cp", os.path.abspath(os.path.join(HERE, s)), "namenode:/tmp/"],
                           cwd=args.compose_dir, capture_output=True, text=True)
        if r.returncode:
            sys.exit("Không chép được %s vào container:\n%s" % (s, r.stderr))
    r = docker("hdfs dfsadmin -report | grep 'Live datanodes'")
    info = r.stdout.strip()
    print("Cluster: " + (info or r.stderr.strip() or "(không có thông tin)"))
    if not info or "(0)" in info:
        sys.exit("Cluster chưa sẵn sàng (không có DataNode nào đang chạy). Kiểm tra bằng 'docker compose ps' và "
                 "'hdfs dfsadmin -report' trong container namenode, rồi chạy lại.")


def run_cluster(name):
    h = args.hadoop_home
    script = " && ".join([
        "export HADOOP_HOME=%s" % h,
        "MR_ENV=HADOOP_MAPRED_HOME=%s" % h,
        "rm -f /tmp/stream_%s.log" % name,
        "hdfs dfs -mkdir -p /retail/stream/incoming",
        "hdfs dfs -put -f /data/stream/incoming/%s.csv /retail/stream/incoming/%s.csv" % (name, name),
        "hdfs dfs -rm -r -f /retail/stream/out/%s" % name,
        ("hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-%s.jar "
         "-D yarn.app.mapreduce.am.env=$MR_ENV -D mapreduce.map.env=$MR_ENV -D mapreduce.reduce.env=$MR_ENV "
         "-D mapreduce.job.reduces=%d %s-files /tmp/mapper.py,/tmp/reducer.py "
         "-mapper '%s mapper.py' -reducer '%s reducer.py' "
         "-input /retail/stream/incoming/%s.csv -output /retail/stream/out/%s > /tmp/stream_%s.log 2>&1"
         % (args.streaming_version, args.reducers,
            "-D mapreduce.job.ubertask.enable=true " if args.uber else "", args.python, args.python, name, name, name)),
        "mkdir -p /data/stream/results",
        "hdfs dfs -cat '/retail/stream/out/%s/part-*' > /data/stream/results/%s.out" % (name, name),
    ])
    r = docker(script)
    if r.returncode:
        tail = docker("tail -25 /tmp/stream_%s.log" % name)
        sys.exit("Job lô %s lỗi:\n%s\n%s\n--- log job ---\n%s" % (name, r.stdout, r.stderr, tail.stdout))
    return os.path.join(results, name + ".out")


if args.mode == "cluster":
    setup_cluster()

state = merge_state.load_state(state_path)
processed = merge_state.load_processed(state_path)
new_lat = not os.path.exists(lat_path)
done = 0
last_new = time.time()
rows_log = []
print("Runner (%s) đang chờ lô mới trong %s ..." % (args.mode, incoming), flush=True)

with open(lat_path, "a", newline="", encoding="utf-8") as lf:
    lw = csv.writer(lf)
    if new_lat:
        lw.writerow(["batch", "rows", "arrival_epoch", "start_epoch", "job_done_epoch", "merged_epoch",
                     "queue_wait_s", "job_s", "latency_s"])
    while done < args.max_batches:
        pending = [p for p in sorted(glob.glob(os.path.join(incoming, "batch_*.csv")))
                   if os.path.splitext(os.path.basename(p))[0] not in processed]
        if not pending:
            if time.time() - last_new > args.idle_timeout:
                print("Không có lô mới trong %.0f giây, dừng." % args.idle_timeout)
                break
            time.sleep(args.poll)
            continue
        last_new = time.time()
        path = pending[0]
        name = os.path.splitext(os.path.basename(path))[0]
        arrival = os.path.getmtime(path)
        with open(path, "rb") as f:
            nrows = sum(1 for _ in f)
        t_start = time.time()
        out = run_local(name) if args.mode == "local" else run_cluster(name)
        t_job = time.time()
        merge_state.merge_batch(state, out)
        processed.add(name)
        merge_state.save_state(state, state_path, processed)
        t_done = time.time()
        lw.writerow([name, nrows, "%.3f" % arrival, "%.3f" % t_start, "%.3f" % t_job, "%.3f" % t_done,
                     "%.2f" % (t_start - arrival), "%.2f" % (t_job - t_start), "%.2f" % (t_done - arrival)])
        lf.flush()
        rows_log.append((t_job - t_start, t_done - arrival))
        done += 1
        print("%s  %s  %d giao dịch | job %.1fs | chờ %.1fs | độ trễ %.1fs | %d khách, doanh thu %.2f"
              % (time.strftime("%H:%M:%S"), name, nrows, t_job - t_start, t_start - arrival, t_done - arrival,
                 len(state), sum(s["total"] for s in state.values())), flush=True)

if rows_log:
    jobs = [r[0] for r in rows_log]
    lats = [r[1] for r in rows_log]
    print("\nTổng kết %d lô: job trung bình %.1fs (trung vị %.1fs, lớn nhất %.1fs); độ trễ trung bình %.1fs, lớn nhất %.1fs"
          % (len(rows_log), statistics.mean(jobs), statistics.median(jobs), max(jobs), statistics.mean(lats), max(lats)))
    top = sorted(state.items(), key=lambda kv: -kv[1]["total"])[:5]
    print("Top 5 khách theo tổng chi tiêu tích lũy:")
    for cid, s in top:
        print("  %s  %.2f  (%d lượt)" % (cid, s["total"], s["count"]))