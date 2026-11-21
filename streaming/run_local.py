"""
Mô phỏng pipeline Streaming trên máy (không cần shell Unix):
  cat input | python mapper.py | sort | python reducer.py

  python streaming/run_local.py data/clean/sample_events.csv data/local_out/stream_sample
Kết quả ghi vào <thư mục>/part-00000, kiểm tra bằng preprocess/04_compare.py.
"""
import os
import subprocess
import sys
import time

if len(sys.argv) != 3:
    sys.exit(__doc__)
src, out_dir = sys.argv[1], sys.argv[2]
here = os.path.dirname(os.path.abspath(__file__))
os.makedirs(out_dir, exist_ok=True)

t0 = time.time()
with open(src, "rb") as f:
    mapped, err = subprocess.Popen([sys.executable, os.path.join(here, "mapper.py")],
                                   stdin=f, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
print(err.decode().strip())
lines = sorted(mapped.splitlines(keepends=True))          # giống pha shuffle/sort theo khóa
reduced, _ = subprocess.Popen([sys.executable, os.path.join(here, "reducer.py")],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE).communicate(b"".join(lines))
with open(os.path.join(out_dir, "part-00000"), "wb") as f:
    f.write(reduced)
n_customers = reduced.count(b"\n")
print(f"Xong: {len(lines):,} dòng map, {n_customers:,} khách, {time.time()-t0:.1f}s -> {out_dir}")
