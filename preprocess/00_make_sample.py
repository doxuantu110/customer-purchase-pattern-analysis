"""Tạo file mẫu từ file CSV lớn (lấy N dòng đầu + dòng tiêu đề).

Chạy từ gốc repo:
    python preprocess/00_make_sample.py                 # mặc định 1 triệu dòng
    python preprocess/00_make_sample.py 2019-Oct.csv 500000
Đầu vào : data/<tên file>   (mặc định 2019-Oct.csv)
Đầu ra  : data/sample_<N>.csv
"""
import sys
from itertools import islice
from pathlib import Path

name = sys.argv[1] if len(sys.argv) > 1 else "2019-Oct.csv"
n = int(sys.argv[2]) if len(sys.argv) > 2 else 1_000_000

src = Path("data") / name
dst = Path("data") / f"sample_{n}.csv"
if not src.exists():
    raise SystemExit(f"Không thấy {src}")

with src.open("r", encoding="utf-8", newline="") as fin, \
     dst.open("w", encoding="utf-8", newline="") as fout:
    lines = list(islice(fin, n + 1))        # +1 cho dòng tiêu đề
    fout.writelines(lines)

print(f"Đã ghi {len(lines) - 1:,} dòng dữ liệu vào {dst}")
print("Dòng tiêu đề:", lines[0].strip())
print("Dòng dữ liệu đầu tiên:", lines[1].strip())
