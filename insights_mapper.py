#!/usr/bin/env python
# Mapper Hadoop Streaming (chạy được trên Python 2.7 và 3): nhận dòng sự kiện 9 cột, phát khóa tổng hợp.
#   H|thứ|giờ                  -> bảng thứ x giờ
#   P|sản phẩm|danh mục|hãng   -> bảng sản phẩm
# Giá trị: số lượt (1) và giá tính bằng xu (số nguyên, tránh sai số số thực).
from __future__ import print_function
import sys
from datetime import datetime

for line in sys.stdin:
    r = line.rstrip("\r\n").split(",")
    # 0 timestamp, 1 event_type, 2 user_id, 3 product_id, 4 category_id, 5 category_code, 6 brand, 7 price
    if len(r) < 9 or r[1] != "purchase":
        continue
    try:
        t = datetime.strptime(r[0][:19], "%Y-%m-%d %H:%M:%S")
        cents = int(round(float(r[7]) * 100))
    except ValueError:
        continue
    print("H|%d|%d\t1\t%d" % (t.isoweekday(), t.hour, cents))
    print("P|%s|%s|%s\t1\t%d" % (r[3], r[5] or "unknown", r[6] or "unknown", cents))
