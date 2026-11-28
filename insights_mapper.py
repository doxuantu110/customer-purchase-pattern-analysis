#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Mapper Hadoop Streaming (chay duoc tren Python 2.7 va 3): nhan dong su kien 9 cot, phat khoa tong hop.
#   H|thu|gio                  -> bang thu x gio
#   P|san pham|danh muc|hang   -> bang san pham
# Gia tri: so luot (1) va gia tinh bang xu (so nguyen, tranh sai so so thuc).
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
