#!/usr/bin/env python
# Reducer Hadoop Streaming: cộng số lượt và doanh thu (xu) theo khóa, xuất doanh thu dạng thập phân.
from __future__ import print_function
import sys

cur, c, s = None, 0, 0
for line in sys.stdin:
    parts = line.rstrip("\r\n").split("\t")
    if len(parts) != 3:
        continue
    k, n, v = parts
    if k != cur:
        if cur is not None:
            print("%s\t%d\t%d.%02d" % (cur, c, s // 100, s % 100))
        cur, c, s = k, 0, 0
    c += int(n)
    s += int(v)
if cur is not None:
    print("%s\t%d\t%d.%02d" % (cur, c, s // 100, s % 100))
