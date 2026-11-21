#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Hadoop Streaming reducer (Python 2.7 và 3). Input đã sắp theo khóa (user_id).
Output cùng định dạng job Java:
  customer_id <TAB> total_spent <TAB> num_purchases <TAB> avg_amount <TAB> first_purchase <TAB> last_purchase
"""
import math
import sys
from itertools import groupby


def text(b):
    return b if isinstance(b, str) else b.decode("latin-1")


def records(stdin):
    for raw in stdin:
        parts = raw.rstrip(b"\r\n").split(b"\t")
        if len(parts) != 3:
            continue
        yield parts[0], float(parts[1]), parts[2]


def main():
    stdin = getattr(sys.stdin, "buffer", sys.stdin)
    for user, group in groupby(records(stdin), key=lambda r: r[0]):
        prices = []
        first = last = None
        for _, price, ts in group:
            prices.append(price)
            if first is None or ts < first:
                first = ts
            if last is None or ts > last:
                last = ts
        total = math.fsum(prices)
        n = len(prices)
        sys.stdout.write("%s\t%.2f\t%d\t%.2f\t%s\t%s\n"
                         % (text(user), total, n, total / n, text(first), text(last)))


if __name__ == "__main__":
    main()
