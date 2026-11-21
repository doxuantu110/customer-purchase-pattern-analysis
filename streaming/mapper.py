#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Hadoop Streaming mapper (chạy được với Python 2.7 và 3).
Input : timestamp,event_type,user_id,product_id,category_id,category_code,brand,price,user_session
Output: user_id <TAB> price <TAB> timestamp        (chỉ dòng purchase hợp lệ)
Counter (nhóm "retail") gửi qua stderr theo giao thức reporter của Streaming.
"""
import sys


def main():
    stdin = getattr(sys.stdin, "buffer", sys.stdin)
    stdout = getattr(sys.stdout, "buffer", sys.stdout)
    purchase = skipped = invalid = 0
    for raw in stdin:
        f = raw.rstrip(b"\r\n").split(b",")
        if len(f) != 9:
            invalid += 1
            continue
        if f[1] != b"purchase":
            skipped += 1
            continue
        ts, user, price = f[0], f[2], f[7]
        try:
            p = float(price)
        except ValueError:
            invalid += 1
            continue
        if not user or not ts or not p > 0:
            invalid += 1
            continue
        stdout.write(user + b"\t" + price + b"\t" + ts + b"\n")
        purchase += 1
    sys.stderr.write("reporter:counter:retail,PURCHASE_RECORDS,%d\n" % purchase)
    sys.stderr.write("reporter:counter:retail,SKIPPED_NON_PURCHASE,%d\n" % skipped)
    if invalid:
        sys.stderr.write("reporter:counter:retail,INVALID_RECORDS,%d\n" % invalid)


if __name__ == "__main__":
    main()
