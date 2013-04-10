#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import sqlite3 as lite
from etool import args


def get_data(conn, ticker_list):
    cur = conn.cursor()

    for ticker in ticker_list:
        with open('./data/' + ticker + ".txt", "w") as o_w:
            sql = "select post_date, zscore30, zscore90 from t_enriched_bloomberg_prices where post_date >='2011-01-01' and post_date <= '2012-11-01' and name = '%s' order by post_date asc" % (ticker)
            o_w.write("post_date\tz30\tz90\n")
            rs = cur.execute(sql)
            for r in rs:
                o_w.write("%s\t%.2f\t%.2f\n" % (r[0], r[1], r[2]))


def merg_data(ticker_list):
    merged = []
    o_file = ""
    for m in ticker_list:
        ds = {}
        o_file += m + "_"
        with open('./data/' + m + ".txt", "r") as o_r:
            l = o_r.readline()
            for l in o_r.readlines():
                dt = l.replace("\n", "").split("\t")
                ds[dt[0]] = [dt[1], dt[2]]
        merged.append(ds)

    sortedD = sorted(merged[0].keys())

    o_file += ".txt"
    with open('./data/' + o_file, "w") as o_w:
        title = ["post_date"]
        for m in ticker_list:
            title.append(m + "_z30")
            title.append(m + "_z90")

        o_w.write("\t".join(title) + "\n")

        for d in sortedD:
            flag = True
            w = []
            for me in merged:
                z = me.get(d, None)
                if z is None:
                    flag = False
                    break
                w.extend(z)

            if not flag:
                continue

            o_w.write(d + "\t" + "\t".join(w) + "\n")


def main():
    ap = args.get_parser()
    ap.add_argument('--db', help="the path of sqlite db")
    ap.add_argument('--ts', type=str, nargs='+', help="the list of tickers")
    ap.add_argument('--merge', type=str, nargs='+', help='merge files with same date')
    arg = ap.parse_args()

    ts = arg.ts
    db = arg.db
    m_list = arg.merge

    if m_list:
        merg_data(m_list)

    if ts and db:
        conn = lite.connect(arg.db)
        ts = arg.ts
        get_data(conn, ts)


if __name__ == "__main__":
    main()
