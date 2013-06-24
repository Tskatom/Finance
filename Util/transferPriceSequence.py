#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import sqlite3 as lite
import math


def create_table(db_file):
    conn = lite.connect(db_file)
    sql = "create table price_sequence(name text, type text, post_date date, z30 int, z90 int, PRIMARY KEY(name, post_date))"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    conn.close()


def transfer_f(zscore):
    if zscore > 0:
        zscore = math.floor(zscore)
        if zscore >= 3:
            zscore = 3.0
    else:
        zscore = math.ceil(zscore)
        if zscore <= -3.0:
            zscore = -3.0
    #merge -0 and 0 into one value
    if zscore < 0.5 and zscore > -0.5:
        zscore = 0.0
    return int(zscore)


def output_sequence(db_file, out_file):
    conn = lite.connect(db_file)
    sql = "select type, name, post_date, zscore30, zscore90 from t_enriched_bloomberg_prices where post_date >='2008-01-01' order by name,post_date"
    cur = conn.cursor()
    rs = cur.execute(sql).fetchall()
    with open(out_file, "w") as w:
        w.write("type|name|post_date|z30|z90\n")
        for r in rs:
            type = r[0]
            name = r[1]
            post_date = r[2]
            z30 = transfer_f(r[3])
            z90 = transfer_f(r[4])
            str = "%s|%s|%s|%.2f|%.2f\n" % (type, name, post_date, z30, z90)
            w.write(str)


if __name__ == "__main__":
    pass
