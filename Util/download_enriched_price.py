#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import os
import boto


def download():
    conn = boto.connect_sdb()
    domain = conn.lookup("t_enriched_bloomberg_prices")
    sql = "select * from t_enriched_bloomberg_prices where postDate >='2012-12-01' and postDate<='2013-07-01'"
    rs = domain.select(sql)
    dir = "/home/vic/workspace/data/stock/enriched"
    for r in rs:
        print r
        name = r["name"]
        z30 = float(r["zscore30"])
        z90 = float(r["zscore90"])
        postDate = r["postDate"]
        file_name = os.path.join(dir, "%s_eprice" % name)
        with open(file_name, 'a') as ap:
            p_str = "%s|%s|%.4f|%.4f\n" % (name, postDate, z30, z90)
            ap.write(p_str)


if __name__ == "__main__":
    download()
