#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import argparse
import boto
import codecs
import json


def load_warning(model, start_date, end_date):
    conn = boto.connect_sdb()
    t_domain = conn.lookup('warnings')
    sql = "select * from warnings where date>='%s' and date <='%s' "
    sql += " and eventType in ('0411', '0412') "
    sql += "and model='%s' and mitreId >'1'"
    sql = sql % (start_date, end_date, model)
    result = t_domain.select(sql)
    result = [r for r in result]
    w_out = codecs.getwriter('utf8')(sys.stdout)
    for r in result:
        w_out.write(json.dumps(r, ensure_ascii=False) + "\n")
    return result


def create_duration_rule():
    indices = ["MERVAL", "MEXBOL", "CRSMBCT", "IGBVL", "IBVC",
               "COLCAP", "BVPSBVPS", "CHILE65", "IBOV", "USDARS",
               "USDBRL", "USDMXN", "USDCOP", "USDCLP", "USDCRC",
               "USDPEN"]
    rule = {"rules": {}, "version": '1'}
    for index in indices:
        rule['rules'][index] = {index: {'z30': {-3.0: 0.5, 3.0: 0.5},
                                        'z90': {-3.0: 0.5, 3.0: 0.5}}}
        with open('duration_rule_v1.txt', 'w') as w:
            w.write(str(rule))
            w.flush()


def out_duration_enriched(rawfile):
    conn = boto.connect_sdb()
    t_domain = conn.lookup("t_enriched_bloomberg_prices")
    sql = "select * from t_enriched_bloomberg_prices where name='%s' and "
    sql += " postDate='%s'"
    with open("replay_duration_enrich.txt", "w") as ew, open(rawfile) as rf:
        for raw in rf:
            raw_m = json.loads(raw)
            postDate = raw_m["date"][0:10]
            name = raw_m["name"]
            f_sql = sql % (name, postDate)
            rs = t_domain.select(f_sql)
            for r in rs:
                ew.write(json.dumps(r) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--model', type=str, help='the model name')
    ap.add_argument('--start', type=str, help='start date')
    ap.add_argument('--end', type=str, help='end date')
    arg = ap.parse_args()

    assert arg.model, 'Please input a model name'
    assert arg.start, 'please input a start date'
    assert arg.end, 'Please input a end date'

    load_warning(arg.model, arg.start, arg.end)

if __name__ == "__main__":
    main()
