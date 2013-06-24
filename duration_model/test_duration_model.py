#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


from etool import args, logs, queue
import boto
import json


__processor__ = 'test_duration_model'
log = logs.getLogger(__processor__)


def get_enriched_prices(t_domain, s_date, e_date):
    sql = "select * from t_enriched_bloomberg_prices where postDate >='%s' and postDate <= '%s'" % (s_date, e_date)
    rs = t_domain.select(sql)
    return rs

if __name__ == "__main__":
    ap = args.get_parser()
    ap.add_argument('--s_date', type=str, help="the start date of the query")
    ap.add_argument('--e_date', type=str, help='the end date of the query')
    ap.add_argument('--f', action='store_true', help='load enriched message from file')
    ap.add_argument('--sdb', action='store_true', help='load enriched message from simpledb')
    ap.add_argument('--file', type=str, help="the file location")
    arg = ap.parse_args()

    assert arg.pub, 'Need a queue to publish'

    logs.init(arg)
    queue.init(arg)
    if arg.sdb:
        conn = boto.connect_sdb()
        t_domain = conn.get_domain('t_enriched_bloomberg_prices')
        rs = get_enriched_prices(t_domain, arg.s_date, arg.e_date)

    if arg.f:
        with open(arg.file, "r") as r:
            rs = [eval(line.strip()) for line in r.readlines()]

    with queue.open(arg.pub, 'w') as q_w, open("surrogate.txt", "w") as s_w:
        for r in rs:
            print r
            q_w.write(r)
            s_w.write(json.dumps(r) + "\n")
