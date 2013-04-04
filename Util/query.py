#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import boto
import argparse
import math
import sqlite3 as lite


KEY = "AKIAJZ2N4UOI4TP4YBRQ"
SECRET = "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"


def parse_args():
    ap = argparse.ArgumentParser("Search the SimpleDB to return prices data")
    ap.add_argument('-s', dest="start_date", metavar="start date to query", type=str)
    ap.add_argument('-e', dest="end_date", help="end date to query", type=str)
    ap.add_argument('-t', dest="table_name", help="table to query", type=str)
    ap.add_argument('-n', dest="name", help="name of the stock or currency", type=str, nargs='?')
    ap.add_argument('-ty', dest="type", help="type of query: index or currency", nargs='?')
    ap.add_argument('-sig', dest="sigma", help="flag to check if filter sigma events", nargs='?')
    ap.add_argument('-gsr', dest="gsr", help="the gsr db file", type=str, nargs='?')
    ap.add_argument('-sim', dest="sim", help="flag to check if display simpleDB data", type=str, nargs='?')
    return ap.parse_args()


def get_domain(conn, t_name):
    return conn.get_domain(t_name)


def query(conn, s_date, e_date, t_name, name=None, type=None, sigma=None):
    sql = "select * from %s where postDate >= '%s' and postDate <= '%s' " % (t_name, s_date, e_date)
    if name is not None:
        sql = "%s and name = '%s' " % (sql, name)
    if type is not None:
        sql = "%s and type = '%s'" % (sql, type)

    print sql

    domain = get_domain(conn, t_name)
    rs = domain.select(sql)
    print "\n%s\t%s\t%s\t%s\t%s\t%s" % ("postDate", "Name", "zscore30", "zscore90", "currentValue", "changePercent")
    for r in rs:
        if sigma is not None:
            if math.fabs(float(r["zscore30"])) >= 4. or math.fabs(float(r["zscore90"])) >= 3.:
                print "%s\t%s\t%s\t%s\t%s\t%s" % (r["postDate"], r["name"], r["zscore30"], r["zscore90"], r["currentValue"], r["changePercent"])
        else:
            print r

    print "\n"


def get_gsr(sdb_conn, s_date, e_date, name=None, type=None):
    sql = "select country, event_code, population, event_date from gsr_event where event_date >='%s' and event_date <= '%s' " % (s_date, e_date)
    if name is not None:
        sql = "%s and population = '%s'" % (sql, name)

    if type == "index":
        sql = "%s and event_code in ('0411','0412')" % (sql)
    elif type == "currency":
        sql = "%s and event_code in ('0421','0422')" % (sql)

    sql = "%s order by event_date asc" % (sql)

    cur = sdb_conn. cursor()
    print "\n GSR Events From %s to %s" % (s_date, e_date)
    for row in cur.execute(sql):
        print "%s\t%s\t%s\t%s" % (row[3], row[2], row[1], row[0])


def main():
    args = parse_args()
    s_date = args.start_date
    e_date = args.end_date
    t_name = args.table_name
    name = args.name
    type = args.type
    sigma = args.sigma
    gsr = args.gsr
    sim = args.sim

    print args

    if gsr is not None:
        sdb_conn = lite.connect(gsr)
        get_gsr(sdb_conn, s_date, e_date, name, type)

    if sim is not None:
        print "-----"
        conn = boto.connect_sdb(KEY, SECRET)
        query(conn, s_date, e_date, t_name, name, type, sigma)

if __name__ == "__main__":
    main()
