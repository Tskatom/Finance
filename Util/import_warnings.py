#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import boto
import sqlite3 as lite
import argparse
import json


KEY = "AKIAJZ2N4UOI4TP4YBRQ"
SECRET = "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"


def arg_parser():
    ap = argparse.ArgumentParser("Loading Warnnings from SimpleDB")
    ap.add_argument('--db', dest='db_file',
                    type=str, help="The path of db file")
    ap.add_argument('--file', dest='warn_file',
                    type=str, help="the path of warn file")
    return ap.parse_args()


def clear_db(s_conn):
    cur = s_conn.cursor()
    sql = "delete from warnings"
    cur.execute(sql)
    s_conn.commit()


def load_warning_file(warn_file):
    warnings = []
    with open(warn_file, "r") as w_r:
        for line in w_r:
            try:
                warn = eval(line)
            except:
                warn = json.loads(line)
            warnings.append(extract_warning(warn))

    return warnings


def extract_warning(r):
    warnId = r["embersId"]
    population = r["population"]
    eventCode = r["eventType"]
    eventDate = r["eventDate"]
    location = r["location"]
    "Extract country from location"
    if location[0] == "[":
        location = eval(location)
        country = location[0]
    else:
        country = location

    probability = float(r["confidence"])

    if probability > 1.5:
        probability = probability / 100
    deliveredDate = r["date"][0:10]
    warn = {"warnId": warnId, "population": population,
            "eventCode": eventCode, "eventDate": eventDate,
            "country": country, "deliveredDate": deliveredDate,
            "probability": probability}
    return warn


def load_warning_sim():
    warnings = []
    conn = boto.connect_sdb(KEY, SECRET)
    domain = conn.lookup("warnings")
    rs = domain.select("select embersId, population,eventType, \
                       eventDate,location,confidence,  \
                       date from warnings where delivered != 'False' \
                       and delivered !='DUPLICATE' and mitreId > '1'\
                       and eventType like '04%'")
    for r in rs:
        warnings.append(extract_warning(r))

    return warnings


def check_duplicate_warn(s_conn, warn):
    sql = "select count(*) from warnings where event_date='%s' \
        and event_code='%s' and population='%s'" % \
        (warn['eventDate'], warn['eventCode'], warn['population'])
    cur = s_conn.cursor()
    r = cur.execute(sql).fetchone()
    if r[0] > 0:
        return True
    else:
        return False


def write_db(s_conn, warnings):
    sql = "insert into warnings(warning_id, \
            country, deliver_date, event_date, \
            event_code, population, probability) \
            values (?,?,?,?,?,?,?)"
    cur = s_conn.cursor()
    for warn in warnings:
        if check_duplicate_warn(s_conn, warn):
            continue
        warnId = warn["warnId"]
        population = warn["population"]
        eventCode = warn["eventCode"]
        eventDate = warn["eventDate"]
        country = warn["country"]
        probability = float(warn["probability"])
        deliveredDate = warn["deliveredDate"]

        cur.execute(sql, [warnId, country, deliveredDate,
                          eventDate, eventCode, population, probability, ])

    s_conn.commit()


def main():
    args = arg_parser()
    db_file = args.db_file
    warn_file = args.warn_file
    s_conn = lite.connect(db_file)

    clear_db(s_conn)

    if warn_file is not None:
        warnings = load_warning_file(warn_file)
    else:
        warnings = load_warning_sim()
    write_db(s_conn, warnings)


if __name__ == "__main__":
    main()
