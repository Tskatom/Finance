#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import boto
import sqlite3 as lite
import argparse


KEY = "AKIAJZ2N4UOI4TP4YBRQ"
SECRET = "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"


def arg_parser():
    ap = argparse.ArgumentParser("Loading Warnnings from SimpleDB")
    ap.add_argument('-d', dest='db_file', type=str, help="The path of db file")
    return ap.parse_args()


def clear_db(s_conn):
    cur = s_conn.cursor()
    sql = "delete from warnings"
    cur.execute(sql)
    s_conn.commit()


def load_warning_sim():
    warnings = []
    conn = boto.connect_sdb(KEY, SECRET)
    domain = conn.get_domain("warnings")
    rs = domain.select("select embersId, population,eventType, eventDate,location,confidence,  date from warnings where delivered != 'False' and eventType like '04%'")
    for r in rs:
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
        warnings.append({"warnId": warnId, "population": population, "eventCode": eventCode, "eventDate": eventDate, "country": country, "deliveredDate": deliveredDate, "probability": probability})

    return warnings


def write_db(s_conn, warnings):
    sql = "insert into warnings(warning_id, country, deliver_date, event_date, event_code, population, probability) values (?,?,?,?,?,?,?)"
    cur = s_conn.cursor()
    for warn in warnings:
        warnId = warn["warnId"]
        population = warn["population"]
        eventCode = warn["eventCode"]
        eventDate = warn["eventDate"]
        country = warn["country"]
        probability = float(warn["probability"])
        deliveredDate = warn["deliveredDate"]

        cur.execute(sql, [warnId, country, deliveredDate, eventDate, eventCode, population, probability, ])

    s_conn.commit()


def main():
    args = arg_parser()
    db_file = args.db_file
    s_conn = lite.connect(db_file)
    clear_db(s_conn)
    warnings = load_warning_sim()
    write_db(s_conn, warnings)


if __name__ == "__main__":
    main()
