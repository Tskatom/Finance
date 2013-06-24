#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import calculator
import hashlib
import os
import boto


def enrich(raw_prices):
    en_prices = []
    for i, price in enumerate(raw_prices[0: -1]):
        #get the past 30 day's daily change
        past30_changes = [float(p["currentValue"]) - float(p["previousClosePrice"]) for p in raw_prices[i + 1: i + 30]]
        day_change = float(price["currentValue"]) - float(price["previousClosePrice"])
        z30 = calculator.calZscore(past30_changes, day_change)

        past90_changes = [float(p["currentValue"]) - float(p["previousClosePrice"]) for p in raw_prices[i + 1: i + 90]]
        z90 = calculator.calZscore(past90_changes, day_change)

        en_message = {}
        en_message["postDate"] = price["date"][0:10]
        en_message["zscore30"] = round(z30, 4)
        en_message["zscore90"] = round(z90, 4)
        en_message["derivedFrom"] = {"derivedIds": [price["embersId"]]}
        en_message["type"] = price["type"]
        en_message["currentValue"] = price["currentValue"]
        en_message["name"] = price["name"]
        en_message["previousCloseValue"] = price["previousClosePrice"]
        en_message["oneDayChange"] = round(day_change, 4)
        en_message["changePercent"] = round(day_change / price["currentValue"], 4)
        en_message["embersId"] = hashlib.sha1(str(en_message)).hexdigest()

        en_prices.append(en_message)

    return en_prices


def process(in_dir, out_dir):
    #get all the file need to be processed
    files = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f))]
    out_file = open(os.path.join(out_dir, "enriched_price.txt"), "w")

    for f in files:
        raw_prices = []
        with open(os.path.join(in_dir, f), "r") as r:
            for line in r.readlines():
                raw_prices.append(eval(line.strip()))

        en_prices = enrich(raw_prices)
        #write enriched message into file
        for e_price in en_prices:
            out_file.write(str(e_price) + "\n")

        out_file.flush()

    out_file.flush()
    out_file.close()


def add_enriched_price(out_file):
    conn = boto.connect_sdb()
    t_domain = conn.lookup("t_enriched_bloomberg_prices")
    sql = "select * from t_enriched_bloomberg_prices where postDate >='2008-01-01' and postDate <= '2013-05-24'"
    rs = t_domain.select(sql)
    with open(out_file, "a") as a:
        for r in rs:
            a.write(str(r) + "\n")

    conn.close()


def crate_test_enrich(in_dir, start_date, end_date):
    #load data from enriched file
    in_file = os.path.join(in_dir, "enriched_price.txt")
    en_messages = {}
    with open(in_file, "r") as r:
        for line in r.readlines():
            m = eval(line.strip())
            p_date = m["postDate"]
            if p_date >= start_date and p_date < end_date:
                if p_date not in en_messages:
                    en_messages[p_date] = []
                en_messages[p_date].append(m)

    #write the test data into file inorder from past to present
    dates = sorted(en_messages.keys())
    test_file = os.path.join(in_dir, "test_%s_%s.txt" % (start_date, end_date))
    with open(test_file, "w") as w:
        for d in dates:
            ps = en_messages[d]
            for p in ps:
                w.write(str(p) + "\n")


if __name__ == "__main__":
    pass
