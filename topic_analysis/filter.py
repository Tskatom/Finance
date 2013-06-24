#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


def load():
    price_f = open("merval.txt", "r")
    trend_f = open("topic_trend.txt", "r")

    price_data = {}
    for line in price_f:
        d = line.split("|")
        price_data[d[0]] = line

    trend_data = {}
    for line in trend_f.readlines()[1:]:
        d = line.split("|")
        trend_data[d[0]] = line

    print trend_data

    new_trend = []
    for day in price_data:
        if day in trend_data:
            new_trend.append([day, trend_data[day]])

    new_trend.sort(key=lambda new_trend: new_trend[0])

    with open("new_trend_topic.txt", "w") as w:
        for d in new_trend:
            w.write(d[1])


if __name__ == "__main__":
    load()
