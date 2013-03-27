#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import sqlite3 as lite
import argparse
from datetime import datetime
import math
from munkres import Munkres
import sys


COUNTRY_LIST = ["Chile", "Costa Rica", "Panamá", "Venezuela", "Colombia", "Peru", "Mexico", "Argentina", "Brazil"]


def arg_parser():
    ap = argparse.ArgumentParser("Score the Warning")
    ap.add_argument('-s', dest="start_date", help="the start day to evalue", type=str)
    ap.add_argument('-e', dest="end_date", help="the end day to evalue", type=str)
    ap.add_argument('-db', dest="db_file", help="the gsr database file", type=str)
    return ap.parse_args()


def load_gsr(conn, country, s_date, e_date, eventType="04"):
    gsrEvents = []
    sql = "select event_id, country, event_code, population, event_date from gsr_event \
            where event_date >='%s' and event_date < '%s' and country = '%s' and event_code like '%s' \
            order by event_id asc" % (s_date, e_date, country, eventType + "%")

    cur = conn.cursor()
    for row in cur.execute(sql):
        gsrEvents.append({"eventId": row[0], "country": row[1], "eventCode": row[2], "population": row[3], "eventDate": row[4]})

    return gsrEvents


def load_warning(conn, country, s_date, e_date, eventType="04"):
    warnings = []
    sql = "select warning_id, country, event_code, population, event_date, deliver_date, probability from warnings \
            where event_date >= '%s' and event_date < '%s' and country = '%s' and event_code like '%s' \
            order by warning_id" % (s_date, e_date, country, eventType + "%")

    cur = conn.cursor()
    for row in cur.execute(sql):
        warnings.append({"warningId": row[0], "country": row[1], "eventCode": row[2], "population": row[3], "eventDate": row[4], "deliverDate": row[5], "probability": row[6]})

    return warnings


def score_location(gsr, warn):
    if gsr["country"] == warn["country"]:
        return 1.
    else:
        return .0


def score_eventtype(gsr, warn):
    x1 = .0
    x2 = .0
    x3 = .0
    g_type = gsr["eventCode"]
    w_type = warn["eventCode"]

    if g_type[0:2] == w_type[0:2]:
        x1 = 1.
        if g_type[2] == w_type[2]:
            x2 = 1.
            if g_type[3] == w_type[3]:
                x3 = 1.

    return 1 / 3 * x1 + 1 / 3 * x2 + 1 / 3 * x3


def score_date(gsr, warn):
    g_date = datetime.strptime(gsr["eventDate"], "%Y-%m-%d")
    w_date = datetime.strptime(warn["eventDate"], "%Y-%m-%d")
    gaps = math.fabs((g_date - w_date).days)
    if gaps > 7:
        gaps = 7

    return 1. - gaps / 7.


def get_quality(gsr, warn):
    s_event = score_eventtype(gsr, warn)
    s_date = score_date(gsr, warn)
    s_location = score_location(gsr, warn)

    return (4. / 3) * (s_event + s_date + s_location)


def score_leadtime(gsr, warn):
    g_date = datetime.strptime(gsr["eventDate"], "%Y-%m-%d")
    wd_date = datetime.strptime(warn["deliveredDate"], "%Y-%m-%d")
    return (g_date - wd_date).days


def score_probability(gsr, warn):
    pass


def match(gsr, warn):
    deliverDate = datetime.strptime(warn["deliverDate"], "%Y-%m-%d")
    eventDate = datetime.strptime(gsr["eventDate"], "%Y-%m-%d")
    warningDate = datetime.strptime(warn["eventDate"], "%Y-%m-%d")

    if (deliverDate - eventDate).days >= 0:
        return False

    if math.fabs((eventDate - warningDate).days) > 7:
        return False

    return True


def do_matching(gsrEvents, warnings):
    matrix = []

    M = len(gsrEvents)
    N = len(warnings)
    for m in range(M):
        row = []
        gsr = gsrEvents[m]
        for n in range(N):
            warn = warnings[n]
            if match(gsr, warn):
                row.append(get_quality(gsr, warn))
            else:
                row.append(.0)
        matrix.append(row)

    costMatrix = []
    print "M", M, "N", N
    #create the cost matrix
    for m in range(M):
        r = []
        gsr = gsrEvents[m]
        for n in range(N):
            warn = warnings[n]
            r.append(sys.maxint - matrix[m][n])
        costMatrix.append(r)

    m = Munkres()
    if M > 0 and N > 0:
        indexes = m.compute(costMatrix)
    else:
        return None, None

    return matrix, indexes


def main():
    args = arg_parser()
    s_date = args.start_date
    e_date = args.end_date
    db_file = args.db_file

    conn = lite.connect(db_file)
    for country in COUNTRY_LIST:
        gsrEvents = load_gsr(conn, country, s_date, e_date)
        warnings = load_warning(conn, country, s_date, e_date)
        score_matrix, indexes = do_matching(gsrEvents, warnings)
        print country, score_matrix, indexes
        "Computing the lead-time and prability"

if  __name__ == "__main__":
    main()
