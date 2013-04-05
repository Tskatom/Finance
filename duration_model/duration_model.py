#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


"""
This baseline model following the assumption that, the high-sigma events always
followed by other high-sigma events in soon future
"""


import warning
from etool import args, logs, queue
import boto
import datetime as dt


STOCK_EVENT = "041"
CURRENCY_EVENT = "042"
EVENTS = ["0411", "0412", "0421", "0422"]
COUNTRY_MARKET = {"MERVAL": "Argentina", "USDARS": "Argentina", "IBOV": "Brazil", "USDBRL": "Brazil",
                  "CHILE65": "Chile", "USDCLP": "Chile", "COLCAP": "Colombia", "USDCOP": "Colombia",
                  "CRSMBCT": "Costa Rica", "USDCRC": "Costa Rica", "MEXBOL": "Mexico", "USDMXN": "Mexico",
                  "BVPSBVPS": "Panama", "IGBVL": "Peru", "USDPEN": "Peru", "IBVC": "Venezuela"}

__processor__ = 'duration_analysis_model'
log = logs.getLogger(__processor__)
__version__ = "0.0.1"


class EmbersException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def checkTradingDay(t_domain, t_date, country):
    "Check weekend"
    week_day = dt.datetime.strptime(t_date, "%Y-%m-%d").weekday()
    if week_day == 5 or week_day == 6:
        return False

    "Check holiday"
    sql = "select count(*) from s_holiday where country='%s' and date ='%s'" % (country, t_date)
    rs = t_domain.select(sql)
    count = 0
    for r in rs:
        count = int(r['Count'])

    if count == 0:
        return True
    else:
        return False


def getTradingDate(conn, post_date, country):
    event_date = dt.datetime.strptime(post_date, "%Y-%m-%d") + dt.timedelta(days=3)
    t_domain = conn.get_domain('s_holiday')
    i = 0
    while True:
        str_date = dt.datetime.strftime(event_date, "%Y-%m-%d")
        if checkTradingDay(t_domain, str_date, country):
            return str_date
        event_date += dt.timedelta(days=1)
        i += 1
        if i > 10:
            raise EmbersException("The duration '%d' between current high-sigma date '%s'  and next one is too long" % (i, post_date))
            return None


def durationProcess(conn, enriched_price, zmq_queue, test_flag=False):
    "get the zscores"
    zscore30 = round(float(enriched_price["zscore30"]), 4)
    zscore90 = round(float(enriched_price["zscore90"]), 4)
    population = enriched_price["name"]
    finance_type = enriched_price["type"]
    post_date = enriched_price["postDate"]
    country = COUNTRY_MARKET[population]

    if finance_type.lower() == "stock":
        event_type = STOCK_EVENT
    elif finance_type.lower() == "currency":
        event_type = CURRENCY_EVENT
    else:
        raise EmbersException("Unkonown Finance price data: " + finance_type)

    if zscore30 > 4. or zscore90 > 3.:
        event_type = "%s%s" % (event_type, "1")
    elif zscore30 < -4. or zscore90 < -3.:
        event_type = "%s%s" % (event_type, "2")
    else:
        pass

    "if trigger the threshold, initiate a warning"
    if event_type in EVENTS:
        warn = warning.warning("Duration Analysis Model")

        "get the latest trading date since three day's later"
        event_date = getTradingDate(conn, post_date, country)
        probability = .6
        derived_from = {"derivedIds": []}
        derived_from["derivedIds"].append(enriched_price["embersId"])
        comment = "Duration analysis Model-Detected High-Sigma Event: %s %s zscore30:%.4f zscore90:%.4f" % (post_date, population, zscore30, zscore90)

        warn.setEventDate(event_date)
        warn.setPopulation(population)
        warn.setProbability(probability)
        warn.setDerivedFrom(derived_from)
        warn.setEventType(event_type)
        warn.setComments(comment)
        warn.setLocation(country)
        if test_flag:
            warn.setDate(post_date)
        warn.generateIdDate()

        warn.send(zmq_queue)


def main():
    ap = args.get_parser()
    ap.add_argument('--test', action="store_true", help="Test Flag, if contain this argument, it means a test case")
    arg = ap.parse_args()

    assert arg.sub, 'Need a queue to subscribe to'
    assert arg.pub, 'Need a queue to publish to'

    logs.init(arg)
    queue.init(arg)
    test_flag = arg.test

    conn = boto.connect_sdb()

    with queue.open(arg.sub, 'r') as inq:
        for m in inq:
            try:
                durationProcess(conn, m, arg.pub, test_flag)
            except KeyboardInterrupt:
                log.info('GOT SIGINT, exiting!')
                break
            except EmbersException as e:
                log.exception(e.value)
            except:
                log.exception("Unexpected exception in process")


if __name__ == "__main__":
    main()
