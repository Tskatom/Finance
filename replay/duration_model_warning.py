#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


"""
This baseline model following the assumption that, the high-sigma events always
followed by other high-sigma events in soon future
"""


from etool import args, logs, queue
import boto
import datetime as dt
import hashlib
from datetime import datetime
import sys
import math
import json


STOCK_EVENT = "041"
CURRENCY_EVENT = "042"
EVENTS = ["0411", "0412", "0421", "0422"]
COUNTRY_MARKET = {"MERVAL": "Argentina", "USDARS": "Argentina", "IBOV": "Brazil", "USDBRL": "Brazil",
                  "CHILE65": "Chile", "USDCLP": "Chile", "COLCAP": "Colombia", "USDCOP": "Colombia",
                  "CRSMBCT": "Costa Rica", "USDCRC": "Costa Rica", "MEXBOL": "Mexico", "USDMXN": "Mexico",
                  "BVPSBVPS": "Panama", "IGBVL": "Peru", "USDPEN": "Peru", "IBVC": "Venezuela"}

MARKET_TYPE = {"MERVAL": "stock", "USDARS": "currency", "IBOV": "stock", "USDBRL": "currency",
               "CHILE65": "stock", "USDCLP": "currency", "COLCAP": "stock", "USDCOP": "currency",
               "CRSMBCT": "stock", "USDCRC": "currency", "MEXBOL": "stock", "USDMXN": "currency",
               "BVPSBVPS": "stock", "IGBVL": "stock", "USDPEN": "currency", "IBVC": "stock"}

__processor__ = 'duration_analysis_model'
log = logs.getLogger(__processor__)
__version__ = "0.0.1"


class warning():
    def __init__(self, model_name):
        self.warning = {}
        self.probability_flag = "confidenceIsProbability"
        self.warning[self.probability_flag] = "True"
        self.date_lab = "date"
        self.event_date_lab = "eventDate"
        self.population_lab = "population"
        self.probability_lab = "confidence"
        self.model_lab = "model"
        self.warning[self.model_lab] = model_name
        self.id_lab = "embersId"
        self.derived_lab = "derivedFrom"
        self.event_type_lab = "eventType"
        self.comment_lab = "comments"
        self.location_lab = "location"

    def send(self, pub_zmq):
        with queue.open(pub_zmq, "w", capture=True) as q_w:
            q_w.write(self.warning)

    def generateIdDate(self):
        if not self.id_lab in self.warning:
            self.warning[self.id_lab] = hashlib.sha1(str(self.warning)).hexdigest()
        if not self.date_lab in self.warning:
            self.warning[self.date_lab] = datetime.utcnow().isoformat()

    def setEventDate(self, event_date):
        self.warning[self.event_date_lab] = event_date

    def setPopulation(self, population):
        self.warning[self.population_lab] = population

    def setProbability(self, probability):
        self.warning[self.probability_lab] = probability

    def setDerivedFrom(self, derived):
        self.warning[self.derived_lab] = derived

    def setEventType(self, event_type):
        self.warning[self.event_type_lab] = event_type

    def setComments(self, comment):
        self.warning[self.comment_lab] = comment

    def setDate(self, deliver_date):
        self.warning[self.date_lab] = deliver_date

    def setLocation(self, location):
        self.warning[self.location_lab] = location


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


def transfer_zs(zscore):
    #smooth the zscore values
    if zscore > 0:
        zscore = math.floor(zscore)
        if zscore >= 3.0:
            zscore = 3.0
    else:
        zscore = math.ceil(zscore)
        if zscore <= -3.0:
            zscore = -3.0

    return zscore


def durationProcess(rule, conn, enriched_price, zmq_queue, test_flag=False):
    "get the zscores"
    zscore30 = round(float(enriched_price["zscore30"]), 4)
    zscore90 = round(float(enriched_price["zscore90"]), 4)
    z30 = transfer_zs(zscore30)
    z90 = transfer_zs(zscore90)
    var_index = enriched_price["name"]
    "check the triger list"
    for rep_index in rule["rules"]:
        if var_index in rule["rules"][rep_index]:
            var_index_zs = rule["rules"][rep_index][var_index]
            if z30 in var_index_zs.get("z30", {}) or z90 in var_index_zs.get("z90", {}):
                "triger the warning"
                population = rep_index
                finance_type = MARKET_TYPE[rep_index]
                post_date = enriched_price["postDate"]
                country = COUNTRY_MARKET[population]
                if finance_type.lower() == "stock":
                    event_type = STOCK_EVENT
                elif finance_type.lower() == "currency":
                    event_type = CURRENCY_EVENT
                else:
                    raise EmbersException("Unkonown Finance price data: " + finance_type)

                if z30 > .0 or z90 > .0:
                    event_type = "%s%s" % (event_type, "1")
                elif z30 < .0 or zscore90 < .0:
                    event_type = "%s%s" % (event_type, "2")
                else:
                    pass

                "if trigger the threshold, initiate a warning"
                if event_type in EVENTS:
                    warn = warning("Duration Analysis Model")

                    "get the latest trading date since three day's later"
                    event_date = getTradingDate(conn, post_date, country)
                    probability = .6
                    derived_from = {"derivedIds": []}
                    derived_from["derivedIds"].append(enriched_price["embersId"])
                    comment = "Duration analysis Model-Detected High-Sigma Event: %s %s zscore30:%.4f zscore90:%.4f|ruleVersion:%s" % (post_date, var_index, zscore30, zscore90, str(rule["version"]))

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
    ap.add_argument('--replay', action="store_true", help="Test Flag, if contain this argument, it means a test case")
    #if the rule file is not indicated in argument, it need to be load from sys.stdin
    ap.add_argument('--rulefile', type=str, help="The rule file for duration analysis model")
    arg = ap.parse_args()

    if not arg.replay:
        assert arg.sub, 'Need a queue to subscribe to'
    assert arg.pub, 'Need a queue to publish to'

    logs.init(arg)
    queue.init(arg)
    test_flag = arg.replay
    if arg.rulefile:
        rule = eval(open(arg.rulefile).read())
    else:
        #load the rules from sys.stdin
        rule = eval(sys.stdin.read())

    conn = boto.connect_sdb()

    if not arg.replay:
        with queue.open(arg.sub, 'r') as inq:
            for m in inq:
                try:
                    durationProcess(rule, conn, m, arg.pub, test_flag)
                except KeyboardInterrupt:
                    log.info('GOT SIGINT, exiting!')
                    break
                except EmbersException as e:
                    log.exception(e.value)
                except:
                    log.exception("Unexpected exception in process")
    else:
        #replay model take enriched file as input
        enrich_messages = sys.stdin.readlines()
        for m in enrich_messages:
            m = json.loads(m.strip())
            try:
                durationProcess(rule, conn, m, arg.pub, test_flag)
            except KeyboardInterrupt:
                log.info('GOT SIGINT, exiting!')
                break
            except EmbersException as e:
                log.exception(e.value)
            except:
                log.exception("Unexpected exception in process")


if __name__ == "__main__":
    main()
