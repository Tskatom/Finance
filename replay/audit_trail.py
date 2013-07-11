#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

import sys
import boto
import json
from datetime import datetime, timedelta
from boilerpipe.extract import Extractor
import urllib2
import codecs


def query_keywords(domain, start_date, end_date, index):
    eids = set()
    terms = {}
    sql = "SELECT * FROM bloomberg_keywords WHERE date >= '%s' "
    sql += "AND date <= '%s' AND stockIndex = '%s'"
    sql = sql % (start_date, end_date, index)
    rs = domain.select(sql)
    for r in rs:
        t = r["term"]
        terms[t] = terms.get(t, 0) + int(r['count'])
        eids.add(r['embersId'])
    return terms, eids


def get_term_list(domain, predict_date, stock_index, duration=3):
    predict_date = datetime.strptime(predict_date, "%Y-%m-%d")
    start_day = (predict_date - timedelta(days=duration)).strftime("%Y-%m-%d")
    end_day = (predict_date).strftime("%Y-%m-%d")
    term_list, news_derived = query_keywords(domain, start_day,
                                             end_day, stock_index)
    return term_list, news_derived


def get_bayesian_trail(domain, main_table, eid):
    #get warning
    sql = "select * from warnings where embersId = '%s'" % eid
    rs = domain.select(sql)
    warning = None
    for r in rs:
        warning = r
    if warning is None:
        print "No such Warning %s" % eid
        sys.exit(1)
    del warning["mitreId"]
    del warning["mitreMessage"]
#    print json.dumps(warning, indent=3)
    #get surrogate data
    derived_info = eval(warning["derivedFrom"])
    warning["derived_from"] = []

    for derivedId in derived_info["derivedIds"]:
        #search from sdb to get surrogate data
        sql = "select * from t_surrogatedata where embersId='%s'" % derivedId
        rs = domain.select(sql)
        surrogate = None
        for r in rs:
            surrogate = r
        if surrogate is None:
            print "No surrogate", eid, "\n", derivedId
            continue
#        print json.dumps(surrogate, indent=3)
        surrogate["derived_from"] = []
        predict_date = surrogate["shiftDate"]
        stock_index = surrogate["population"]
        #get the past three day's enriched price data
        sql = "select * from t_enriched_bloomberg_prices where name='%s' "
        sql += " and postDate < '%s' order by postDate desc "
        sql = sql % (stock_index, predict_date)
        price_rs = domain.select(sql, max_items=10)
        enriched_prices = [r for r in price_rs]

        #get past 3 day's keywords and news
        terms, article_ids = get_term_list(domain, predict_date, stock_index)
        articles = []
        if len(article_ids) > 0:
            #print "warning[%s] Document Ids:--- %s" % (eid, article_ids)
            for article_id in article_ids:
                article = get_article_by_id(main_table, article_id)
                if article:
                    articles.append(article)

        surrogate["derived_from"] = {"0": articles,
                                     "1": terms,
                                     "2": enriched_prices}
        del warning["derivedFrom"]

        warning["derived_from"].append(surrogate)
    return warning


def get_article_by_id(main_table, eid):
    try:
        item = main_table.get_item(hash_key=eid)
        inFrom = item["From"]
        inTo = item["To"]
        inFile = item["In"]
        raw_text = get_content_byoffset(inFile, inFrom, inTo)
        news = json.loads(raw_text)
        extractor = Extractor(extractor='ArticleExtractor',
                              html=news["content"])
        content = extractor.getText().encode("utf-8")
        author = news["author"]
        url = news["url"]
        postDate = news["postTime"]
        title = news["title"]
        s_news = {"title": title, "url": url,
                  "postDate": postDate, "content": content,
                  "author": author}
        return s_news
    except:
        print "Can not get the article:%s\n %s " % (eid, sys.exc_info()[0])
        return None


def get_content_byoffset(url, start, end):
    req = urllib2.Request(url)
    req.headers["Range"] = "bytes=%s-%s" % (start, end)
    f = urllib2.urlopen(req)
    content = f.read()
    return content


def bayesian():
    conn_sdb = boto.connect_sdb()
    domain = conn_sdb.lookup("warnings")

    conn_dyn = boto.connect_dynamodb()
    main_table = conn_dyn.lookup("LocationsAndOffsets")

    sql = "select embersId from warnings where mitreId >'1' and "
    sql += " model = 'Bayesian - Time serial Model' and date >'2013-05-01' "

    rs = domain.select(sql)
    at_file = "bayesian_audit_trail.txt"
    atw = codecs.open(at_file, "w")
    for r in rs:
        eid = r["embersId"]
        audit_trail = get_bayesian_trail(domain, main_table, eid)
        j_str = json.dumps(audit_trail)
        atw.write(j_str + "\n")
    atw.flush()
    atw.close()


def get_duration_trail(domain, eid):
    sql = "select * from warnings where embersId = '%s' " % eid
    rs = domain.select(sql)
    warning = None

    for r in rs:
        warning = r
    if warning is None:
        print "There is no such warning : %s" % eid
        return

    #get the derived message
    trig_message = None
    derivedIds = eval(warning["derivedFrom"])["derivedIds"]
    for did in derivedIds:
        sql = "select * from t_enriched_bloomberg_prices "
        sql += "where embersId = '%s' " % did
        rs = domain.select(sql)
        for r in rs:
            trig_message = r

    # choose proper rule file, if warning
    # sent before 2013-05-28, choose rule file v1
    # else choose rule file v2
    deliver_date = warning["date"][0:10]
    if deliver_date <= "2013-05-28":
        r_version = "v3"
    else:
        r_version = "v3"

    rule_file = "duration_rule_%s.txt" % r_version

    tar_index = warning["population"]
    rule = eval(open(rule_file, 'r').read())
    rule_index = rule["rules"][tar_index].keys()
    rule_index.append(tar_index)
    rule_index = set(rule_index)

    rule_message = {}
    for r_index in rule_index:
        sql = "select * from t_enriched_bloomberg_prices "
        sql += "where name='%s' " % r_index
        sql += " and postDate <= '%s' order by postDate desc " % deliver_date
        rs = domain.select(sql, max_items=12)
        rule_message[r_index] = [r for r in rs]

    #construct derivedFrom message
    derived_from = {}
    derived_from["0"] = trig_message
    derived_from[1] = rule_message
    derived_from["2"] = rule["rules"][tar_index]
    warning["derived_from"] = []
    warning["derived_from"].append(derived_from)
    del warning["derivedFrom"]

    return warning


def duration():
    conn_sdb = boto.connect_sdb()
    domain = conn_sdb.lookup("warnings")
    sql = "select embersId from warnings where mitreId > '1' "
    sql += " and model='Duration Analysis Model'  and date > '2013-05-01' "

    rs = domain.select(sql)
    at_file = "duration_audit_trail.txt"
    atw = codecs.open(at_file, 'w')
    for r in rs:
        eid = r['embersId']
        audit_trail = get_duration_trail(domain, eid)
        atw.write(json.dumps(audit_trail) + "\n")
    atw.flush()
    atw.close()


def main():
#    duration()
    bayesian()


if __name__ == "__main__":
    main()
