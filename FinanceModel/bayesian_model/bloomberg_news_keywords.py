#!/usr/bin/env python

import sys
import os.path
import codecs
import json
import boto

DOMAIN = 'bloomberg_keywords'

def get_domain():
    """Get the storage domain (table) for the Bloomberg data."""
    conn = boto.connect_sdb()
    conn.create_domain(DOMAIN) # you can create repeatedly
    return conn.get_domain(DOMAIN)

def store_keywords(keywords, message, domain=None):
    if not domain:
        domain = get_domain()

    keys = set(['embersId', 'date', 'postTime', 'stockIndex'])
    datum = {k: v for (k,v) in message.items() if k in keys }
    for (kw, c) in keywords.items():
        d = datum.copy()
        d['term'] = kw
        d['count'] = c
        id = "%s_%s_%s_%s" % (d['embersId'], d['date'], d['term'], d['stockIndex'])
        domain.put_attributes(id, d)
        print d

def filter_keywords(message, keywords):
    b = message.get('BasisEnrichment')
    if not b:
        return None

    result = {}
    for l in (i['lemma'] for i in b['tokens']):
        if l in keywords:
            result[l] = result.get(l, 0) + 1

    return result


def get_conf(file_name):
    with codecs.open(file_name, encoding='utf8', mode='r') as c:
        result = json.load(c)
    #log.debug('read config from %s', file_name)
    return result


def main():

    conf = get_conf(os.path.join(os.path.dirname(__file__), 'bloomberg_news_keywords.conf'),)
#with open(arg.sub, 'r') as inq:
#    m = inq.read()
    keywords = set(conf.get('keywords', []))
    domain = get_domain()
    with codecs.getreader('utf-8')(sys.stdin) as ins:
        for l in ins:
            m = json.loads(l, encoding='utf8')
            kw = filter_keywords(m, keywords)
            store_keywords(kw, m, domain)


if __name__ == "__main__":
    main()
