#!/usr/bin/env python

import sys
import os.path
import codecs
import json
import boto
from etool import logs, args, queue

log = logs.getLogger('bloomberg_news_keywords')

def get_domain(args):
    """Get the storage domain (table) for the Bloomberg data."""
    conn = boto.connect_sdb()
    conn.create_domain(args.domain) # you can create repeatedly
    return conn.get_domain(args.domain)

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
    ap = args.get_parser()
    ap.add_argument('--cat', action="store_true",
                    help='Read input from standard in and write to standard out.')
    ap.add_argument('--domain', default='bloomberg_keywords', type=str, nargs='?',
                    help='The SDB domain to use.')
    arg = ap.parse_args()
    assert arg.sub, 'Need a queue to subscribe to.'
    assert arg.domain, 'Need a domain to connect to.'

    logs.init(arg)
    queue.init(arg)
    conf = get_conf(os.path.join(os.path.dirname(__file__), 'bloomberg_news_keywords.conf'),)
    keywords = set(conf.get('keywords', []))
    domain = get_domain(arg)

    with queue.open(arg.sub, 'r') as inq:
        for m in inq:
            try:
                kw = filter_keywords(m, keywords)
                store_keywords(kw, m, domain)
            except KeyboardInterrupt:
                log.info('Got SIGINT, exiting.')
                break

            except:
                log.exception('Unexpected exception processing keywords.')


if __name__ == "__main__":
    main()
