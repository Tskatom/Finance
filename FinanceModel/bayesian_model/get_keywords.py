#!/usr/bin/env python

import sys
import boto

DOMAIN = 't_enriched_bloomberg_prices'

def get_domain(keyId,secret):
    """Get the storage domain (table) for the Bloomberg data."""
    conn = boto.connect_sdb(keyId,secret)
    conn.create_domain(DOMAIN) # you can create repeatedly
    return conn.get_domain(DOMAIN)

# data format
# {'count': 5, u'postTime': u'2012-11-08T16:42:41', 
# 'term': u'drop', 
# u'date': u'2012-11-08T19:09:55.583531', 
# u'stockIndex': u'IBOV', 
# u'embersId': u'20cc4524914984d9777f3308578e4683fe1df2f0'}

def query_keywords(domain, start_date,end_date, index):

    eids = set()
    terms = {}
    rs = domain.select("SELECT * FROM bloomberg_keywords WHERE date >= '%s' AND date <= '%s' AND stockIndex = '%s'" % (start_date,end_date, index))
    # you can do order by, but the column must appear in the where clause
    for r in rs:
        t = r['term']
        terms[t] = terms.get(t, 0) + int(r['count'])
        eids.add(r['embersId'])

    return (terms, eids)


def q2(domain):
    rs = domain.select("select * from warnings where eventType='0411' or eventType='0412' or eventType='0421' or eventType='0422'")
    for r in rs:
        print r
        
def main():
    keyId = "AKIAJZ2N4UOI4TP4YBRQ"
    secret = "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"
#    (t, i) = query_keywords(domain, '2012-11-08','2012-11-09', 'IBOV')
#    
#    print t
#    print t.get('claim')
#    print i
    conn = boto.connect_sdb(keyId,secret)
    domains = conn.get_all_domains()
    for d in domains:
        print d

    b_prices_domain = conn.get_domain("t_surrogatedata")
    
    rs = b_prices_domain.select("select * from t_enriched_bloomberg_prices where name='IBVC' and postDate='2013-02-07'")
    for r in rs:
        print r
    
if __name__ == "__main__":
    main()
