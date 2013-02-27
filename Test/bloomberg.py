#!/usr/bin/env python

import sys
import json
from datetime import datetime
import boto

#DOMAIN = 'bloomberg_prices'
DOMAIN = 'bloomberg_prices'

def get_domain():
    """Get the storage domain (table) for the Bloomberg data."""
    keyId = "AKIAJZ2N4UOI4TP4YBRQ"
    secretkey = "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"
    conn = boto.connect_sdb(keyId,secretkey)
    conn.create_domain(DOMAIN) # you can create repeatedly
    return conn.get_domain(DOMAIN)

def store(message, domain=None):
    """Save a message to SimpleDB"""
    assert message, 'Message is empty, cannot store it.'

    if not domain:
        domain = get_domain()

    domain.put_attributes(message['embersId'], message)

def fix_dates(message):
    """Defubarification of the date formats in the current messages."""
    def to_iso(val):
        f = '%m/%d/%Y %H:%M:%S'
        try:
            d = datetime.strptime(val, f)
            return d.isoformat()
        except:
            pass

        return None

    d = to_iso(message.get("updateTime", None))
    if d:
        message["updateTime"] = d

    d = to_iso(message.get("queryTime", None))
    if d:
        message["queryTime"] = d
        
    return message

def make_statement(begin=None, end=None, name=None, typ=None):
    """Generate a query against the Bloomberg data set. Options are:
    - begin: the start date/time as an ISO string 
             (must be lexically less than the date you want)
    - end: the end date/time as an ISO string (same caveat)
    - name: the ticker symbol for the security (Bloomberg symbology).
    - typ: the security type, one of 'stock', 'currency'
    """
    wc = []
    if begin:
        wc.append("updateTime >= '%s'" % (begin,))

    if end:
        wc.append("updateTime <= '%s'" % (end,))

    if name:
        wc.append("name = '%s'" % (name,))

    if typ:
        wc.append("type = '%s'" % (typ,))

    if wc:
        return "SELECT * FROM %s WHERE %s" % (DOMAIN, ' AND '.join(wc))
    else:
        return "SELECT * FROM %s" % (DOMAIN,)

def query(domain=None, **kw):
    """Run a query with the normal parameters (see make_statement) and return the result object."""
    if not domain:
        domain = get_domain()
        
    s = make_statement(**kw)
    return domain.select(s)

def main():
    """Push the messages to simple db"""
 
    d = get_domain()
    print "hi"
    results = d.select("select * from bloomberg_prices where name='USDARS' and date < '2012-10-02T03:00:21' order by date desc",max_items=2)
#    results = d.select("select * from bloomberg_prices limit 2")
    for result in results:
        print result 
#    for l in sys.stdin:
#        m = json.loads(l, encoding="utf8")
#        m = fix_dates(m)
#        del m['feed']
#        store(m, domain=d)
#        print m

if __name__ == "__main__":
    main()
