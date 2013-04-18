#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"

"""
https://www.grupoaval.com/portales/jsp/historicotabla.jsp?indi=4795&fecini=04/17/2013&fecfin=04/18/2013
http://www.bolchile.cl/portlets/CentroDatosPortlet/RecuperarDatosServlet?idioma=en&tipoInstr=INDICES_MERCADO&instrumento=CHILE65&vista=PRECIOS&fechaInicio=15/03/2013&fechaTermino=18/04/2013&temporalidad=DIA&fechaComposicion=18/04/2013
"""

from etool import message, logs, args, queue
from bs4 import BeautifulSoup
import urllib2
import json
import re
from datetime import datetime, timedelta
import sys
import boto


__processor__ = 'captureOtherPrice'
log = logs.getLogger(__processor__)


STOCK_CON = {"CHILE65": {"urlStr":"http://www.bolchile.cl/portlets/CentroDatosPortlet/RecuperarDatosServlet?idioma=en&tipoInstr=INDICES_MERCADO&instrumento=CHILE65&vista=PRECIOS&fechaInicio=%s&fechaTermino=%s&temporalidad=DIA" , "feed": "www.bolchile.cl", "type": 'stock', "tFormat": "%d/%m/%Y"},
        "COLCAP": {"urlStr": "https://www.grupoaval.com/portales/jsp/historicotabla.jsp?indi=4795&fecini=%s&fecfin=%s", "feed": "www.grupoaval.com", "type": 'stock', "tFormat": "%m/%d/%Y"}}


def get_domain(arg):
    """Get the storage domain (table) for the Bloomberg data."""
    conn = boto.connect_sdb()
    conn.create_domain(arg.domain) # you can create repeatedly
    return conn.get_domain(arg.domain)


def store(arg, message, domain=None):
    """Save a message to SimpleDB"""
    assert message, 'Message is empty, cannot store it.'

    if not domain:
        domain = get_domain(arg)
        domain.put_attributes(message['embersId'], message)


def scrape_chile65_url(url):
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        #To check if data exists
        p = re.compile(u"there is no information", flags=re.I)
        if p.search(soup.text):
            return ['n/a'] * 3

        #get the price data table
        content = soup.find_all('table')
        if len(content) == 0:
            return ['n/a'] * 3

        price_table = content[-1]
        price_data = [d.text.replace("\n","").replace(",", "").replace(" ", "").strip() for d in price_table.find('tr').find_all('td')]

        #get the last price
        last_price = float(price_data[2])

        #get the previous price
        previous_last_price = last_price - float(price_data[3])

        #get the post date
        t_format = "%d-%b-%Y"
        post_time = datetime.strptime(price_data[0], t_format).isoformat()
        
        return post_time, str(last_price), str(previous_last_price)
    except:
        log.exception('Error when extract %s : %s' % (url, sys.exc_info()[0]))
        return ['n/a'] * 3


def scrape_colcap_url(url):
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        #get price lastest day's data
        latest_day = soup.find('tr')
        if latest_day is None:
            return ['n/a'] * 3
        
        price_data = [d.text.replace("\n", "").replace(" ","").replace(",", "").strip() for d in latest_day.find_all('td')]
        # get last price
        last_price = price_data[1]
        
        #get the post_time
        post_time = datetime.strptime(price_data[0], "%d/%m/%y").isoformat()
        
        #get the previous last price
        previous_day = latest_day.find_next_sibling()
        if previous_day is None:
            previous_last_price = 'n/a'
            return post_time, last_price, previous_last_price
        p_price_data = [d.text.replace("\n", "").replace(" ","").replace(",", "").strip() for d in previous_day.find_all('td')]
        previous_last_price = p_price_data[1]

        return post_time, last_price, previous_last_price
    except:
        log.exception("Error: %s %s" % (url, sys.exc_info()[0]))
        return ['n/a'] * 3


def ingest_price(arg, stock, scrape_f):
    #initiate url
    s_date = datetime.strftime(datetime.strptime(arg.d, "%Y-%m-%d") + timedelta(days=-30), STOCK_CON[stock]['tFormat'])
    e_date = datetime.strftime(datetime.strptime(arg.d, "%Y-%m-%d"), STOCK_CON[stock]["tFormat"])
    url = STOCK_CON[stock]["urlStr"] % (s_date, e_date)

    post_time, last_price, previous_last_price = scrape_f(url)
    if post_time == 'n/a':
        return None
    
    nowstr = datetime.utcnow().isoformat()

    #create json message
    msg={"previousCloseValue": previous_last_price,
            "date": post_time,
            "queryTime": nowstr,
            "originalUpdateTime": post_time,
            "name": stock,
            "feed": STOCK_CON[stock]['feed'],
            "currentValue": last_price,
            "type": STOCK_CON[stock]['type']}

    msg = message.add_embers_ids(msg)
    return msg

def main():
    ap = args.get_parser()
    default_day = datetime.strftime(datetime.now(), "%Y-%m-%d") 
    ap.add_argument('--d', type=str, default=default_day, help="The day to ingest, Format: dd/mm/yyyy")
    ap.add_argument('--domain', default='bloomberg_prices', help="The simpleDB table to store raw data")
    arg = ap.parse_args()
    
    assert arg.pub, 'Need a queue to publish'
    queue.init(arg)
    logs.init(arg)

    with queue.open(arg.pub, 'w') as out_q:
        for stock in STOCK_CON:
            if stock == "COLCAP":
                scrape_f = scrape_colcap_url
            if stock == "CHILE65":
                scrape_f = scrape_chile65_url
            msg = ingest_price(arg, stock, scrape_f)
            if msg is not None:
                out_q.write(msg)
                store(arg, msg)

if __name__ == "__main__":
    main()
