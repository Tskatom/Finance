#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import urllib2
import urllib
import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime
import urlparse
import hashlib


SYMBOLS = {"HSI": "^HSI", "UKX": "^FTSE", "AEX": "AEX.AS", "CCMP": "^IXIC", "SMI": "^SSMI", "CAC": "^FCHI", "SPTSX": "^GSPTSE", "FTSEMIB": "FTSEMIB.MI", "AS51": "^AXJO", "DAX": "^GDAXI", "SX5E": "^STOXX50E", "INDU": "^DJI", "IBEX": "^IBEX", "NKY": "^N225", "OMX": "^OMX"}


def download_stock(url, stock):
    data = urllib2.urlopen(url).readlines()[1:]
    print data
    prices = []
    for line in data:
        line = line.strip().split(",")
        post_date = line[0]
        close_price = float(line[6])
        price = {"name": stock, "date": post_date, "currentValue": close_price,
                 "feed": "Yahoo Finance", "queryTime": datetime.now().isoformat(),
                 "type": "stock"}

        embersId = hashlib.sha1(str(price)).hexdigest()
        price["embersId"] = embersId
        prices.append(price)

    #add previousPrice for each record
    for i, price in enumerate(prices[:-1]):
        price["previousClosePrice"] = prices[i + 1]["currentValue"]

    return prices[: -1]


def download_dowjones(url, stock):
    prices = []
    soup = BeautifulSoup(urllib2.urlopen(url))
    #get current page's price data
    contents = soup.find("table", "yfnc_datamodoutline1").table
    records = contents.find_all("tr")
    if len(records) > 0:
        records = records[1:-1]

    for tr in records:
        #get the price data
        tds = tr.find_all("td")
        if len(tds) > 0:
            post_date = datetime.strptime(tds[0].text, "%b %d, %Y").strftime("%Y-%m-%d")
            close_price = float(tds[6].text.replace(",", ""))

            price = {"name": stock, "date": post_date, "currentValue": close_price,
                     "feed": "Yahoo Finance", "queryTime": datetime.now().isoformat(),
                     "type": "stock"}
            embersId = hashlib.sha1(str(price)).hexdigest()
            price["embersId"] = embersId
            prices.append(price)

    #check if exists next page, if exist then invoke the method again
    next = soup.find("a", attrs={"rel": "next"})
    if next:
        child_url = urlparse.urljoin(url, next['href'])
        sub_result = download_dowjones(child_url, stock)
        prices.extend(sub_result)

    return prices


def process(out_dir):
    paras = {"a": "04", "b": "24",
             "c": "2013", "d": "04", "e": "27",
             "f": "2013", "g": "d", "ignore": ".csv"}
    url = "http://ichart.yahoo.com/table.csv"

    for stock, symbol in SYMBOLS.items():
        paras["s"] = symbol
        final_url = "%s?%s" % (url, urllib.urlencode(paras))
        try:
            if stock == "INDU":
                dow_url = 'http://finance.yahoo.com/q/hp'
                final_url = "%s?%s" % (dow_url, urllib.urlencode(paras))
                data = download_dowjones(final_url, stock)
                for i, price in enumerate(data[:-1]):
                    price["previousClosePrice"] = data[i + 1]["currentValue"]
                data = data[:-1]
            else:
                data = download_stock(final_url, stock)

            #write data into file
            file_name = out_dir + os.sep + stock + ".txt"
            with open(file_name, "w") as w:
                for d in data:
                    w.write(str(d) + "\n")
        except:
            print stock, "\n", final_url, "\n", sys.exc_info()[0], "\n"


if __name__ == "__main__":
    pass
