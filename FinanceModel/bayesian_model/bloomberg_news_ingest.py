#!/usr/bin/env python 
# -*- coding: utf-8 -*-

from __future__ import with_statement
import urllib2
from bs4 import BeautifulSoup
from datetime import datetime
import json
import argparse
import shelve
import os.path
import codecs
from etool import queue, logs, message, args

"""
    The Steps for scraping news from Bloomberg
    1. read the config file to initiate the company List dir, AlreadyDownloadedNews file, output dir of Collected result and port for ZMQ
    2. iterate the list of company and get the news list for each company
    3. load AlreadyDownloadedNews file as json object, For each news to be scraped, check if it is already downloaded by check if its title in the AlreadyDownloadedNews file
    4. push the news to ZMQ
"""

__processor__ = 'bloomberg_news_ingest'
log = logs.getLogger(__processor__)

def get_stock_news(index, company, seen_it):
    result = []
    # read the news for the company
    company = company.replace("\r","").replace("\n","").strip()
    companyUrl = "http://www.bloomberg.com/quote/"+company+"/news#news_tab_company_news"
    soup = BeautifulSoup(urllib2.urlopen(companyUrl,timeout=60))
    # get article urls
    urlElements = soup.findAll(id="news_tab_company_news_panel")
    for urlElement in urlElements:
        elements = urlElement.findAll(attrs={'data-type':"Story"})
        for ele in elements:
            newsUrl = "http://www.bloomberg.com" + ele["href"]
            title = ele.string.encode('ascii', 'ignore')
            if not seen_it.has_key(str(title)):
                seen_it[str(title)] = datetime.utcnow()
                article = get_news_by_url(newsUrl)
                article["stockIndex"] = index
                article["company"] = company
                result.append(article)
                # TODO - this should be a generator

    return result

            
def get_news_by_url(url):
    article = {}
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        # title
        title = ""
        titleElements = soup.findAll(id="disqus_title")
        for ele in titleElements:
            title = ele.getText().encode('utf-8')
        article["title"] = title 
        
        # get article timestamps
        postTime = ""
        postTimeElements = soup.findAll(attrs={'class':"datestamp"})
        for ele in postTimeElements:
            timeStamp = float(ele["epoch"])
        #postTime = datetime.strftime("%Y-%m-%d %H:%M:%S",datetime.fromtimestamp(timeStamp/1000))
        postTime = datetime.fromtimestamp(timeStamp/1000)
        postTimeStr = postTime.isoformat()
        article["postTime"] = postTimeStr
        
        # get date (should be part of the time?)
        postDay = postTime.date()
        article["postDate"] = datetime.strftime(postDay,"%Y-%m-%d");
        
        # author
        author = ""
        authorElements = soup.findAll(attrs={'class':"byline"})
        for ele in authorElements:
            author = ele.contents[0].strip().replace("By","").replace("-","").replace("and", ",").strip();
        article["author"] = author
        
        # content - FIXME - Extractor undefined
        content = soup.body.get_text()
        article["content"] =  content
        
        # source info
        source = "Bloomberg News"
        article["source"] = source
        
        # time stamp
        updateTime = datetime.utcnow().isoformat()
        article["updateTime"] = updateTime
        # the message format specified field
        article["date"] = updateTime
        
        # date? why are dates and times separate?
        updateDate = datetime.strftime(datetime.utcnow(),"%Y-%m-%d")
        article["updateDate"] = updateDate

        article["url"] =  url        
        article = message.add_embers_ids(article)
        
    except KeyboardInterrupt:
        raise

    except:
        log.exception("Could not ingest %s" % (url,))
        return {}

    log.debug("Successfully ingested %s" % (url,))
    return article


def get_conf(file_name):
    with codecs.open(file_name, encoding='utf8', mode='r') as c:
        result = json.load(c)
    log.debug('read config from %s', file_name)
    return result

             
def main():
    ap = args.get_parser()
    ap.add_argument('-c', '--conf', metavar='CONF', type=str, nargs='?', 
                    default=os.path.join(os.path.dirname(__file__), 'bloomberg_news_ingest.conf'),
                    help='The location of the configuration file.')
    arg = ap.parse_args()
    assert arg.pub, "--pub required. Need a queue to publish on"

    logs.init(arg)
    conf = get_conf(arg.conf)
    seen_it = shelve.open("bloomberg_news_seen_it.db")
    
    try:
        with queue.open(arg.pub, 'w', capture=True) as outq:
            for (index, companies) in conf.items():
                for company in companies:
                    articles = get_stock_news(index, company, seen_it)
                    for a in articles:
                        outq.write(a)

    except KeyboardInterrupt:
        log.info('GOT SIGINT, exiting')


if __name__ == "__main__":
    log.info("Starting")
    main()
    log.info("Done")
