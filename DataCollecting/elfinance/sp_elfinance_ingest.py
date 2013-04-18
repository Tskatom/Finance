#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = "Wei Wang"
__email__ = "tskatom@vt.edu"


import sys
import os
from bs4 import BeautifulSoup
import urllib2
from datetime import datetime, timedelta
from etool import logs, args, message
import shelve
import json
from news import News
import locale


__processor__ = "sp_elfinance_ingest"
log = logs.getLogger(__processor__)


def get_news_by_url(url):
    news = News()
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))

        #title
        title = soup.find("div", "pg-story-head md").find("h2").text
        news.set_title(title)

        #postTime
        author_posttime = soup.find("p", "dateline").text.replace("\n","").lower().replace("\t","").split("/")
        post_time = author_posttime[1].replace("pm", "").replace("am", "").strip()
        
        t_format = "%d %b %Y, %I:%M"
        post_time = datetime.strptime(post_time, t_format).isoformat()
        news.set_posttime(post_time)

        #author
        author = author_posttime[0]
        news.set_author(author)

        #url
        news.set_url(url)

        #date
        date = datetime.utcnow().isoformat()
        news.set_date(date)

        #source
        source = 'elfinancierocr'
        news.set_source(source)

        #content, encoding, id, country, labels
        paragraphs = soup.find("div", "pg-story-body mce").find_all('p')
        content = " ".join([unicode(p.text) for p in paragraphs])
        news.set_content(content)

        #encoding
        encoding = 'utf-8'
        news.set_encoding(encoding)

        news.news = message.add_embers_ids(news.news)

        return news.news
    except:
        log.exception("Exceptopn when extracting %s %s" % (url, sys.exc_info()[0]))
        return None


def get_daily_news(start_url, seen_it):
    daily_news = []
    try:
        soup = BeautifulSoup(urllib2.urlopen(start_url))
        urls = soup.find_all("h2" , "headline headline-small")
        for url in urls:
            try:
                info = url.find("a")
                real_url = "http://www.elfinancierocr.com%s" % (info['href'])
                title = info.text.encode("ascii", 'ignore')
                if title not in seen_it.keys():
                    print real_url
                    news = get_news_by_url(real_url)
                    if news is not None:
                        daily_news.append(news)
                        seen_it[title] = datetime.now().isoformat()
            except:
                log.exception("Error for daily news: %s %s" % (url, sys.exc_info()[0]))
                continue
    except:
        log.exception("Error occur In get_daily_news %s %s" % (start_url, sys.exc_info()[0]))
    
    return daily_news
    
    
def get_category_news(category, seen_it, out_dir):
    start_url = "http://www.elfinancierocr.com/%s/" % (category)
    
    initiate_url = "http://www.elfinancierocr.com/%s/" % (category)
    soup = BeautifulSoup(urllib2.urlopen(initiate_url))
    print soup.find_all("a", "page")[-1]
    pages = int(soup.find_all("a", "page")[-1].text)
    #get the maximum page
    for x in range(pages):
        if x == 0:
            s_url = initiate_url
        else:
            s_url = "%s?page=%s" % (initiate_url, str(x + 1))
        
        daily_news = get_daily_news(s_url, seen_it)
        with open("%selfinancierocr_%s_%s.txt" % (out_dir, category, str(x + 1)), 'w') as w:
            for news in daily_news:
                w.write(json.dumps(news) + "\n")
    
    
def main():
    ap = args.get_parser()
    ap.add_argument('--o', type=str, help="the output dir to store news")
    arg = ap.parse_args()
    
    assert arg.o, 'Need a dir to store news'
    logs.init(arg)
    locale.setlocale(locale.LC_TIME, 'es_ES.utf-8')
    
    seen_it = shelve.open('elfinance_seen_it.db')
    
    cas = ['finanzas']
    for ca in cas:
        get_category_news(ca, seen_it, arg.o)
                
if __name__ == "__main__":
    main()    
          