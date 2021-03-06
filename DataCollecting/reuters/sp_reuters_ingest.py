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


__processor__ = "sp_reuters_ingest"
log = logs.getLogger(__processor__)


def normalize_text(text):
    if isinstance(text, unicode):
        return text.encode('ascii', 'ignore')
    else:
        return text


def get_news_by_url(url):
    news = News()
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))

        #title
        title = soup.find_all('h1')[0].text
        news.set_title(title)

        #postTime
        post_time = soup.select('meta[name="REVISION_DATE"]')[0]['content']
        t_format = "%a %b %d %H:%M:%S %Z %Y"
        post_time = datetime.strptime(post_time, t_format).isoformat()
        news.set_posttime(post_time)

        #author
        author = soup.select('meta[name="Author"]')[0]['content']
        news.set_author(author)

        #url
        news.set_url(url)

        #date
        date = datetime.utcnow().isoformat()
        news.set_date(date)

        #source
        source = 'lta_reuters'
        news.set_source(source)

        #content, encoding, id, country, labels
        paragraphs = soup.find(id='resizeableText').find_all('p')
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


def get_daily_news(post_date, seen_it, region):
    #post_date format: mmddyyyy
    daily_url = 'http://%s.reuters.com/news/archive/topNews?date=%s' % (region, post_date)
    daily_news = []
    try:
        soup = BeautifulSoup(urllib2.urlopen(daily_url))
        urls = soup.find("div", "module").find_all('a')
        for url in urls:
            try:
                title = url.text.encode('ascii', 'ignore')
                real_url = "http://%s.reuters.com%s" % (region, url['href'])
                if not seen_it.has_key(title):
                    news = get_news_by_url(real_url)
                    if news is not None:
                        seen_it[title] = datetime.utcnow().isoformat()
                        daily_news.append(news)
            except:
                log.exception("Error individual url: %s" % (sys.exc_info()[0]))
                continue
    except:
        log.exception("Error When Loading the url list %s %s" % (daily_url, sys.exc_info()[0]))

    return daily_news


def main():
    global log
    ap = args.get_parser()
    ap.add_argument('--s_date', type=str, help="the start date to ingest: format mmddyyyy")
    ap.add_argument('--e_date', type=str, help="the end of date to ingest: format mmddyyyy")
    ap.add_argument('--o', type=str, help="the output directory")
    ap.add_argument('--region', type=str, help="the region of the web site")
    arg = ap.parse_args()
    logs.init(arg)

    t_format = "%m%d%Y"
    s_date = datetime.strptime(arg.s_date, t_format)
    e_date = datetime.strptime(arg.e_date, t_format)
    d_delta = (e_date - s_date).days

    seen_it = shelve.open("%s_reuters_news_seen_it.db" % (arg.region))
    i = 0
    while i <= d_delta:
        day_str = datetime.strftime(s_date + timedelta(days=i), t_format)
        print "Extracting %s" % (day_str)
        "write news to day file"
        with open("%s%s%s_ita_reuters_%s.txt" % (arg.o, os.sep, day_str, arg.region), 'w') as w:
            daily_news = get_daily_news(day_str, seen_it, arg.region)
            for news in daily_news:
                w.write(json.dumps(news) + "\n")
        i += 1


if __name__ == "__main__":
    main()
