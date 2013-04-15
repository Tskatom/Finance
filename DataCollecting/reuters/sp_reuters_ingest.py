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


__processor__ = 'lat_reuters_ingest'
log = logs.getLogger(__processor__)


class News():
    def __init__(self):
        self.news = {}
        self.title_lab = "title"
        self.post_time_lab = "postTime"
        self.author_lab = "author"
        self.url_lab = "url"
        self.date_lab = "date"
        self.source_lab = "source"
        self.content_lab = "content"
        self.encoding_lab = "encoding"
        self.id_lab = "embersId"
        self.country_lab = "countries"
        self.news[self.country_lab] = []
        self.labels_lab = "labels"
        self.news[self.labels_lab] = []

    def set_title(self, title):
        self.news[self.title_lab] = title

    def set_posttime(self, post_time):
        self.news[self.post_time_lab] = post_time

    def set_author(self, author):
        self.news[self.author_lab] = author

    def set_url(self, url):
        self.news[self.url_lab] = url

    def set_date(self, date):
        self.news[self.date_lab] = date

    def set_source(self, source):
        self.news[self.source_lab] = source

    def set_content(self, content):
        self.news[self.content_lab] = content

    def set_encoding(self, encoding):
        self.news[self.encoding_lab] = encoding

    def set_id(self, embersId):
        self.news[self.id_lab] = embersId

    def set_country(self, country):
        self.news[self.country_lab].append(country)

    def set_labels(self, label):
        self.news[self.labels_lab].append(label)


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


def get_daily_news(post_date, seen_it):
    #post_date format: mmddyyyy
    daily_url = 'http://lta.reuters.com/news/archive/topNews?date=%s' % (post_date)
    try:
        soup = BeautifulSoup(urllib2.urlopen(daily_url))
        urls = soup.find("div", "module").find_all('a')
        for url in urls:
            try:
                title = url.text.encode('ascii', 'ignore')
                real_url = "http://lta.reuters.com%s" % (url['href'])
                if not seen_it.has_key(title):
                    news = get_news_by_url(real_url)
                    if news is not None:
                        seen_it[title] = datetime.utcnow().isoformat()
                        yield news
            except:
                log.exception("Error individual url: %s" % (sys.exc_info()[0]))
                continue
    except:
        log.exception("Error When Loading the url list %s %s" % (daily_url, sys.exc_info()[0]))


def main():
    ap = args.get_parser()
    ap.add_argument('--s_date', type=str, help="the start date to ingest: format mmddyyyy")
    ap.add_argument('--e_date', type=str, help="the end of date to ingest: format mmddyyyy")
    ap.add_argument('--o', type=str, help="the output directory")
    arg = ap.parse_args()
    logs.init(arg)
    
    t_format = "%m%d%Y"
    s_date = datetime.strptime(arg.s_date, t_format)
    e_date = datetime.strptime(arg.e_date, t_format)
    d_delta = (e_date - s_date).days

    seen_it = shelve.open("reuters_news_seen_it.db")
    i = 0
    while i <= d_delta:
        day_str = datetime.strftime(s_date + timedelta(days=i), t_format)
        print "Extracting %s" % (day_str)
        "write news to day file"
        with open("%s%s%s_ita_reuters.txt" % (arg.o, os.sep, day_str), 'w') as w:
            for news in get_daily_news(day_str, seen_it):
                w.write(json.dumps(news) + "\n")
        i += 1


if __name__ == "__main__":
    main()
