#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import with_statement
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import argparse
import shelve
import requests
from boilerpipe.extract import Extractor


"""
Scraping all the world news from Bloomberg, without any keyword filtering

1. specify a date of which day news will be collected, crawl that day's content as json object.
2. load AlreadyDownloadedNews file as json object, for each news to be scraped, check if it is already downloaded, by checking whether its title is in the AlreadyDownloadedNews file.
3. push the news to ZMQ
"""

def	get_world_news(collectDay, seen_it):
    "read news of collect_day"
    collectDay = collectDay.replace("\r", "").replace("\n", "").strip()
    archivedUrl = "http://www.bloomberg.com/archive/news/" + collectDay
    r = requests.get(archivedUrl, timeout=60)
    soup = BeautifulSoup(r.text)
    "go to todayArchivedUrl to get the article urls"

    count = 0
    urlElements = soup.findAll("ul", {"class": "stories"})
    for urlElement in urlElements:
        elements = urlElement.findAll('a', href=True)
        for ele in elements:
            newsUrl = "http://www.bloomberg.com/" + ele["href"]
            title = ele.string.encode('utf-8', 'ignore')
            if not seen_it.has_key(str(title)):
                article = get_news_by_url(newsUrl)
                seen_it[str(title)] = datetime.utcnow()
                article = get_news_by_url(newsUrl)
                "Just to keep in the same format with Wei's news"
                article["stockIndex"] = ""
                article["company"] = ""
                count += 1


def	get_news_by_url(url):
    article = {}
    try:
        r = requests.get(url, timeout=60)
        soup = BeautifulSoup(r.text)
        # title
        title = ""
        titleElements = soup.findAll(id="disqus_title")
        for ele in titleElements:
            title = ele.getText().encode('utf-8')
        article["title"] = title

        # get article timestamps
        postTime = ""
        postTimeElements = soup.findAll(attrs={'class': "datestamp"})
        for ele in postTimeElements:
            timeStamp = float(ele["epoch"])
        postTime = datetime.fromtimestamp(timeStamp / 1000)
        postTimeStr = postTime.isoformat()

        article["postTime"] = postTimeStr

        # get date (should be part of the time?)
        postDay = postTime.date()
        article["postDate"] = datetime.strftime(postDay, "%Y-%m-%d");

        # author
        author = ""
        authorElements = soup.findAll(attrs={'class': "byline"})
        for ele in authorElements:
            author = ele.contents[0].strip().replace("By", "").replace("-", "").replace("and", ",").strip();
        article["author"] = author

        # content
        extractor=Extractor(extractor='ArticleExtractor',url=url)
        content = unicode(extractor.getText())
        article["content"] =  content
        
        print article['content']



        # source info
        source = "Bloomberg News"
        article["source"] = source

        # time stamp
        #updateTime = datetime.utcnow().isoformat()
        updateTime = datetime.now().isoformat()
        article["updateTime"] = updateTime
        # the message format specified field
        article["date"] = updateTime

        # date? why are dates and times separate?
        updateDate = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")
        article["updateDate"] = updateDate

        article["url"] = url

    except Exception:
        print "Could not ingest url "
        return {}

    print "Successfully ingested %s" % (url)
    return article


def	main():
    ap = argparse.ArgumentParser()
    t_format = "%Y-%m-%d"
    default_day = datetime.strftime(datetime.now(), t_format)
    ap.add_argument('-day', dest="collect_day", metavar="NEWS COLLECT DAY", type=str, default=default_day, nargs="?", help="The day news to be collected: %Y-%m-%d, like 2012-12-13")
    arg = ap.parse_args()
    collect_day = arg.collect_day
    collect_yesterday = datetime.strftime(datetime.strptime(collect_day, t_format) + timedelta(days=-1), t_format)
    days = [collect_yesterday, collect_day]
    seen_it = shelve.open("bloomberg_world_news.db")
    for t_day in days:
        get_world_news(t_day, seen_it)

if __name__ == "__main__":
    main()
