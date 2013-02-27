#!/usr/bin/env python
#-*- coding:utf-8 -*-

import nltk
import json
import sys
import hashlib
from datetime import datetime
from etool import queue,logs
import argparse
import sqlite3 as lite

"""
The parameters for bloomberg_news_process
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='Sqlite database file')
    ap.add_argument('-f',dest="bloomberg_news_file",metavar="BLOOMBERG_NEWS",type=str,help='The daily BLOOMBERG NEWS file')
"""

__processor__ = 'bloomberg_news_process'
log = logs.getLogger(__processor__)
logs.init()

def insert_news(conn,article):
    try:
        cur = conn.cursor()
        sql = "insert into t_daily_news(embers_id,title,author,post_time,post_date,content,stock_index,source,update_time,url) values (?,?,?,?,?,?,?,?,?,?)"
        embersId = article["embersId"]
        title = article["title"]
        author = article["author"]
        postTime = article["postTime"]
        postDate = article["postDate"]
        content = article["content"]
        stockIndex = article["stockIndex"]
        source = article["source"]
        updateTime = article["updateTime"]
        url = article["url"]
        cur.execute(sql,(embersId,title,author,postTime,postDate,content,stockIndex,source,updateTime,url))
        conn.commit()
        return True
    except:
        log.info("Error: %s",(sys.exc_info()[0],))
        return False

def insert_enriched_news(conn,enriched_news):
    try:
        cur = conn.cursor()
        insertSql = "insert into t_daily_enrichednews (embers_id,derived_from,title,author,post_time,post_date,content,stock_index,source,update_time) values (?,?,?,?,?,?,?,?,?,?)"
        derivedFrom = enriched_news["derivedFrom"] 
        title = enriched_news["title"]
        author = enriched_news["author"]
        postTime = enriched_news["postTime"]
        postDate = enriched_news["postDate"]
        content = enriched_news["content"]
        stockIndex = enriched_news["stockIndex"]
        source = enriched_news["source"]
        updateTime = enriched_news["updateTime"]
        embersId = enriched_news["embersId"]
        cur.execute(insertSql,(embersId,derivedFrom,title,author,postTime,postDate,content,stockIndex,source,updateTime,))
        conn.commit()
    except lite.Error, e:
        log.info("Error: %s",(e.args[0],))
        pass

def process(port,conn,blg_news_file):
    "Get all the news"
    newsList = []
    with open(blg_news_file,"r") as news_file:
        lines = news_file.readlines()
        for line in lines:
            line = line.replace("\r","").replace("\n","")
            news = json.loads(line)
            newsList.append(news)
            
    enrichedNewsList = []
    for news in newsList:
        if_succ = insert_news(conn, news)
        if if_succ:
            enrichedNews = process_news(news)
            if enrichedNews:
                enrichedNewsList.append(enrichedNews)
    
    #Write the enricheNews to SqliteDB and push them into ZMQ
    with queue.open(port, 'w', capture=True) as outq:
        for enrichedNews in enrichedNewsList:
            outq.write(enrichedNews)
            insert_enriched_news(conn,enrichedNews)

def process_news(news):
    try:
        content = news["content"]
        
        tokens = nltk.word_tokenize(content)
        stemmer = nltk.stem.snowball.SnowballStemmer('english')
        words = [w.lower().strip() for w in tokens if w not in [",",".",")","]","(","[","*",";","...",":","&",'"',"'"] and not w.isdigit()]
        words = [w for w in words if w.encode("utf8") not in nltk.corpus.stopwords.words('english')]
        stemmedWords = [stemmer.stem(w) for w in words]
        fdist=nltk.FreqDist(stemmedWords)
        
        updateTime = datetime.now().isoformat()
        
        enrichedData = {}
        enrichedData["derivedFrom"] = news["embersId"]
        enrichedData["title"] = news["title"]
        enrichedData["author"] = news["author"]
        enrichedData["postTime"] = news["postTime"]
        enrichedData["postDate"] =  news["postDate"]
        enrichedData["content"] =  json.dumps(fdist)
        enrichedData["stockIndex"] = news["stockIndex"]
        enrichedData["source"] = news["source"]
        enrichedData["updateTime"] = updateTime
        enrichedData["updateDate"] = datetime.strftime(datetime.now(),"%Y-%m-%d")
        embersId = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
        enrichedData["embersId"] = embersId
    except :
        log.info("Error: %s",(sys.exc_info()[0],))
        enrichedData = {}
    finally:
        return enrichedData

def parse_args():
    ap = argparse.ArgumentParser("Preprocess the Bloomberg News")
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
    ap.add_argument('-f',dest="bloomberg_news_file",metavar="BLOOMBERG_NEWS",type=str,help='The daily BLOOMBERG NEWS file')
    return ap.parse_args()    

def main():
    args = parse_args()
    port = args.port
    conn = lite.connect(args.db_file)
    bloomberg_news_file = args.bloomberg_news_file
    process(port,conn,bloomberg_news_file)
    conn.close()
if __name__ == "__main__":
    main()

    
