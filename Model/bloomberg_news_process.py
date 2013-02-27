#-*- coding:utf8 -*-
import nltk
import json
import sys
import hashlib
from datetime import datetime
from etool import queue,logs
import boto
import argparse


__processor__ = 'bloomberg_news_process'
log = logs.getLogger(__processor__)


def process(port,keyId,secret,operateDate):
    #get DB connection
    conn = boto.connect_sdb(keyId,secret)
    domain = conn.get_domain("bloomberg_news")
    sql = "select * from {} where updateDate = '{}'".format(operateDate)
    results = domain.select(sql)
    enrichedNewsList = []
    for result in results:
        enrichedNews = process_news(result)
        if enrichedNews:
            enrichedNewsList.append(enrichedNews)
    
    enrichedDomain = conn.get_domain("enriched_news")
    
    #Write the enricheNews to simpleDB and push them into ZMQ
    with queue.open(port, 'w', capture=True) as outq:
        for enrichedNews in enrichedNewsList:
            outq.write(enrichedNews)
            enrichedDomain.put_attributes(enrichedNews["embersId"], enrichedNews)

def process_news(news):
    try:
        content = news["content"]
        
        tokens = nltk.word_tokenize(content)
        stemmer = nltk.stem.snowball.SnowballStemmer('english')
        words = [w.lower().strip() for w in tokens if w not in [",",".",")","]","(","[","*",";","...",":","&",'"',"'","’"] and not w.isdigit()]
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
        enrichedData["content"] = fdist
        enrichedData["stockIndex"] = news["stockIndex"]
        enrichedData["source"] = news["source"]
        enrichedData["updateTime"] = updateTime
        enrichedData["updateDate"] = datetime.strftime(datetime.now(),"%Y-%m-%d")
        embersId = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
        enrichedData["emberdId"] = embersId
    except :
        log.info( "Error****: ", sys.exc_info()[0])
        enrichedData = {}
    finally:
        return enrichedData

def parse_args():
    ap = argparse.ArgumentParser("Preprocess the Bloomberg News")
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-k',dest="key_id",metavar="KeyId for AWS",type=str,nargs="+",help="The key id for aws")
    ap.add_argument('-s',dest="secret",metavar="secret key for AWS",type=str,nargs="+",help="The secret key for aws")
    defaultDay = datetime.strftime(datetime.now(),"%Y-%m-%d")
    ap.add_argument('-d',dest="operate_day",metavar="OPERATION DATE",default=defaultDay,type=str,nargs="?",help="The Day collecting the news")
    return ap.parse_args()    

def main():
    args = parse_args()
    port = args.port
    keyId = args.key_id
    secretKey = args.secret
    operateDay = args.operate_day
    process(port,keyId,secretKey,operateDay)
    
if __name__ == "__main__":
    main()
    