# -*- coding: utf-8 -*-

from __future__ import with_statement
import urllib2
from BeautifulSoup import BeautifulSoup
from datetime import datetime
from boilerpipe.extract import Extractor
import sys
import hashlib
import json
import argparse
from etool import queue,logs

"""
    The Steps for scraping news from Bloomberg
    1. read the config file to initiate the company List dir, AlreadyDownloadedNews file, output dir of Collected result and port for ZMQ
    2. iterate the list of company and get the news list for each company
    3. load AlreadyDownloadedNews file as json object, For each news to be scraped, check if it is already downloaded by check if its title in the AlreadyDownloadedNews file
    4. push the news to ZMQ
"""
companyList = {}
stockNews = {}
newsAlreadyDownload = None
newsAlreadDownloadFilePath = ""
companyListDir = ""
dailyNewsOutPath = ""
port = ""
__processor__ = 'bloomberg_news_ingest'
log = logs.getLogger(__processor__)

def initiate():
    global newsAlreadyDownload
    global companyListDir
    global port
    global newsAlreadDownloadFilePath
    global companyList
    global dailyNewsOutPath
    
    args = parse_args()
    logs.init()
    
    newsAlreadDownloadFilePath = args.f_downloaded 
    companyListDir = args.f_company_list
    port = args.port
    dailyNewsOutPath = args.f_out
    
    newsAlreadyDownload = json.load(open(newsAlreadDownloadFilePath))
    companyList = json.load(open(companyListDir))
    
def get_stock_news():
    global stockNews
    "Scrape the news from Bloomberg"
    for stockIndex in companyList:
        stockNews[stockIndex] = []
        for company in companyList[stockIndex]:
            "construct the url for each company"
            company = company.replace("\r","").replace("\n","").strip()
            companyUrl = "http://www.bloomberg.com/quote/"+company+"/news#news_tab_company_news"
            soup = BeautifulSoup(urllib2.urlopen(companyUrl,timeout=60))
            "Get the News Urls of specifical Company"
            urlElements = soup.findAll(id="news_tab_company_news_panel")
            for urlElement in urlElements:
                elements = urlElement.findAll(attrs={'data-type':"Story"})
                for ele in elements:
                    newsUrl = "http://www.bloomberg.com" + ele["href"]
                    title = ele.string
                    ifExisted = check_article_already_downloaded(title)
                    if ifExisted:
                        continue
                    else:
                        article = get_news_by_url(newsUrl)
                        article["stockIndex"] = stockIndex
                        stockNews[stockIndex].append(article)
            
def get_news_by_url(url):
    article = {}
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        "Get the title of News"
        title = ""
        titleElements = soup.findAll(id="disqus_title")
        for ele in titleElements:
            title = ele.getText().encode('utf-8')
        article["title"] = title 
        
        "Get the posttime of News,Timezone ET"
        postTime = ""
        postTimeElements = soup.findAll(attrs={'class':"datestamp"})
        for ele in postTimeElements:
            timeStamp = float(ele["epoch"])
        #postTime = datetime.strftime("%Y-%m-%d %H:%M:%S",datetime.fromtimestamp(timeStamp/1000))
        postTime = datetime.fromtimestamp(timeStamp/1000)
        postTimeStr = postTime.isoformat()
        article["postTime"] = postTimeStr
        
        "Initiate the post date"
        postDay = postTime.date()
        article["postDate"] = datetime.strftime(postDay,"%Y-%m-%d");
        
        "Get the author information "
        author = ""
        authorElements = soup.findAll(attrs={'class':"byline"})
        for ele in authorElements:
            author = ele.contents[0].strip().replace("By","").replace("-","").replace("and", ",").strip();
        article["author"] = author
        
        "Get the content of article"
        extractor=Extractor(extractor='ArticleExtractor',url=url)
        content = extractor.getText().encode("utf-8")
        article["content"] =  content
        
        "Initiate the Sources"
        source = "Bloomberg News"
        article["source"] = source
        
        "Initiate the update_time"
        updateTime = datetime.now().isoformat()
        article["updateTime"] = updateTime
        
        "Initiate the update_date"
        updateDate = datetime.strftime(datetime.now(),"%Y-%m-%d")
        article["updateDate"] = updateDate
        
        "settup URL"
        article["url"] =  url
        
        "Initiate the embers_id"
        embersId = hashlib.sha1(json.dumps(article)).hexdigest()
        article["embersId"] =  embersId
        
    except:
        log.info("Error: %s",(sys.exc_info()[0],))
        article = {}
    finally:
        return article

def check_article_already_downloaded(title):
    "Check if this article has already been downloaded, if so, then not access the webpage"
    global newsAlreadyDownload
    if title in newsAlreadyDownload:
        return True
    else:
        newsAlreadyDownload.append(title)
        return False
    
def push_news_to_ZMQ():
    global port 
    with queue.open(port, 'w', capture=True) as outq:
        for stock in stockNews:
            for article in stockNews[stock]:
                outq.write(article)  

def output_to_file():
    global stockNews,dailyNewsOutPath
    with open(dailyNewsOutPath,'w') as f_out:
        for stock in stockNews:
            for article in stockNews[stock]:
                f_out.write(json.dumps(article,encoding="utf-8"))
                f_out.write("\n") 
        
    

def parse_args():
    ap = argparse.ArgumentParser("Scrape the content from Bloomberg News and push them to to ZMQ!")
    ap.add_argument('-c',dest="f_company_list",metavar="COMPANY_LIST",default="./data/companyList.json",type=str,nargs='?',help='the company list file')
    ap.add_argument('-d',dest="f_downloaded",metavar="ALREADY DOWNLOADED NEWS",default="./data/BloombergNewsDownloaded.json", type=str,nargs='?',help="The already downloaded news")
    ap.add_argument('-o',dest="f_out",metavar="OUTPUT", type=str,help="The output file of the news")
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    return ap.parse_args() 

def end():
    global newsAlreadDownloadFilePath
    global newsAlreadyDownload
    newsAlreadyDownloadStr = json.dumps(newsAlreadyDownload)
    with open(newsAlreadDownloadFilePath,"w") as output:
        output.write(newsAlreadyDownloadStr)   
             
def main():
    "Initiate the global parameters"
    initiate()
    
    "Get the news related to stock market"
    get_stock_news()
    
    "store the news to File"
    output_to_file()
    
    "Push the news to ZMQ"
    push_news_to_ZMQ()
    
    "Write the title of Already download news to File"
    end()

if __name__ == "__main__":
    log.info("Start Time: %s",(datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),))
    main()
    log.info("End Time: %s",(datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"),))

    