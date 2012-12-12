# -*- coding: utf-8 -*-

from __future__ import with_statement
import ConfigParser
import os
import urllib2
from BeautifulSoup import BeautifulSoup
from datetime import datetime
from boilerpipe.extract import Extractor
import sqlite3 as lite
import sys
import hashlib
import json

companyList = {}
stockNews = {}
con = None
cur = None
config = None
newsAlreadyDownload = None

def initiate():
    global config
    global newsAlreadyDownload
    
    config = ConfigParser.ConfigParser()
    with open('../Config/config.cfg','r') as cfgFile:
        config.readfp(cfgFile)
    newsAlreadDownloadFilePath = config.get("model", "newsAlreadyDownload") 
    newsAlreadyDownload = json.load(open(newsAlreadDownloadFilePath))
    
    get_db_connection()
    
def end():
    global config
    global newsAlreadyDownload
    
    config = ConfigParser.ConfigParser()
    with open('../Config/config.cfg','r') as cfgFile:
        config.readfp(cfgFile)
    newsAlreadDownloadFilePath = config.get("info", "newsAlreadyDownload") 
    newsAlreadyDownloadStr = json.dumps(newsAlreadyDownload)
    with open(newsAlreadDownloadFilePath,"w") as output:
        output.write(newsAlreadyDownloadStr)
    
    close_db_connection() 
        
def get_db_connection():
    global cur
    global con
    try:
        con = lite.connect("d:/sqlite/embers.db")
        con.text_factory = str
        cur = con.cursor()
    except lite.Error, e:
        print "Error: %s" % e.args[0]

def close_db_connection():
    global con
    con.commit()
    if con:
        con.close()    

def get_all_companies():
    "Read Company List Directory from config file"
    global config
    companyListDir = config.get('info','companyListDir')
    dirList = os.listdir(companyListDir)
    "Iteratively read the stock member files and store them in a List "
    for fName in dirList:
        stockIndex = fName[4:len(fName)-4]
        companyList[stockIndex] = []
        filePath = companyListDir + "/" + fName
        with open(filePath,'r') as comFile:
            lines = comFile.readlines()
            for line in lines:
                tickerName = line.replace("\n","").split(" ")[0] + ":" + line.replace("\n","").split(" ")[1]
                companyList[stockIndex].append(tickerName)
    return companyList

def get_stock_news():
    "Scrape the news from Bloomberg"
    for stockIndex in companyList:
        stockNews[stockIndex] = []
        for company in companyList[stockIndex]:
            "construct the url for each company"
            companyUrl = "http://www.bloomberg.com/quote/"+company+"/news#news_tab_company_news";
            print companyUrl
            soup = BeautifulSoup(urllib2.urlopen(companyUrl))
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
                        article["stock_index"] = stockIndex
                        stockNews[stockIndex].append(article)
            
def get_news_by_url(url):
    print "Come to get_news_by_url"
    article = {}
    try:
        soup = BeautifulSoup(urllib2.urlopen(url))
        "Get the title of News"
        title = ""
        titleElements = soup.findAll(id="disqus_title")
        for ele in titleElements:
            title = ele.getText().encode('utf-8')
        article["title"] = title 
        print title
        
        "Get the posttime of News,Timezone ET"
        postTime = ""
        postTimeElements = soup.findAll(attrs={'class':"datestamp"})
        for ele in postTimeElements:
            timeStamp = float(ele["epoch"])
        postTime = datetime.fromtimestamp(timeStamp/1000)
        article["post_time"] = postTime
        
        "Initiate the post date"
        postDay = postTime.date()
        article["post_date"] = postDay;
        
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
        updateTime = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
        article["update_time"] = updateTime
        
        "Initiate the embers_id"
        embersId = hashlib.sha1(content).hexdigest()
        article["embers_id"] =  embersId

        "settup URL"
        article["url"] =  url
    except:
        print "Error: %s" %sys.exc_info()[0]
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
    
def check_article_existed(article):
    try:
        global con
        global cur
        flag = True
        title = article["title"]
        postDay = datetime.strftime(article["post_date"],"%Y-%m-%d")
        sql = "select count(*) count from t_daily_news where post_date=? and title=?"
        cur.execute(sql,(postDay,title,))
        count = cur.fetchone()[0]
        count = int(count)
        if count == 0:
            flag = False
        else:
            flag = True
    except lite.ProgrammingError as e:
        print e
    except:
        print "Error: %s" %sys.exc_info()[0]
    finally:
        return flag

def insert_news(article):
    try:
        global con
        sql = "insert into t_daily_news(embers_id,title,author,post_time,post_date,content,stock_index,source,update_time,url) values (?,?,?,?,?,?,?,?,?,?)"
        embersId = article["embers_id"]
        title = article["title"]
        author = article["author"]
        postTime = article["post_time"]
        postDate = article["post_date"]
        content = article["content"]
        stockIndex = article["stock_index"]
        source = article["source"]
        updateTime = article["update_time"]
        url = article["newsUrl"]
        cur.execute(sql,(embersId,title,author,postTime,postDate,content,stockIndex,source,updateTime,url))
        
    except lite.Error, e:
        print "Error: %s" % e.args[0]
    finally:
        pass

def insert_news_mission(article):
    try:
        global con
        global cur
        sql = "insert into t_news_process_mission(embers_id,mission_name,mission_status,insert_time) values (?,?,?,datetime('now','localtime'))"
        
        embersId = article["embers_id"]
        missionName = "Bag of Words"
        missionStatus = "0"
        cur.execute(sql,(embersId,missionName,missionStatus))
        
    except lite.Error, e:
        print "Error: %s" % e.args[0]
    finally:
        pass
    
def import_to_database():
    global con
    for stock in stockNews:
        i = 0
        for article in stockNews[stock]:
            article["stock_index"] = stock
            "Check if the article has being collected: if so,just skip, otherwise insert into database"
            "commit to database for each 10 records"
            ifExisted = check_article_existed(article)
            if ifExisted:
                continue
            else:
                insert_news(article)
                i = i +1
                if i >= 100:
                    con.commit()
                    i = 0
                
def execute():
    get_all_companies()
    get_stock_news()
    import_to_database()
    end()

initiate()
#get_db_connection()
if __name__ == "__main__":
    execute()