import sqlite3 as lite
from Util import common
import json
from datetime import datetime
import hashlib
from etool import logs
import sys
import argparse
# import history raw data into database    
con = None
cur = None
__processor__ = "ImportArchivedNews"
log = logs.getLogger(__processor__)

def init():
    global con
    global cur
    
    con = common.getDBConnection()
    cur = con.cursor()
    logs.init()

def insert_news(article):
    try:
        global con
        global cur
        
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
        
    except lite.Error, e:
        log.info( "Error insert_news: %s" % e.args[0])
    finally:
        pass

def check_article_existed(article):
    try:
        global con
        global cur
        flag = True
        title = article["title"]
        postDay = article["postDate"]
        sql = "select count(*) count from t_daily_news where post_date=? and title=?"
        cur.execute(sql,(postDay,title,))
        count = cur.fetchone()[0]
        count = int(count)
        if count == 0:
            flag = False
        else:
            flag = True
    except lite.ProgrammingError as e:
        log.info( e)
    except:
        log.info( "Error+++++: %s" %sys.exc_info())
    finally:
        return flag  
    
def insert_news_mission(article):
    try:
        global con
        global cur
        sql = "insert into t_news_process_mission(embers_id,mission_name,mission_status,insert_time) values (?,?,?,datetime('now','localtime'))"
        
        embersId = article["embersId"]
        missionName = "Bag of Words"
        missionStatus = "0"
        cur.execute(sql,(embersId,missionName,missionStatus))
        
    except lite.Error, e:
        log.info( "Error Insert news Mission: %s" % e.args[0])
    finally:
        pass
            
def import_news_to_database():
    try:
        global con
        init()
        historyNews = open(common.get_configuration( "training", 'GROUP_STOCK_NEWS'))
        historyNewsJson = json.load(historyNews)
        i = 0
        for stockIndex in historyNewsJson:
            for article in historyNewsJson[stockIndex].values():
                news = {}
                news["title"] = article["title"]
                news["author"] = article["author"]
                postTime = article["postTime"].split(".")[0]
                postTime = datetime.strptime(postTime,"%Y-%m-%d %H:%M:%S")
                news["postTime"] = postTime
                news["postDate"] = postTime.date()
                news["content"] = article["content"]
                news["stockIndex"] = stockIndex
                news["source"] = "Bloomberg News"
                news["updateTime"] = article["queryTime"]
                news["url"] = article["newsUrl"]
                embersId = hashlib.sha1(article["content"]).hexdigest()
                news["embersId"] = embersId
                ifExisted = check_article_existed(news)
                if not ifExisted:
                    insert_news(news)
                    "Insert into Mission process"
                    insert_news_mission(news)
                i = i + 1
                if i % 1000 == 0:
                    con.commit()
        con.commit()
    except lite.Error, e:
        print "Error: %s" % e.args[0]
    finally:
        con.close()

def parse_args():
    ap = argparse.ArgumentParser("Import the archived news")
    ap.add_argument('-c','--conf',metavar='CONFIG',type=str,default='../Config/config.cfg',nargs='?',help='The path of config file')        
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    conf = args.conf
    common.init(conf)
    import_news_to_database()