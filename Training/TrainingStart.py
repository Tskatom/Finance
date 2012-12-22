#-*- coding:utf8-*-
from Util import common
import sys
import json
import time
import argparse
import nltk
import hashlib
from datetime import datetime
import sqlite3 as lite

from Training import ClusteringTraingSet as ct
from Training import CreatingTrendsContribution as ctc
from Training import CreatingVocabulary as cv
from Training import OutputTestStockIndexData
from Training import CreatingTermContribution as ctermc
from DataPreprocess import ImportNewsProcess as inp,import_historical_stock_v2 as ihs
from etool import logs
import cProfile

"""
For the Test Phase, we need to do the following steps:
1. Cluster the time serial of stock index value
2. Create the vocabulary List 
3. Compute the Trends Contribution
4. Computing Term Contribution

The prerequisite of these 4 steps is import the Archieve News and Historical Stock index values into database Firstly
And One thing need to do is clear all the Enriched data, surrogatedata and Warning data
"""

__processor__ = "TraingingStart"
log = logs.getLogger(__processor__)
logs.init()

#Clear the Testing Phase data in database to retrainng the data
def data_clear():
    con = common.getDBConnection()
    cur = con.cursor()
    
    "clear the surrogate data"
    clearSql = "delete from t_surrogatedata"
    cur.execute(clearSql)
    con.commit()
    
    "clear the warning data"
    clearSql = "delete from t_warningmessage"
    cur.execute(clearSql)
    con.commit()
    time.sleep(3)
    if con:
        con.close()

def divide_archived_news(traingingStart,trainingEnd,estimationStart,estimationEnd):
    archivedNewsPath = common.get_configuration("training", "GROUP_STOCK_NEWS")
    
    timelineBegin = time.strptime(traingingStart, "%Y-%m-%d")
    timelineEnd = time.strptime(trainingEnd, "%Y-%m-%d")
    
    "Write Training data and Test Data to File"
    trainingFilePath = common.get_configuration("training", "TRAINING_NEWS_FILE")
    with open(trainingFilePath,"w") as output:
        for line in open(archivedNewsPath,"r"):
            news = json.loads(line)
            post_date = time.strptime(news["postDate"],"%Y-%m-%d")
            if post_date <= timelineEnd and post_date >= timelineBegin:
                output.write(json.dumps(news))
                output.write("\n")

def check_enrichedata_existed(embersId):
    try:
        con = common.getDBConnection()
        cur = con.cursor()
        flag = True
        sql = "select count(*) count from t_daily_enrichednews where embers_id=?"
        cur.execute(sql,(embersId,))
        count = cur.fetchone()[0]
        count = int(count)
        if count == 0:
            flag = False
        else:
            flag = True
    except lite.ProgrammingError as e:
        log.info(e)
    except:
        log.info( "Error: %s" %(sys.exc_info()[0]))
    finally:
        return flag

def get_uncompleted_mission():
    con = common.getDBConnection()
    cur = con.cursor()
    try:
        sql = "select embers_id from t_news_process_mission where mission_status = '0'"
        cur.execute(sql)
        rows = cur.fetchall()
        i = 0
        for row in rows:
            sql2 = "select embers_id,title,author,post_time,post_date,stock_index,content,source,update_time from t_daily_news where embers_id=?"
            cur2 = con.cursor()
            cur2.execute(sql2,(row[0],))
            rows2 = cur2.fetchall()
            for row2 in rows2:
                insertSql = "insert into t_daily_enrichednews (embers_id,derived_from,title,author,post_time,post_date,content,stock_index,source,raw_update_time,update_time) values (?,?,?,?,?,?,?,?,?,?,?)"
                updateSql = "update t_news_process_mission set mission_status=? where embers_id=?"
                derivedFrom = "["+row2[0]+"]"
                title = row2[1]
                author = row2[2]
                postTime = row2[3]
                postDate = row2[4]
                stockIndex = row2[5]
                content = row2[6]
                source = row2[7]
                rawUpdateTime = row2[8]
                try:
                    tokens = nltk.word_tokenize(content)
                    stemmer = nltk.stem.snowball.SnowballStemmer('english')
                    words = [w.lower().strip() for w in tokens if w not in [",",".",")","]","(","[","*",";","...",":","&",'"',"'","’"] and not w.isdigit()]
                    words = [w for w in words if w.encode("utf8") not in nltk.corpus.stopwords.words('english')]
#                        stemmedWords = [stemmer.stem(w) for w in words]
                    stemmedWords = []
                    currentWord = ""
                    for w in words:
                        currentWord = w
                        stemmedWords.append(stemmer.stem(w))
                    fdist=nltk.FreqDist(stemmedWords)
                    jsonStr = json.dumps(fdist)
                    embersId = hashlib.sha1(jsonStr).hexdigest()
                    updateTime = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    
                    enrichedData = {}
                    enrichedData["emberdId"] = embersId
                    enrichedData["derivedFrom"] = derivedFrom
                    enrichedData["title"] = title
                    enrichedData["author"] = author
                    enrichedData["postTime"] = postTime
                    enrichedData["postDate"] =  postDate
                    enrichedData["content"] = jsonStr
                    enrichedData["stockIndex"] = stockIndex
                    enrichedData["source"] = source
                    enrichedData["updateTime"] = updateTime
                    enrichedData["rawUpdateTime"] = rawUpdateTime
                    
                    cur3 = con.cursor()
                    if not check_enrichedata_existed(embersId):
                        cur3.execute(insertSql,(embersId,derivedFrom,title,author,postTime,postDate,jsonStr,stockIndex,source,rawUpdateTime,updateTime))
                        
                    cur3.execute(updateSql,("1",row2[0],)) 
                    i = i + 1
                    if i%100 == 0:
                        con.commit()
                except lite.ProgrammingError as e:
                    log.info(e)            
                except:
                    continue
        con.commit()        
    except lite.OperationalError as e:
        log.info(e)
    except:
        log.info("Error:%s" %(sys.exc_info()[0]))

def execute(traingStart,traingEnd,estimationStart,estimationEnd,key_num,clu_num,days_back):
    "Clear the database data"
    print "Clear Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    data_clear()
    print "Clear End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Divide the Originial News File into Two Parts:Training part and Test Part"
    print "Divide News File Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    divide_archived_news(traingStart,traingEnd,estimationStart,estimationEnd)
    print "Divide News File End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Clustering the time serial Stock index value"
    print "Clustering Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    ct.clusterSet(traingStart,traingEnd,clu_num)
    print "Clustering End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Computing the trends contribution and probability"
    print "Trend Contribution Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    ctc.compute_trend_contribution(clu_num)
    print "Trend Contribution End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Creating the Vocabulary"
    print "Creating the Vocabulary Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    cv.create_vocabulary(key_num)
    print "Creating the Vocabulary End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Computing the Term Contribution"
    print "Computing the Term Contribution Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    ctermc.compute_term_contribution(days_back)
    print "Computing the Term Contribution End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
#    "RawNewsProcess Start"
#    print "RawNewsProcess Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
#    get_uncompleted_mission()
#    print "RawNewsProcess End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
#    "Import Enriched Data Start"
#    print "Import Enriched Data Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
#    OutputTestStockIndexData.export_test_stock_data(estimationStart,estimationEnd)
#    print "Import Enriched Data End Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
def parse_args():
    ap = argparse.ArgumentParser("The automatic training and test tools")
    ap.add_argument('-c','--conf',metavar='CONFIG',type=str,default='../Config/config.cfg',nargs='?',help='The path of config file')
    ap.add_argument('-ts','--tStart',metavar='TRAING START',type=str,nargs='?',help='The start day of Training set')    
    ap.add_argument('-td','--tEnd',metavar='TRAING END',type=str,nargs='?',help='The end day of Training set')
    ap.add_argument('-es','--eStart',metavar='ESTIMATION START',type=str,nargs='?',help='The start day of estimation set')
    ap.add_argument('-ed','--eEnd',metavar='ESTIMATION End',type=str,nargs='?',help='The end day of estimation set')
    ap.add_argument('-k',dest="key_numbers",type=int,nargs='?',default=200,help='The number of key words')
    ap.add_argument('-clu',dest="c_numbers",type=int,nargs='?',default=11,help='The numberf of clusters')
    ap.add_argument('-db',dest="days_back",type=int,nargs='?',default=1,help='The numberf of clusters')
    return ap.parse_args()

def main():
    args = parse_args()
    configFilePath = args.conf
    trainingStart = args.tStart
    trainingEnd = args.tEnd
    estimationStart = args.eStart
    estimationEnd = args.eEnd
    key_num = args.key_numbers
    clu_num = args.c_numbers
    days_back = args.days_back
    
    common.init(configFilePath)
    
    execute(trainingStart,trainingEnd,estimationStart,estimationEnd,key_num,clu_num,days_back)

if __name__ == "__main__":
    cProfile.run("main()")
    
    
    