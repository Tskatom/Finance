#-*- coding:utf8 -*-
import nltk
import json
import sqlite3 as lite
import sys
import exceptions
import hashlib
from datetime import datetime
from Util import common

con = None
cur = None

def get_db_connection():
    global cur
    global con
    try:
        con = common.getDBConnection()
        con.text_factory = str
        cur = con.cursor()
    except lite.Error, e:
        print "Error: %s" % e.args[0]

def close_db_connection():
    global con
    con.commit()
    if con:
        con.close()  

def get_uncompleted_mission():
    global con
    global cur
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
                updateSql = "update t_news_process_mission set mission_status='1' and finish_time=? where embers_id=?"
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
                    words = [w.lower() for w in tokens if w not in [",",".",")","]","(","[","*",";","...",":","&",'"'] and not w.isdigit()]
                    words = [w for w in words if w.encode("utf8") not in nltk.corpus.stopwords.words('english')]
                    stemmedWords = [stemmer.stem(w) for w in words]
                    fdist=nltk.FreqDist(stemmedWords)
                    jsonStr = json.dumps(fdist)
                    embersId = hashlib.sha1(jsonStr).hexdigest()
                    updateTime = datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
                    cur.execute(insertSql,(embersId,derivedFrom,title,author,postTime,postDate,jsonStr,stockIndex,source,rawUpdateTime,updateTime))
                    cur.execute(updateSql,(updateTime,row2[0])) 
                    i = i + 1
                    if i%100 == 0:
                        con.commit()
                except lite.ProgrammingError as e:
                    print e               
                except:
                    print "Error: ", sys.exc_info()[0]
                    continue
    except exceptions.IndexError as e:
        print e            
    except lite.OperationalError as e:
        print e
    except:
        print "Error: ", sys.exc_info()[0]

def execute():
    get_db_connection()
    get_uncompleted_mission()
    close_db_connection()
    
if __name__ == "__mian__":
    execute()