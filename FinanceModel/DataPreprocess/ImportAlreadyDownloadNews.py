'''
Created on Oct 3, 2012

@author: Vic
'''
import sqlite3 as lite
import json
import ConfigParser
from Util import common

def execute():
    try:
        con = common.getDBConnection()
        con.text_factory = str
        cur = con.cursor()
        
        config = ConfigParser.ConfigParser()
        with open('../Config/config.cfg','r') as cfgFile:
            config.readfp(cfgFile)
        newsAlreadDownloadFilePath = config.get("model", "NEWS_ALREADY_DOWNLOADED") 
        newsAlreadyDownload = []
        sql = "select title from t_daily_news"
        cur.execute(sql)
        rows = cur.fetchall()
        i = 0
        for row in rows:
            newsAlreadyDownload.append(row[0])
            i = i + 1
            print i
        with open(newsAlreadDownloadFilePath,"w") as output:
            output.write(json.dumps(newsAlreadyDownload))
    except lite.Error, e:
        print "Error: %s" % e.args[0]

execute()