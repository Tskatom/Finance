'''
Created on Oct 3, 2012

@author: Vic
'''
import json
import sqlite3 as lite
from Util import common
import hashlib

def execute():
    try:
        con = common.getDBConnection()
        cur = con.cursor()
        
        stockIndexValues = []
        sql = "select sub_sequence,date,last_price,one_day_change,zscore30,zscore90,stock_index from t_daily_stockindices"
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            indexValue = {}
            indexValue["sub_sequence"] = row[0]
            indexValue["date"] = row[1]
            indexValue["lastPrice"] = row[2]
            indexValue["oneDayChange"] = row[3]
            indexValue["zscore30"] = row[4]
            indexValue["zscore90"] = row[5]
            indexValue["stockIndex"] = row[6]
            embersId = hashlib.sha1(json.dumps(indexValue)).hexdigest()
            indexValue["embersId"] = embersId
            stockIndexValues.append(indexValue)
        
        insertSql = "insert into t_daily_stockindex(embers_id,sub_sequence,stock_index,date,last_price,one_day_change,zscore30,zscore90) values (?,?,?,?,?,?,?,?) "
        for stock in stockIndexValues:
            cur.execute(insertSql,(stock["embersId"], stock["sub_sequence"], stock["stockIndex"],stock["date"],stock["lastPrice"],stock["oneDayChange"],stock["zscore30"],stock["zscore90"]))
        
        con.commit()
    except lite.Error, e:
        print "Error: %s" % e.args[0]

if __name__ == "__main__":
    execute()