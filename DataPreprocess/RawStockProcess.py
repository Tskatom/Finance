#-*- coding:utf8 -*-
import json
import sqlite3 as lite
import sys
import hashlib
from Util import calculator
from Util import common

#rawData = {"previousCloseValue":"2381.22","stockIndex":"MERVAL","updateTime":"05/11/2012 16:01:01","feed":"Bloomberg - Stock Index","queryTime":"08/12/2012 00:59:02","currentValue":"2410.85","embersId":"6364b631340cc9b0a32816ee3943ae2cee6baa2a"}
enrichedData = {}

con = None
cur = None
stockRecords = {}

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

def check_if_existed(rawIndexData):
    global cur
    ifExisted = True
    sql = "select count(*) from t_daily_stockindex where stock_index = ? and date = ?"
    stockIndex =  rawIndexData["stockIndex"]  
    tmpUT =  rawIndexData["updateTime"].split(" ")[0]
    updateTime = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1] 
    print updateTime
    cur.execute(sql,(stockIndex,updateTime))
    count = cur.fetchone()[0]
    if count == 0:
        ifExisted = False
    return ifExisted

 
def get_subsequence(stockIndex,updateDate):
    global cur
    sql = "select max(sub_sequence) from t_daily_stockindex where stock_index = ? and date = ?"
    cur.execute(sql,(stockIndex,updateDate))
    count = cur.fetchone()[0]
    if count == None:
        count = 0
    return count

def getZscore(curDate,stockIndex,curDiff,duration):
    global con
    global cur
    scores = []
    sql = "select one_day_change from t_daily_stockindex where date<? and stock_index = ? order by date desc limit ?"
    cur.execute(sql,(curDate,stockIndex,duration))
    rows = cur.fetchall()
    for row in rows:
        scores.append(row[0])
    zscore = calculator.calZscore(scores, curDiff)
    return zscore
    
def import_data(rawIndexData):
    global con
    global cur
    "Check if current data already in database, if not exist then insert otherwise skip"
    ifExisted = check_if_existed(rawIndexData)
    print ifExisted
    if not ifExisted:
        sql = "insert into t_daily_stockindex (embers_id,sub_sequence,stock_index,date,last_price,one_day_change,zscore30,zscore90) values (?,?,?,?,?,?,?,?) "
        embersId = rawIndexData["embersId"]
        stockIndex = rawIndexData["stockIndex"]
        tmpUT =  rawIndexData["updateTime"].split(" ")[0]
        updateTime = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1]
        lastPrice = float(rawIndexData["currentValue"])
        preLastPrice = float(rawIndexData["previousCloseValue"])
        oneDayChange = lastPrice - preLastPrice
        subSequence = get_subsequence(stockIndex,updateTime) + 1
        
        "calculate zscore 30"
        zscore30 = getZscore(updateTime,stockIndex,oneDayChange,30)
        zscore90 = getZscore(updateTime,stockIndex,oneDayChange,90)
        
        cur.execute(sql,(embersId,subSequence,stockIndex,updateTime,lastPrice,oneDayChange,zscore30,zscore90))
        con.commit()
        
        "Initiate the enriched Data"
        trendType = get_trend_type(rawIndexData)
        derivedFrom = "[" + embersId + "]"
        enrichedData["derivedFrom"] = derivedFrom
        enrichedData["stockIndex"] = stockIndex
        enrichedData["date"] = updateTime
        enrichedData["lastPrice"] = lastPrice
        enrichedData["oneDayChange"] = oneDayChange
        enrichedData["changePercent"] = round((lastPrice - preLastPrice)/preLastPrice,4)
        enrichedData["trendType"] = trendType
        enrichedData["subsequenceId"] = subSequence
        enrichedDataEmID = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
        enrichedData["embersId"] = enrichedDataEmID
        
        print enrichedData
        insert_enriched_data()
        
def insert_enriched_data():
    global con
    global cur
    sql = "insert into t_daily_enrichedindex (embers_id,derived_from,sub_sequence,stock_index,date,last_price,one_day_change,change_percent,trend_type) values (?,?,?,?,?,?,?,?,?)"
    enrichedDataEmID = enrichedData["embersId"]
    derivedFrom = enrichedData["derivedFrom"]
    subSequence = enrichedData["subsequenceId"]
    stockIndex = enrichedData["stockIndex"] 
    updateTime = enrichedData["date"] 
    lastPrice = enrichedData["lastPrice"] 
    oneDayChange = enrichedData["oneDayChange"] 
    changePercent = enrichedData["changePercent"]
    trendType = enrichedData["trendType"]
    cur.execute(sql,(enrichedDataEmID,derivedFrom,subSequence,stockIndex,updateTime,lastPrice,oneDayChange,changePercent,trendType))
    con.commit()
    
def get_trend_type(rawIndexData):
    """
    Computing current day's trend type, compareing change percent to the trend range,
    Choose the nearnes trend as current day's type    
    """
    "Load the trend type range file"
    rangeFilePath = common.get_configuration("model", "TREND_RANGE_FILE")
    tFile = open(rangeFilePath)
    trendsJson = json.load(tFile)
    
    "Get the indicated stock range"
    stockIndex = rawIndexData["stockIndex"]
    tJson = trendsJson[stockIndex]
    print tJson
    
    "Computing change percent"
    lastPrice = float(rawIndexData["currentValue"])
    preLastPrice = float(rawIndexData["previousCloseValue"])
    changePercent = round((lastPrice - preLastPrice)/preLastPrice,4)
    
    distance = 10000
    trendType = None
    for type in tJson:
        tmpDistance = min(abs(changePercent-tJson[type][0]),abs(changePercent-tJson[type][1]))
        if tmpDistance < distance:
            distance = tmpDistance
            trendType = type
    return trendType
    
def execute(rawData):
    get_db_connection()
    import_data(rawData)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print len(sys.argv) ,"Please Input Raw stock index value as following format: \n", '{"previousCloseValue":"2381.22","stockIndex":"MERVAL","updateTime":"05/11/2012 16:01:01","feed":"Bloomberg - Stock Index","queryTime":"08/12/2012 00:59:02","currentValue":"2410.85","embersId":"6364b631340cc9b0a32816ee3943ae2cee6baa2a"}'
        exit(0)
    rawDataStr = sys.argv[1]
    print rawDataStr
    rawData = json.loads(rawDataStr)
    execute(rawData)