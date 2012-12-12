#-*- coding:utf8 -*-
import json
import sqlite3 as lite
import sys
import hashlib
from Util import calculator
from Util import common
from etool import logs
import argparse

#rawData = {'feed': 'Bloomberg - Stock Index', 'updateTime': '09/28/2012 16:01:02', 'name': 'MERVAL', 'currentValue': '2451.73', 'queryTime': '10/01/2012 03:00:03', 'previousCloseValue': '2494.18', 'date': '2012-10-01T03:00:03', 'type': 'stock', 'embersId': '971752f23223c344e8732f32922e3f8e75ebd3ff'}
#EnrichedData = 

con = None
cur = None
stockRecords = {}

__processor__ = 'stock_process'
log = logs.getLogger(__processor__)

def init(cfgPath):
    common.init(cfgPath)
    logs.init()

def get_db_connection():
    global cur
    global con
    try:
        con = common.getDBConnection()
        con.text_factory = str
        cur = con.cursor()
    except lite.Error, e:
        log.info("Error: %s" % e.args[0])

def close_db_connection():
    global con
    con.commit()
    if con:
        con.close()  

def check_if_existed(rawIndexData):
    global cur
    ifExisted = True
    sql = "select count(*) from t_daily_stockindex where stock_index = ? and date = ?"
    stockIndex =  rawIndexData["name"]  
    tmpUT =  rawIndexData["updateTime"].split(" ")[0]
    updateTime = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1] 
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
    if not ifExisted:
        sql = "insert into t_daily_stockindex (embers_id,sub_sequence,stock_index,date,last_price,one_day_change,zscore30,zscore90) values (?,?,?,?,?,?,?,?) "
        embersId = rawIndexData["embersId"]
        stockIndex = rawIndexData["name"]
        tmpUT =  rawIndexData["updateTime"].split(" ")[0]
        updateTime = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1]
        lastPrice = float(rawIndexData["currentValue"])
        preLastPrice = float(rawIndexData["previousCloseValue"])
        oneDayChange = lastPrice - preLastPrice
        subSequence = get_subsequence(stockIndex,updateTime) + 1
        
        "calculate zscore 30 and zscore 90"
        zscore30 = getZscore(updateTime,stockIndex,oneDayChange,30)
        zscore90 = getZscore(updateTime,stockIndex,oneDayChange,90)
        
        cur.execute(sql,(embersId,subSequence,stockIndex,updateTime,lastPrice,oneDayChange,zscore30,zscore90))
        con.commit()
        
        "Initiate the enriched Data"
        enrichedData = {}
        
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
        
        insert_enriched_data(enrichedData)
        
def insert_enriched_data(enrichedData):
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
    Computing current day's trend changeType, compareing change percent to the trend range,
    Choose the nearnes trend as current day's changeType    
    """
    "Load the trend changeType range file"
    rangeFilePath = common.get_configuration("model", "TREND_RANGE_FILE")
    tFile = open(rangeFilePath)
    trendsJson = json.load(tFile)
    tFile.close()
    
    "Get the indicated stock range"
    stockIndex = rawIndexData["name"]
    tJson = trendsJson[stockIndex]
    
    "Computing change percent"
    lastPrice = float(rawIndexData["currentValue"])
    preLastPrice = float(rawIndexData["previousCloseValue"])
    changePercent = round((lastPrice - preLastPrice)/preLastPrice,4)
    
    distance = 10000
    trendType = None
    for changeType in tJson:
        tmpDistance = min(abs(changePercent-tJson[changeType][0]),abs(changePercent-tJson[changeType][1]))
        if tmpDistance < distance:
            distance = tmpDistance
            trendType = changeType
            
    #According the current change percent to adjust the range of trend type
    bottom = tJson[trendType][0]
    top = tJson[trendType][1]
    
    if changePercent > top:
        top = changePercent
    
    if changePercent < bottom:
        bottom = changePercent
    
    trendsJson[stockIndex][trendType][0] = bottom
    trendsJson[stockIndex][trendType][1] = top
    
    with open(rangeFilePath,"w") as rangeFile:
        rangeFile.write(json.dumps(trendsJson))
        
    return trendType
    
def execute(rawDataPath,cfgPath):
    init(cfgPath)
    get_db_connection()
    rawDataList = []
    with open(rawDataPath,'r') as rawDataFile:
        lines = rawDataFile.readlines()
        for line in lines:
            rawData = json.loads(line.replace("\n","").replace("\r",""))
            rawDataList.append(rawData)
            
    for rawData in rawDataList:
        import_data(rawData)
    close_db_connection()

def parse_args():
    ap = argparse.ArgumentParser("Process the raw stock index data")
    ap.add_argument('-f',dest="bloomberg_price_file",metavar="STOCK PRICE",type=str,help='The stock price file')
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-k',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
    ap.add_argument('-s',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
    return ap.parse_args() 
        
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print len(sys.argv) ,"Please Enter the Path of the rawData\n", ''
        exit(0)
    rawDataPath = sys.argv[1]
    execute(rawDataPath)