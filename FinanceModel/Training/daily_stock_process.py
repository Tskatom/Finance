#-*- coding:utf8 -*-
import json
import sqlite3 as lite
import hashlib
from Util import calculator
from etool import logs
import argparse
from datetime import datetime

#rawData = {'feed': 'Bloomberg - Stock Index', 'updateTime': '09/28/2012 16:01:02', 'name': 'MERVAL', 'currentValue': '2451.73', 'queryTime': '10/01/2012 03:00:03', 'previousCloseValue': '2494.18', 'date': '2012-10-01T03:00:03', 'type': 'stock', 'embersId': '971752f23223c344e8732f32922e3f8e75ebd3ff'}
#EnrichedData = 

"""
Description: 
    input Parameters:
    ap.add_argument('-f',dest="bloomberg_price_file",metavar="STOCK PRICE",type=str,help='The daily stock price file')
    ap.add_argument('-t',dest="trend_file",metavar="TREND RANGE FILE",type=str,help='The trend type range')
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
Data flow:
    1> read daily stock price from input file
    2> store the raw price to table : t_bloomberg_prices
    3> constructure enriched_price and store the enriched price to table: t_enriched_bloomberg_price
        one_day_change, change_percent,zscore30,zscore90,trendType
    4> push the enriched_price to ZMQ
"""

__processor__ = 'stock_process'
log = logs.getLogger(__processor__)
logs.init()
TREND_RANGE = {}

def getZscore(conn,cur_date,stock_index,cur_diff,duration):
    cur = conn.cursor()
    scores = []
    sql = "select one_day_change from t_enriched_bloomberg_prices where post_date<? and name = ? order by post_date desc limit ?"
    cur.execute(sql,(cur_date,stock_index,duration))
    rows = cur.fetchall()
    for row in rows:
        scores.append(row[0])
    zscore = calculator.calZscore(scores, cur_diff)
    return zscore
    
        
def get_trend_type(TREND_RANGE,stockIndex,changePercent):
    """
    Computing current day's trend changeType, compareing change percent to the trend range,
    Choose the nearnes trend as current day's changeType    
    """
    
    "Get the indicated stock range"
    tJson = TREND_RANGE[stockIndex]
    
    "Computing change percent"
    
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
    
    TREND_RANGE[stockIndex][trendType][0] = bottom
    TREND_RANGE[stockIndex][trendType][1] = top
    
    return trendType
    

def process(conn,TREND_RANGE,stockIndex,predict_date):
    
    sql = "select embers_id,change_percent from t_enriched_bloomberg_prices where name=? and post_date<? order by post_date desc limit 1"
    cur = conn.cursor()
    cur.execute(sql,[stockIndex,predict_date])
    r = cur.fetchone()
    embersId = r[0]
    changePercent = r[1]    
    trendType = get_trend_type(TREND_RANGE,stockIndex,changePercent)
    
    sql = "update t_enriched_bloomberg_prices set trend_type=? where embers_id = ?"
    cur.execute(sql,([trendType,embersId]))
    conn.commit()        

def process_data(conn,trend_file,predict_date,stock_list):
    #initiate parameters
#    TREND_RANGE
#    args = parse_args()
#    bloomberg_price_file = args.bloomberg_price_file
    "Load the trend changeType range file"
    trendObject = None
    with open(trend_file,"r") as tFile:
        trendObject = json.load(tFile)
    "Get the latest version of Trend Ranage"
    trend_versionNum = max([int(v) for v in trendObject.keys()])
    "To avoid changing the initiate values, we first transfer the json obj to string ,then load it to create a news object"
    TREND_RANGE = json.loads(json.dumps(trendObject[str(trend_versionNum)]))
    
    #get raw price list
#    raw_price_list = []
#    with open(bloomberg_price_file,'r') as raw_file:
#        lines = raw_file.readlines()
#        for line in lines:
#            raw_data = json.loads(line.replace("\n","").replace("\r",""))
#            raw_price_list.append(raw_data)
            
    #process data one by one
    for stock in stock_list:
        process(conn,TREND_RANGE,stock,predict_date)
    
    "Write back the trendFile"
    new_version_num = trend_versionNum + 1
    trendObject[str(new_version_num)] = TREND_RANGE
    with open(trend_file,"w") as tFile:
        tFile.write(json.dumps(trendObject))

def test():
    stock_list = ["MERVAL","MEXBOL"]
    trend_file = "./trendRange.json"
    conn = lite.Connection("d:/embers/embers_v2.db")
    predict_date = "2012-07-12"
    process_data(conn,trend_file,predict_date,stock_list)

if __name__ == "__main__":
    test()
