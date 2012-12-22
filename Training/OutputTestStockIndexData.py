#-*- coding:utf8-*-
from Util import common
import json
import hashlib
from Util import calculator
from datetime import datetime


def insert_enriched_data(conn,enrichedData):
    cur = conn.cursor()
    sql = "insert into t_enriched_bloomberg_prices (embers_id,derived_from,type,name,post_date,operate_time,current_value,previous_close_value,one_day_change,change_percent,zscore30,zscore90,trend_type) values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    enrichedDataEmID = enrichedData["embersId"]
    derivedFrom = enrichedData["derivedFrom"]
    ty = enrichedData["type"]
    name = enrichedData["name"] 
    postDate = enrichedData["postDate"] 
    operateTime = enrichedData["operateTime"] 
    currentValue = enrichedData["currentValue"] 
    previousCloseValue = enrichedData["previousCloseValue"]
    oneDayChange = enrichedData["oneDayChange"]
    changePercent = enrichedData["changePercent"]
    zscore30 = enrichedData["zscore30"]
    zscore90 = enrichedData["zscore90"]
    trendType = enrichedData["trendType"]
    
    cur.execute(sql,(enrichedDataEmID,derivedFrom,ty,name,postDate,operateTime,currentValue,previousCloseValue,oneDayChange,changePercent,zscore30,zscore90,trendType))
    
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
    
def export_test_stock_data(estimationStart,estimationEnd):
    
    con = common.getDBConnection()
    cur = con.cursor()
    sql = "select embers_id,type,name,current_value,previous_close_value,update_time,query_time,post_date,source from t_bloomberg_prices where post_date>=? and post_date<=?"
    cur.execute(sql,(estimationStart,estimationEnd,))
    results = cur.fetchall()
    
    for row in results:
        embers_id = row[0]
        ty = row[1]
        name = row[2]
        update_time = row[5]
        last_price = float(row[3])
        pre_last_price = float(row[4])
        one_day_change = round(last_price - pre_last_price,4)
        query_time = row[6]
        source = row[8]
        post_date = row[7]
        
        "Initiate the enriched Data"
        enrichedData = {}
        
        "calculate zscore 30 and zscore 90"
        zscore30 = getZscore(con,post_date,name,one_day_change,30)
        zscore90 = getZscore(con,post_date,name,one_day_change,90)
        
        changePercent = round((last_price - pre_last_price)/pre_last_price,4)
        
        trend_type = get_trend_type(name,changePercent)
        derived_from = "[" + embers_id + "]"
        enrichedData["derivedFrom"] = derived_from
        enrichedData["type"] = ty
        enrichedData["name"] = name
        enrichedData["postDate"] = post_date
        enrichedData["currentValue"] = last_price
        enrichedData["previousCloseValue"] = pre_last_price
        enrichedData["oneDayChange"] = one_day_change
        enrichedData["changePercent"] = round((last_price - pre_last_price)/pre_last_price,4)
        enrichedData["trendType"] = trend_type
        enrichedData["zscore30"] = zscore30
        enrichedData["zscore90"] = zscore90
        enrichedData["operateTime"] = datetime.now().isoformat()
        enrichedDataEmID = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
        enrichedData["embersId"] = enrichedDataEmID
        
        insert_enriched_data(con,enrichedData)
        
    con.commit() 
        
      
def get_trend_type(stockIndex,changePercent):
    """
    Computing current day's trend type, compareing change percent to the trend range,
    Choose the nearnes trend as current day's type    
    """
    "Load the trend type range file"
    rangeFilePath = common.get_configuration("model", "TREND_RANGE_FILE")
    tFile = open(rangeFilePath)
    trendsJson = json.load(tFile)
    tFile.close()
    tJson = trendsJson[stockIndex]
    
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

if __name__=="__main__":
    export_test_stock_data("2012-10-17","2012-11-30") 