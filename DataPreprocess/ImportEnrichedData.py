#********************************************************************************************************************
"This is to the pre-process stage for enriching the data. In this file, the main function is mentioned below:"
"1.It will calulate the ZScore values"
"2.Import the data into database"
#********************************************************************************************************************
#-*- coding:utf8-*-
from datetime import datetime
import json
import hashlib
from Util import common,calculator
import cProfile

def InitiateEnrichedData(con, committedInterval, data):
    "Initiate the enriched Data"
    enrichedData = {}
    post_date = data[1]
    name = data[6]
    one_day_change = data[4]
    embers_id = data[0]
    last_price = data[2]
    pre_last_price = data[3]
    change_percent = data[5]
    "calculate zscore 30 and zscore 90"
    zscore30 = getZscore(con,post_date,name,one_day_change,30)
    zscore90 = getZscore(con,post_date,name,one_day_change,90)
    
    derived_from = "[" + embers_id + "]"
    enrichedData["derivedFrom"] = derived_from
    enrichedData["type"] = "stock"
    enrichedData["name"] = name
    enrichedData["postDate"] = post_date
    enrichedData["currentValue"] = last_price
    enrichedData["previousCloseValue"] = pre_last_price
    enrichedData["oneDayChange"] = one_day_change
    enrichedData["changePercent"] = change_percent
    enrichedData["zscore30"] = zscore30
    enrichedData["zscore90"] = zscore90
    enrichedData["operateTime"] = datetime.now().isoformat()
    enrichedDataEmID = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
    enrichedData["embersId"] = enrichedDataEmID
    
    insert_enriched_data(con,enrichedData)
    committedInterval = committedInterval + 1
    if committedInterval % 1000 == 0:
        con.commit()

def clusterSet(traingingStart,traningEndDate): 
    con = common.getDBConnection()
    cur = con.cursor()
    
    finalClusterRecord = []
    stockList = ["MERVAL","MEXBOL","CHILE65","BVPSBVPS","COLCAP","CRSMBCT","IBOV","IGBVL","IBVC"]
    for stock in stockList:
        sql = "select embers_id,post_date,current_value,previous_close_value,round(current_value-previous_close_value,4),round((current_value-previous_close_value)/previous_close_value,4),name from t_bloomberg_prices where name=? and post_date<=? and post_date>=? order by post_date asc"
        cur.execute(sql,(stock,traningEndDate,traingingStart))
        rows = cur.fetchall()
        "The number of rows to be committed for each interval"
        committedInterval = 0
        
        for row in rows:
            newRow = list(row)
            "Insert the pre-enriched stock index data into Database"
            InitiateEnrichedData(con, committedInterval, newRow)
            finalClusterRecord.append(newRow)
        con.commit()
    "Write the training data into file"
    trendSetRecordFile = common.get_configuration("training", "TRAINING_TREND_RECORDS")
    dataStr = json.dumps(finalClusterRecord)
    with open(trendSetRecordFile,"w") as output:
        output.write(dataStr)
    
    if con:
        con.close()

def insert_enriched_data(conn,enrichedData):
    cur = conn.cursor()
    sql = "insert into t_enriched_bloomberg_prices (embers_id,derived_from,type,name,post_date,operate_time,current_value,previous_close_value,one_day_change,change_percent,zscore30,zscore90) values (?,?,?,?,?,?,?,?,?,?,?,?)"
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
    cur.execute(sql,(enrichedDataEmID,derivedFrom,ty,name,postDate,operateTime,currentValue,previousCloseValue,oneDayChange,changePercent,zscore30,zscore90))

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

def main():
    clusterSet("2002-01-01","2010-10-31")
     
if __name__ == "__main__":
#    if len(sys.argv) != 2:
#            print len(sys.argv) ,"Please Input Traing End Time value as following format: yyyy-mm-dd \n"
#            exit(0)
#    trainingEndDate = sys.argv[1]
#    print trainingEndDate
    cProfile.run("main()")