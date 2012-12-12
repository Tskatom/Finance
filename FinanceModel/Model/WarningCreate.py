import sqlite3 as lite
import sys
import json
from Util import calculator
import time
from datetime import datetime,timedelta
import hashlib
from Util import common
import EnrichDataProcess as ed
from etool import queue,logs

outputResult = {}
__processor__ = 'WarningCreate'
log = logs.getLogger(__processor__)

def init(cfgPath):
    common.init(cfgPath)
    logs.init()
    
def dailySigmaTrends(stockIndex,cluster,m30,m90,std30,std90,curValue):
    #computing the bottom and upper line for daily sigma event
    s4Bottom = m30 - 4*std30
    s4Upper = m30 + 4*std30
    s3Bottom = m90 - 3*std90
    s3Upper = m90 + 3*std90
    
    bottom = s4Bottom
    upper = s4Upper
    if s4Bottom >= s3Bottom:
        bottom = s3Bottom
    if s3Upper <= s4Upper:
        upper = s3Upper
    #Get the span of input cluster
    """
    One point needed to be changed later: currently we just merge the two type of 
    extreme into one trend type 7, and we need to divide type 7 into type 7 and 11
    """
    trendRangePath = common.get_configuration("model", "TREND_RANGE_FILE")
    clusterDis = json.load(open(trendRangePath))
    #get the span of the input trend type
    cBottom = 0.0
    cUpper = 0.0
    
    clusters = clusterDis[stockIndex]
    for clu in clusters:
        if clu == cluster:
            cBottom = clusters[clu][0] * curValue
            cUpper = clusters[clu][1] * curValue
    
    #If Nothing happen, the eventType will be 0000
    eventType = "0000"
    
    if cBottom <= bottom:
        eventType = "0412"
    if cUpper >= upper:
        eventType = "0411"
    
    #If the predictive trends is the extreme value(Type == 1 and 6)
    #If previous day is not extreme sigma day, then predict that the next day will be extreme day
#    if eventType != "0000":
#        print "eventType:%s cBottom: %0.4f, bottom:%0.4f, cUpper:%0.4f, upper:%0.4f" %(eventType,cBottom,bottom,cUpper,upper)
    return eventType,cBottom,cUpper

def warningCheck(surObj):
#    surObj = {'embersId': 'f0c030a20e28a12134d9ad0e98fd0861fae7438b', 'confidence': 0.13429584033181682, 'strength': '4', 'derivedFrom': [u'5df18f77723885a12fa6943421c819c90c6a2a02', u'be031c4dcf3eb9bba2d86870683897dfc4ec4051', u'3c6571a4d89b17ed01f1345c80cf2802a8a02b7b'], 'shiftDate': '2011-08-08', 'shiftType': 'Trend', 'location': u'Colombia', 'date': '2012-10-03', 'model': 'Finance Stock Model', 'valueSpectrum': 'changePercent', 'confidenceIsProbability': True, 'population': 'COLCAP'}
    stockIndex = surObj["population"]
    trendType = surObj["strength"]
    date = surObj["shiftDate"]
    
    try:
        con = common.getDBConnection()
        cur = con.cursor()
        pClusster = trendType
            
    
        sql = "select sub_sequence,last_price from t_daily_stockindex where stock_index=? and date<? order by date desc limit 1"
        cur.execute(sql,(stockIndex,date))
        row = cur.fetchone()
        subSequence = row[0]
        currentVal = row[1]
        
        querySql = "select one_day_change from t_daily_stockindex where stock_index=? and sub_sequence>=? and sub_sequence<=?"
        cur.execute(querySql,(stockIndex,subSequence-29,subSequence))
        rows = cur.fetchall()
        moving30 = []
        for row in rows:
            moving30.append(row[0])
        
        querySql = "select one_day_change from t_daily_stockindex where stock_index=? and sub_sequence>=? and sub_sequence<=?"
        cur.execute(querySql,(stockIndex,subSequence-89,subSequence))
        rows = cur.fetchall()
        moving90 = []
        for row in rows:
            moving90.append(row[0])
        
        m30 = sum(moving30)/len(moving30)
        m90 = sum(moving90)/len(moving90)
        std30 = calculator.calSD(moving30)
        std90 = calculator.calSD(moving90)
        
        eventType,cButtom,cUpper = dailySigmaTrends(stockIndex,str(pClusster),m30,m90,std30,std90,currentVal)

        dailyRecord = {}
        dailyRecord["date"] = date
        dailyRecord["cBottom"] = cButtom
        dailyRecord["cUpper"] = cUpper
        dailyRecord["currentValue"] = currentVal
        
        "Construct the warning message"
        warningMessage ={}
        date = surObj["date"]
        derivedFrom = surObj["embersId"]
        model = surObj["model"]
        event = eventType
        confidence = surObj["confidence"]
        confidenceIsProbability = surObj["confidenceIsProbability"]
        eventDate= surObj["shiftDate"]
        population = surObj["population"]
        location = surObj["location"]
        
        warningMessage["date"] = date
        warningMessage["derivedFrom"] = derivedFrom
        warningMessage["model"] = model
        warningMessage["eventType"] = event
        warningMessage["confidence"] = confidence
        warningMessage["confidenceIsProbability"] = confidenceIsProbability
        warningMessage["eventDate"] = eventDate
        warningMessage["population"] = population
        warningMessage["location"] = location
        
        embersId = hashlib.sha1(json.dumps(warningMessage)).hexdigest()
        warningMessage["embersId"] = embersId
        
        if eventType != "0000":
            insert_warningmessage(warningMessage)
            return warningMessage
        else:
            return None
        
    except lite.Error, e:
        log.exception( "Error: %s" % e.args[0])
    finally:
        if con:
            con.close()   

def insert_warningmessage(warningMessage):
    try:
        con = common.getDBConnection()
        cur = con.cursor()
        
        "If the warning is already in database, do not need to insert"
        checkSql = "select count(*) from t_warningmessage where embers_id = ?"
        embersId = warningMessage["embersId"]
        cur.execute(checkSql,(embersId,)) 
        count = cur.fetchone()[0]

        if count == 0:
            insertSql = "insert into t_warningmessage (embers_id,derived_from,model,event_type,confidence,confidence_isprobability,\
            event_date,location,population) values (?,?,?,?,?,?,?,?,?)"
            
            derivedFrom = json.dumps(warningMessage["derivedFrom"])
            model = warningMessage["model"]
            eventType =  warningMessage["eventType"]
            confidence = warningMessage["confidence"]
            confidenceIsProbability = warningMessage["confidenceIsProbability"] 
            eventDate = warningMessage["eventDate"]
            population = warningMessage["population"] 
            location = warningMessage["location"]
            
            cur.execute(insertSql,(embersId,derivedFrom,model,eventType,confidence,confidenceIsProbability,eventDate,population,location))
            con.commit()
    except Exception as e:
        log.exception( "Error: %s" % e.args[0])
    finally:
        if con:
            con.close()   
                 
def execute(date,cfgPath):
    init(cfgPath)
    enricheDa = ed.Enriched_Data(cfgPath)
    obj = enricheDa.enrich_all_stock(date)
    warningList = []
    for item in obj:
        warning = warningCheck(item)
        if warning is not None:
            warningList.append(warning) 
    
    #push warning to ZMQ
    port = common.get_configuration("info", "ZMQ_PORT")
    with queue.open(port, 'w', capture=True) as outq:
        for warning in warningList:
            outq.write(json.dumps(warning, encoding='utf8'))    
                
    return warningList   

if __name__ == "__main__":
    if len(sys.argv)==4:
        startDay = sys.argv[1]
        endDay = sys.argv[2]
        cfgPath = sys.argv[3]
        
        startD = datetime.strptime(startDay,"%Y-%m-%d")
        endD = datetime.strptime(endDay,"%Y-%m-%d")
        resultFile = common.get_configuration("training", "TESTING_RESULT_FILE")
        warningResult = open(resultFile,"w")
        while startD <= endD:
            predictiveDay = datetime.strftime(startD,"%Y-%m-%d")
            warningList = execute(predictiveDay,cfgPath)
            if warningList is not None:
                for warning in warningList:
                    warningResult.write(json.dumps(warning))
                    warningResult.write("\n")
            startD = startD + timedelta(days=1)
        warningResult.close()
    elif len(sys.argv) == 2:
        "The imput date format should be 'yyyy-mm-dd'"
        predictiveDay = sys.argv[1]
        warningList = execute(predictiveDay)
        print warningList
    elif len(sys.argv) == 1:
        predictiveDay = time.strftime('%Y-%m-%d',time.localtime(time.time()+24*60*60))
        warningList = execute(predictiveDay)
        print warningList
    