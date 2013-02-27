#!/usr/bin/env python 
# -*- coding: utf-8 -*-
from __future__ import division
import sys
import os.path
import argparse
from datetime import datetime,timedelta
from etool import queue,logs
import json
import math
import operator
import hashlib
import calculator
import sqlite3 as lite
import boto
import pytz
import nltk

__processor__ = 'bayesian_model'
log = logs.getLogger(__processor__)
__version__ = "0.0.1"

"""
Applying bayesian model to predict the stock flucturation.
Parameters for bayesian model:
    ap.add_argument('-c',dest="model_cfg",metavar="MODEL CFG",default="./bayesian_model.conf",type=str,nargs='?',help='the config file')
    ap.add_argument('-zw',dest="warning_port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-zs',dest="surrogate_port",metavar="ZMQ PORT",default="tcp://*:30114",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
    ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
    utc_dt = T_UTC.localize(datetime.utcnow())
    eas_dt = utc_dt.astimezone(T_EASTERN)
    default_day = datetime.strftime(eas_dt + timedelta(days =1),"%Y-%m-%d")
    ap.add_argument('-d',dest="predict_date",metavar="PREDICT DATE",type=str,default=default_day,nargs="?",help="The day to be predicted")
    ap.add_argument('-s',dest="stock_list",metavar="Stock List",type=str,nargs="+",help="The list of stock to be predicted")
    ap.add_argument('-rg',dest="rege_date",metavar="Regenerate Date",type=str,help="The date need to be regerated")
Data flow:
    1> retrieve 3 past day's news from sqlite database table: t_daily_enrichednews
    2> retrieve 3 past day's trend type from sqlite database table: t_enriched_bloomberg_prices
    3> predict the stock trend type of the day to be predicted (This is surrogate data, which is sent to ZMQ and stored in sqlite table: t_surrogatedata)
    4> according to the stock predicted trend type, check if it will cause a sigma event (If this is a warning, it will be sent to ZMQ and be stored in table: t_warningmessage )
    
"""
def query_keywords(domain, start_date,end_date, index):

    eids = set()
    terms = {}
    rs = domain.select("SELECT * FROM bloomberg_keywords WHERE date >= '%s' AND date <= '%s' AND stockIndex = '%s'" % (start_date,end_date, index))
    # you can do order by, but the column must appear in the where clause
    for r in rs:
        t = r['term']
        terms[t] = terms.get(t, 0) + int(r['count'])
        eids.add(r['embersId'])

    return (terms, eids)

def check_if_tradingday(conn,predictiveDate,stockIndex):
    "Check if the day weekend"
    weekDay = datetime.strptime(predictiveDate,"%Y-%m-%d").weekday()
    if weekDay == 5 or weekDay == 6:
        log.info("%s For %s is Weekend, Just Skip!" %(predictiveDate,stockIndex))
        return False
    
    "Check if the day is holiday"
    t_domain = conn.get_domain('s_holiday')
    sql = "select count(*) from s_holiday where stockIndex = '{}' and date='{}' ".format(stockIndex,predictiveDate)
    rs = t_domain.select(sql)
    count = 0
    for r in rs:
        count = int(r['Count'])
    if count == 0:
        return True
    else:
        log.info( "%s For %s is Holiday, Just Skip!" %(predictiveDate,stockIndex))
        return False
    
def get_past_cluster_list(conn,predict_date,stock_index,duration=3):

    #get the past n days trend type 
    cluster_types_history = []
    stock_derived = []
    table_name = "t_enriched_bloomberg_prices"
    sql = "select trendType,embersId from {} where postDate < '{}' and name = '{}' order by postDate desc".format(table_name,predict_date,stock_index)
    en_domain = conn.get_domain(table_name)
    results = en_domain.select(sql,max_items=duration)
    for result in results:
        cluster_types_history.append(result['trendType'])
        stock_derived.append(result['embersId'])
    return cluster_types_history, stock_derived

def get_term_list(conn,predict_date,stock_index,duration=1):

    #get the past n day's news
    "Get past 3 day's news before Predictive Day "
    predict_date = datetime.strptime(predict_date, "%Y-%m-%d" )
    start_day = ( predict_date - timedelta( days = duration ) ).strftime( "%Y-%m-%d" )
    end_day = ( predict_date - timedelta( days = 1 ) ).strftime( "%Y-%m-%d" )
    "SimpleDB version of get matched key words"
    key_words_domain = conn.get_domain("bloomberg_keywords")
    termList,news_derived = query_keywords(key_words_domain, start_day,end_day, stock_index)
    return termList,news_derived

# calculate the stock index contribution for the coming day
def compute_stock_index_probability(conn,predict_date, cluster_type , stock_index, cluster_types_history ):
    try:
        "Get the clusters List"
        cluster_probability = CONFIG["clusterProbability"]
        cluster_json = {}
        cluster_contribution_json = {}
        cluster_json = cluster_probability[stock_index]
        "Get the contribution of each cluster"
        cluster_contribution_json = CONFIG["clusterContribution"]
        
        #computing probability   
        stock_probability = 0
        for key in cluster_contribution_json[stock_index].keys():
            if key == str(cluster_type):
                "Search from the Cluster contribution Matrix to get the contribution probability"
                stock_probability = stock_probability + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[0] ) - 1][2] ) ) + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[1] ) - 1][1] ) ) + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[2] ) - 1][0] ) ) + math.log( float( cluster_json[str( cluster_type )] ) )
        return stock_probability
    except Exception as e:
        log.info( "Error in computing stock index probability: %s" % e.args)

# calculate the stock news contribution for the coming day
def compute_stock_news_probability(conn,predict_date, cluster_type , stock_index, termList):
    try:
        term_contribution_json = CONFIG["termContribution"]
        
        term_probability = 0.0
        if stock_index in term_contribution_json:
            for term_cluster_type in term_contribution_json[stock_index].keys():
                if term_cluster_type == str(cluster_type):    
                    stermlist = term_contribution_json[stock_index][term_cluster_type]
                    #print stermlist                            
                    for word, count in termList.iteritems():                    
                        if word in stermlist:                        
                            #print word
                            term_probability =  term_probability + count * math.log( float( term_contribution_json[stock_index][term_cluster_type][word] ) )
        
        return term_probability
    except IOError:
        log.info( "Can't open the file:stock_raw_data.json.")
    except Exception as e:
        log.info( "Error in computing stock news probability: %s" % e.message)    
    return None

def insert_surrogatedata(conn,surrogateData):
    try:
        "If the surrogate data is already in database, do not need to insert"
        checkSql = "select count(*) from t_surrogatedata where embers_id = '{}'".format(surrogateData["embersId"])
        conn.create_domain("t_surrogatedata")
        t_domain = conn.get_domain("t_surrogatedata")
        rs = t_domain.select(checkSql)
        count = 0
        for r in rs:
            count = int(r['Count'])
        if count == 0:
            "Remove the derivedFrom attribute in message "
            del(surrogateData["derivedFrom"])
            t_domain.put_attributes(surrogateData["embersId"], surrogateData)
    except Exception as e:
        log.info( "Error: %s" %e.args[0])
    finally:
        pass
                
#predict the stock change type
def process_single_stock(conn,predict_date,stock_index,regeFlag=False):
    try:
        "Check if the predictive Day is trading day, if so continue, otherwise just return None"
        if_trading_day = check_if_tradingday(conn,predict_date,stock_index) 
        if if_trading_day is False:
            return None
        
        predictiveResults = {}
        finalRatio = {}
        clusterProbability = {}
        predictiveProbability = 0
        stockDerived = []
        newsDerived = []
        
        "Iteratively compute the probabilty of each cluster for the stock "
        cluster_pro_list = CONFIG["clusterProbability"][stock_index]
        
        term_list,newsDerived = get_term_list(conn, predict_date, stock_index)
        his_cluster_list,stockDerived = get_past_cluster_list(conn,predict_date,stock_index)
        
        for cluster_type in cluster_pro_list:
            "compute the contribution of 3 past day's trend "
            stockIndexProbability = compute_stock_index_probability(conn,predict_date, cluster_type , stock_index, his_cluster_list )
            "compute the contribution of 3 past day's news"
            newsProbability = compute_stock_news_probability(conn,predict_date, cluster_type , stock_index,term_list )
            "combine two contribution together"
            predictiveProbability = math.exp( stockIndexProbability + newsProbability )
            predictiveResults[cluster_type] = predictiveProbability
        
        sumProbability = sum( predictiveResults.itervalues() ) 
        
        "Get the maximum probability between the predictive values"
        for item_key, item_value in predictiveResults.iteritems():
            finalRatio[item_key] = item_value / sumProbability
        sorted_ratio = sorted( finalRatio.iteritems(), key = operator.itemgetter( 1 ), reverse = True )
        clusterProbability[stock_index] = {}
        clusterProbability[stock_index][predict_date] = sorted_ratio[0]
        
        "Construct the Surrogate data"
        surrogateData = {}
        "Merge News Derived and Stock Derived"
        derivedFrom = {"derivedIds":[]}
        for item in stockDerived:
            derivedFrom["derivedIds"].append(item)
        for item in newsDerived:
            derivedFrom["derivedIds"].append(item)
        "construct surrogate data"    
        model = 'Bayesian - Time serial Model'
        location = CONFIG["location"][stock_index]
        population = stock_index
        confidence = round(sorted_ratio[0][1],2)
        confidenceIsProbability = True
        shiftType = "Trend"
        valueSpectrum = "changePercent"
        strength = sorted_ratio[0][0]
        shiftDate = predict_date
        
        surrogateData["derivedFrom"] = derivedFrom
        surrogateData["model"] = model
        surrogateData["location"] = location
        surrogateData["population"] = population
        surrogateData["confidence"] = confidence
        surrogateData["confidenceIsProbability"] = confidenceIsProbability
        surrogateData["shiftType"] = shiftType
        surrogateData["valueSpectrum"] = valueSpectrum
        surrogateData["strength"] = strength
        surrogateData["shiftDate"] = shiftDate
        surrogateData["version"] = __version__
        comments = {}
        comments["configVersion"] = CONFIG["version"]
        surrogateData["comments"] = json.dumps(comments)
        surrogateData["description"] = "Predict the change type of the future day"
        "Transer From UTC to eastern Time"
        utc_dt = T_UTC.localize(datetime.utcnow())
        eas_dt = utc_dt.astimezone(T_EASTERN)
        surrogateData["dateProduced"] = eas_dt.isoformat()
        
        "Generate Embers Id"
        jsonStr = json.dumps(surrogateData)
        embersId = hashlib.sha1(json.dumps(jsonStr)).hexdigest()
        surrogateData["embersId"] = embersId
        
        "if the action is not for regenerating past warning, then store the surrogate and warning"
        if not regeFlag:
            #push surrodate data into ZMQ
            with queue.open(SURROGATE_PORT, 'w', capture=False) as outq:
                outq.write(surrogateData)
            
            "Insert the surrogatedata to Simple DB: "
            insert_surrogatedata(conn, surrogateData)
        
        return surrogateData
    except Exception as e:
        log.exception( "process_single_stock Error: %s" % e.message)
        return None

def dailySigmaTrends(stockIndex,cluster,m30,m90,std30,std90,curValue):
    #computing the bottom and upper line for daily sigma event
    "get warning threshold"
    warning_threshold = CONFIG["warning_threshold"]
    s4Bottom = m30 - warning_threshold[0]*std30
    s4Upper = m30 + warning_threshold[0]*std30
    s3Bottom = m90 - warning_threshold[1]*std90
    s3Upper = m90 + warning_threshold[1]*std90
    
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
    
    "Load the latest version of cluster range object, the newest version has the maximum version num"
    clusterDis = CONFIG["trendRange"]["range"]
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


def insert_warningmessage(conn,warningMessage):
    try:
        "If the warning is already in database, do not need to insert"
        checkSql = "select count(*) from t_warningmessage where embers_id = '{}'".format(warningMessage["embersId"])
        count = 0
        conn.create_domain("t_warningmessage")
        t_domain = conn.get_domain("t_warningmessage")
        rs = t_domain.select(checkSql)
        for r in rs:
            count = int(r['Count'])
        if count == 0:
            "Remove the derivedFrom attribute in warningmessage"
            del(warningMessage["derivedFrom"])
            t_domain.put_attributes(warningMessage["embersId"], warningMessage)
    except Exception as e:
        log.exception( "insert_warningmessage Error: %s" % e.args[0])
    finally:
        pass

# using surrogate data to determine whether it triger a sigma event 
def warning_check(conn,surObj,regeFlag=False):
#   surObj = {'embersId': 'f0c030a20e28a12134d9ad0e98fd0861fae7438b', 'confidence': 0.13429584033181682, 'strength': '4', 'derivedFrom': [u'5df18f77723885a12fa6943421c819c90c6a2a02', u'be031c4dcf3eb9bba2d86870683897dfc4ec4051', u'3c6571a4d89b17ed01f1345c80cf2802a8a02b7b'], 'shiftDate': '2011-08-08', 'shiftType': 'Trend', 'location': u'Colombia', 'date': '2012-10-03', 'model': 'Finance Stock Model', 'valueSpectrum': 'changePercent', 'confidenceIsProbability': True, 'population': 'COLCAP'}
    stock_index = surObj["population"]
    trend_type = surObj["strength"]
    date = surObj["shiftDate"]
    
    try:
        pClusster = trend_type
        table_name = "t_enriched_bloomberg_prices"  
        t_domain = conn.get_domain(table_name)
        
        sql = "select currentValue from {} where name='{}' and postDate < '{}' order by postDate desc".format(table_name,stock_index,date)
        current_val = 0.0
        rs = t_domain.select(sql,max_items=1)
        for r in rs:
            current_val = float(r['currentValue'])
        
        querySql = "select oneDayChange from {} where name='{}' and postDate <'{}' order by postDate desc".format(table_name,stock_index,date)
        rs = t_domain.select(querySql,max_items=30)
        moving30 = []
        for r in rs:
            moving30.append(float(r['oneDayChange']))
            
        querySql = "select oneDayChange from {} where name='{}' and postDate <'{}' order by postDate desc".format(table_name,stock_index,date)
        rs = t_domain.select(querySql,max_items=90)
        moving90 = []
        for r in rs:
            moving90.append(float(r['oneDayChange']))
        
        m30 = sum(moving30)/len(moving30)
        m90 = sum(moving90)/len(moving90)
        std30 = calculator.calSD(moving30)
        std90 = calculator.calSD(moving90)
        
        eventType,cButtom,cUpper = dailySigmaTrends(stock_index,str(pClusster),m30,m90,std30,std90,current_val)

        dailyRecord = {}
        dailyRecord["date"] = date
        dailyRecord["cBottom"] = cButtom
        dailyRecord["cUpper"] = cUpper
        dailyRecord["currentValue"] = current_val
        
        "Construct the warning message"
        warningMessage ={}
        derivedFrom = {"derivedIds":[surObj["embersId"]]}
        model = surObj["model"]
        event = eventType
        confidence = surObj["confidence"]
        confidenceIsProbability = surObj["confidenceIsProbability"]
        eventDate= surObj["shiftDate"]
        population = surObj["population"]
        location = surObj["location"]
        comments = surObj["comments"]
        comObj = json.loads(comments)
        
        warningMessage["derivedFrom"] = derivedFrom
        warningMessage["model"] = model
        warningMessage["eventType"] = event
        warningMessage["confidence"] = confidence
        warningMessage["confidenceIsProbability"] = confidenceIsProbability
        warningMessage["eventDate"] = eventDate
        warningMessage["population"] = population
        warningMessage["location"] = location
        warningMessage["version"] = __version__
        operateTime = datetime.utcnow().isoformat()
        warningMessage["dateProduced"] = operateTime
        comObj["trendVersion"] = CONFIG["trendRange"]["version"]
        warningMessage["comments"] = json.dumps(comObj)
        warningMessage["description"] = "Use Bayesian to predict stock sigma events"
        
        embersId = hashlib.sha1(json.dumps(warningMessage)).hexdigest()
        warningMessage["embersId"] = embersId
        
        if not regeFlag:
            if eventType != "0000":
                "Push warningmessage to ZMQ"
                with queue.open(WARNING_PORT, 'w', capture=False) as outq:
                        outq.write(warningMessage)
                
            insert_warningmessage(conn,warningMessage)
        
        if eventType != "0000":
            return warningMessage
        else:
            return None
        
    except lite.Error, e:
        log.exception( "Error: %s" % e.args[0])
    finally:
        pass    

def get_predicion_version(conn,rege_date):
    t_domain = conn.get_domain('t_warningmessage')
    sql = "select comments from t_warningmessage where eventDate = '{}'".format(rege_date)
    rs = t_domain.select(sql,max_items=1)
    versionStr = {}
    for r in rs:
        versionStr = r['comments']
    return json.loads(versionStr)

def re_training(conn,predict_date,stock_list):
    clu_num = len(CONFIG["clusterProbability"]["MEXBOL"])
    t_domain = conn.get_domain("t_enriched_bloomberg_prices")
    
    stockGroupTrend = {}
    for index in stock_list:
        sql = "select name,postDate,trendType from t_enriched_bloomberg_prices where name = '{}' and postDate >='2003-01-01' and postDate < '{}' order by postDate asc ".format(index,predict_date)
        if index not in stockGroupTrend:
            stockGroupTrend[index] = []
        rs = t_domain.select(sql)
        for r in rs:
            stockGroupTrend[index].append(int(r["trendType"]))
    
    finalClusterMatrix = {}
    finalClusterProbability = {}
    for item in stockGroupTrend:
        #read all the line and skip the first line
        trendsSerial = stockGroupTrend[item]
        clusterDist = nltk.FreqDist(trendsSerial)
        clusterProbability = {}
        for cl in clusterDist:
            clusterProbability[str(cl)] = "%0.4f" %(clusterDist[cl]/sum(clusterDist.values()))
        finalClusterProbability[item] = clusterProbability
        
        #Define the ultimated json object
        clusterMatrix = {}
        for cluster in range(1,clu_num+1):
            #create matrix for each cluster
            matrix = [[0 for col in range(3)] for row in range(clu_num)]
            for i in range(0,len(trendsSerial)):
                if cluster == trendsSerial[i]:
                    t1 = 0
                    t2 = 0
                    t3 = 0
                    if i - 1 >= 0:
                        t1 = trendsSerial[i-1]
                        matrix[t1-1][0] = matrix[t1-1][0] + 1
                    if i - 2 >= 0:
                        t2 = trendsSerial[i-2]
                        matrix[t2-1][1] = matrix[t2-1][1] + 1
                    if i - 3 >= 0:
                        t3 = trendsSerial[i-3]
                        matrix[t3-1][2] = matrix[t3-1][2] + 1
            #calculating the contribution matrix
            contributionMatrix = [[0 for col in range(3)] for row in range(clu_num)]
            sumCol = [0,0,0]
            for col in range(3):
                for row in range(clu_num):
                    sumCol[col] = sumCol[col] + matrix[row][col]
            
            for col in range(3):
                for row in range(clu_num):
                    contributionMatrix[row][col] = "%0.4f" %((matrix[row][col] + 1)/(sumCol[col]+clu_num))
            clusterMatrix[str(cluster)] = contributionMatrix
            finalClusterMatrix[item] = clusterMatrix
    
    print finalClusterProbability,finalClusterMatrix
    return finalClusterProbability,finalClusterMatrix
     
def parse_args():
    ap = argparse.ArgumentParser("Apply the bayesian model to predict stock warning")
    ap.add_argument('-c',dest="model_cfg",metavar="MODEL CFG",
                    default=os.path.join(os.path.dirname(__file__), "bayesian_model.conf"),
                    type=str,nargs='?',help='the config file')
    ap.add_argument('-zw',dest="warning_port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-zs',dest="surrogate_port",metavar="ZMQ PORT",default="tcp://*:30114",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
    ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
    utc_dt = T_UTC.localize(datetime.utcnow())
    eas_dt = utc_dt.astimezone(T_EASTERN)
    default_day = datetime.strftime(eas_dt + timedelta(days =1),"%Y-%m-%d")
    ap.add_argument('-d',dest="predict_date",metavar="PREDICT DATE",type=str,default=default_day,nargs="?",help="The day to be predicted")
    ap.add_argument('-s',dest="stock_list",metavar="Stock List",type=str,nargs="+",help="The list of stock to be predicted")
    ap.add_argument('-rg',dest="rege_date",metavar="Regenerate Date",type=str,help="The date need to be regerated")
    ap.add_argument('--verbose', action="store_true",
                    help="Write debug messages.")
    return ap.parse_args()

def main():
    global CONFIG,VOCABULARY_FILE,WARNING_PORT,SURROGATE_PORT,__version__,KEY_ID,SECRET,T_EASTERN,T_UTC
    
    "Initiate the TimeZone Setting"
    T_UTC = pytz.utc
    T_EASTERN = pytz.timezone("US/Eastern")
    
    "Get the input args"
    args = parse_args()
    rege_date = args.rege_date
    KEY_ID = args.key_id
    SECRET = args.secret
    logs.init(args)
    queue.init(args)
    #replace dbconnection to simple DB    
#   conn = lite.connect(db_file)
    conn = boto.connect_sdb(KEY_ID,SECRET)
    "if rege_date is not none, it means to regenerate the past day's prediction"
    if not rege_date:
        "Normal predict"
        predict_date = args.predict_date
        model_cfg = args.model_cfg
        WARNING_PORT = args.warning_port
        SURROGATE_PORT = args.surrogate_port
        
        stock_list = None
        if args.stock_list:
            stock_list = args.stock_list
        

        "Get the Latest version of Config Object"
        f = open(model_cfg,"r")
        configObj = json.load(f)
        f.close()
        con_versionNum = max([int(v) for v in configObj.keys()])
        CONFIG = configObj[str(con_versionNum)]
        
        
        "Get the Latest version of Trend Range Object"
        clusterTrends = json.load(sys.stdin)
        trend_versionNum = max([int(v) for v in clusterTrends.keys()])
        CONFIG["trendRange"] = {"version":str(trend_versionNum),"range":clusterTrends[str(trend_versionNum)]}
        
        if not stock_list:
            stock_list = CONFIG["stocks"]
            
        "Retrain the model configuration if current day is Saturday"
        weekDay = datetime.strptime(predict_date,"%Y-%m-%d").weekday()
        if weekDay == 5:
            finalClusterProbability,finalClusterMatrix = re_training(conn,predict_date,stock_list)
            new_config = json.loads(json.dumps(CONFIG))
            new_config["clusterProbability"] = finalClusterProbability
            new_config["clusterContribution"] = finalClusterMatrix
            "Write back to configure file"
            new_version_num = con_versionNum + 1
            new_config["version"] = new_version_num
            configObj[str(new_version_num)] = new_config
            with open(model_cfg,"w") as out_q:
                out_q.write(json.dumps(configObj))
        
        "Process stock each by each"
        for stock in stock_list:
            surrogate = process_single_stock(conn,predict_date,stock)
            if surrogate:
                warning = warning_check(conn,surrogate)
        
    else:
        "regenerate the old prediction"
        model_cfg = args.model_cfg
        stock_list = None
        if args.stock_list:
            stock_list = args.stock_list
            
        "Get the version of Config Object for the indicated prediction"
        versionObj = get_predicion_version(conn,rege_date)
        configVersionNum = versionObj["configVersion"]
        trendVersionNum = versionObj["trendVersion"]
        
        configObj = json.load(open(model_cfg))
        CONFIG = configObj[configVersionNum]
        
        "Get the Latest version of Trend Range Object"
        clusterTrends = json.load(sys.stdin)
        CONFIG["trendRange"] = {"version":str(trendVersionNum),"range":clusterTrends[trendVersionNum]}
        
        if not stock_list:
            stock_list = CONFIG["stocks"]
        
        "Process stock each by each"
        for stock in stock_list:
            surrogate = process_single_stock(conn,rege_date,stock,True)
            if surrogate:
                warning = warning_check(conn,surrogate,True)
                return warning
        
    if conn:
        conn.close()
        
CONFIG = {}
WARNING_PORT = ""
SURROGATE_PORT = ""
KEY_ID = ""
SECRET = ""

if __name__ == "__main__":
    main()
        
