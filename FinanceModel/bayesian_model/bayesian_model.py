import argparse
from datetime import datetime,timedelta
from etool import queue,logs
import json
import math
import operator
import hashlib
import calculator
import sqlite3 as lite

__processor__ = 'bayesian_model'
log = logs.getLogger(__processor__)
logs.init()
__version__ = "0.0.1"

"""
Applying bayesian model to predict the stock flucturation.
Parameters for bayesian model:
    ap.add_argument('-c',dest="model_cfg",metavar="MODEL CFG",default="./data/bayesian_model.cfg",type=str,nargs='?',help='the config file')
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The sqlite database file')
    ap.add_argument('-d',dest="predict_date",metavar="PREDICT DATE",type=str,default=default_day,nargs="?",help="The day to be predicted")
    ap.add_argument('-s',dest="stock_list",metavar="Stock List",type=str,nargs="+",help="The list of stock to be predicted")
    ap.add_argument('-rg',dest="rege_date",metavar="Regenerate Date",type=str,help="The date need to be regerated")
Data flow:
    1> retrieve 3 past day's news from sqlite database table: t_daily_enrichednews
    2> retrieve 3 past day's trend type from sqlite database table: t_enriched_bloomberg_prices
    3> predict the stock trend type of the day to be predicted (This is surrogate data, which is sent to ZMQ and stored in sqlite table: t_surrogatedata)
    4> according to the stock predicted trend type, check if it will cause a sigma event (If this is a warning, it will be sent to ZMQ and be stored in table: t_warningmessage )
    
"""

def check_if_tradingday(conn,predictiveDate,stockIndex):
    "Check if the day weekend"
    weekDay = datetime.strptime(predictiveDate,"%Y-%m-%d").weekday()
    if weekDay == 5 or weekDay == 6:
        log.info("%s For %s is Weekend, Just Skip!" %(predictiveDate,stockIndex))
        return False
    
    "Check if the day is holiday"
    cur = conn.cursor()
    sql = "select count(*) from s_holiday a,s_stock_country b where a.country = b.country\
    and b.stock_index=? and a.date = ?"
    cur.execute(sql,(stockIndex,predictiveDate))
    count = cur.fetchone()[0]
    if count == 0:
        return True
    else:
        log.info( "%s For %s is Holiday, Just Skip!" %(predictiveDate,stockIndex))
        return False

# calculate the stock index contribution for the coming day
def compute_stock_index_probability(conn,predict_date, cluster_type , stock_index, duration=3 ):
    try:
        "Get the clusters List"
        cluster_probability = CONFIG["clusterProbability"]
        cluster_json = {}
        cluster_contribution_json = {}
        cluster_json = cluster_probability[stock_index]
        "Get the contribution of each cluster"
        cluster_contribution_json = CONFIG["clusterContribution"]
        
        #get the past n days trend type 
        cluster_types_history = []
        stock_derived = []
        cur = conn.cursor()
        table_name = "t_enriched_bloomberg_prices"
        sql = "select trend_type,embers_id from {} where post_date < '{}' and name = '{}' order by post_date desc limit {}".format(table_name,predict_date,stock_index,duration)
        cur.execute(sql)
        results = cur.fetchall()
        for result in results:
            cluster_types_history.append(result[0])
            stock_derived.append(result[1])
         
        #computing probability   
        stock_probability = 0
        for key in cluster_contribution_json[stock_index].keys():
            if key == str(cluster_type):
                "Search from the Cluster contribution Matrix to get the contribution probability"
                stock_probability = stock_probability + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[0] ) - 1][2] ) ) + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[1] ) - 1][1] ) ) + math.log( float( cluster_contribution_json[stock_index][key][int( cluster_types_history[2] ) - 1][0] ) ) + math.log( float( cluster_json[str( cluster_type )] ) )
        
        return stock_probability,stock_derived
    except Exception as e:
        log.info( "Error in computing stock index probability: %s" % e.args)

# calculate the stock news contribution for the coming day
def compute_stock_news_probability(conn,predict_date, cluster_type , stock_index,duraiton=3 ):
    try:
        term_contribution_json = CONFIG["termContribution"]
        #get the past n day's news
        "Get past 3 day's news before Predictive Day "
        predict_date = datetime.strptime(predict_date, "%Y-%m-%d" )
        start_day = ( predict_date - timedelta( days = 3 ) ).strftime( "%Y-%m-%d" )
        end_day = ( predict_date - timedelta( days = 1 ) ).strftime( "%Y-%m-%d" )
        table_name = "t_daily_enrichednews"
        sqlquery = "select content,embers_id from {} where post_date>='{}' and post_date<='{}' and stock_index='{}'".format(table_name,start_day,end_day,stock_index)
        cur = conn.cursor()
        cur.execute(sqlquery)
        results = cur.fetchall()
        
        "Initiate the words List"
        termList = {}
        for term in CONFIG["kyewordList"]:
            termList[term] = 0
            
        news_derived = []
        "Merge all the term in each record"
        for record in results:
            jsonRecord = record[0]
            news_derived.append(record[1])
            for curWord in jsonRecord:
                if curWord in termList:
                    termList[curWord] = termList[curWord] + jsonRecord[curWord]
        
        term_probability = 0
        if stock_index in term_contribution_json:
            for term_cluster_type in term_contribution_json[stock_index].keys():
                if term_cluster_type == str(cluster_type):    
                    stermlist = term_contribution_json[stock_index][term_cluster_type]
                    #print stermlist                            
                    for word, count in termList.iteritems():                    
                        if word in stermlist:                        
                            #print word
                            term_probability =  count * math.log( float( term_contribution_json[stock_index][term_cluster_type][word] ) )
        
        return term_probability,news_derived
    except IOError:
        log.info( "Can't open the file:stock_raw_data.json.")
    except Exception as e:
        log.info( "Error in computing stock news probability: %s" % e.message)    
    return None

def insert_surrogatedata(conn,surrogateData):
    try:
        cur = conn.cursor()
        
        "If the surrogate data is already in database, do not need to insert"
        checkSql = "select count(*) from t_surrogatedata where embers_id = ?"
        embersId = surrogateData["embersId"]
        cur.execute(checkSql,(embersId,)) 
        count = cur.fetchone()[0]
        
        if count == 0:
            insertSql = "insert into t_surrogatedata (embers_id,derived_from,shift_date,shift_type,confidence,\
            strength,location,model,value_spectrum,confidence_isprobability,population,version,comments,description,operate_time) values \
            (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            
            embersId = surrogateData["embersId"]
            derivedFrom = json.dumps(surrogateData["derivedFrom"])
            shiftDate = surrogateData["shiftDate"]
            shiftType = surrogateData["shiftType"]
            confidence = surrogateData["confidence"]
            strength = surrogateData["strength"]
            location = surrogateData["location"]
            model = surrogateData["model"]
            valueSpectrum = surrogateData["valueSpectrum"]
            confidenceIsPrabability = surrogateData["confidenceIsProbability"]
            population = surrogateData["population"]
            comments = surrogateData["comments"]
            description = surrogateData["description"]
            operateTime = surrogateData["dateProduced"]
            version = surrogateData["version"]
            
            cur.execute(insertSql,(embersId,derivedFrom,shiftDate,shiftType,confidence,strength,location,model,valueSpectrum,confidenceIsPrabability,population,
                                   version,comments,description,operateTime))
            conn.commit()
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
        for cluster_type in cluster_pro_list:
            "compute the contribution of 3 past day's trend "
            stockIndexProbability,stockDerived = compute_stock_index_probability(conn,predict_date, cluster_type , stock_index )
            "compute the contribution of 3 past day's news"
            newsProbability,newsDerived = compute_stock_news_probability(conn,predict_date, cluster_type , stock_index )
            "combine two contribution together"
            predictiveProbability = math.exp( stockIndexProbability + newsProbability ) * float( 1e90 )
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
        derivedFrom = []
        for item in stockDerived:
            derivedFrom.append(item)
        for item in newsDerived:
            derivedFrom.append(item)
        "construct surrogate data"    
        model = 'Bayesian - Time serial Model'
        location = CONFIG["location"][stock_index]
        population = stock_index
        confidence = sorted_ratio[0][1]
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
        surrogateData["dateProduced"] = datetime.now().isoformat()
        
        "Generate Embers Id"
        jsonStr = json.dumps(surrogateData)
        embersId = hashlib.sha1(json.dumps(jsonStr)).hexdigest()
        surrogateData["embersId"] = embersId
        
        "if the action is not for regenerating past warning, then store the surrogate and warning"
        if not regeFlag:
            "Insert the surrogatedata to sqlite DB"
            insert_surrogatedata(conn, surrogateData)
            
            #push surrodate data into ZMQ
            with queue.open(PORT, 'w', capture=False) as outq:
                outq.write(surrogateData)
        
        return surrogateData
    except Exception as e:
        log.exception( "process_single_stock Error: %s" % e.message)
        return None

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
        cur = conn.cursor()
        
        "If the warning is already in database, do not need to insert"
        checkSql = "select count(*) from t_warningmessage where embers_id = ?"
        embersId = warningMessage["embersId"]
        cur.execute(checkSql,(embersId,)) 
        count = cur.fetchone()[0]

        if count == 0:
            insertSql = "insert into t_warningmessage (embers_id,derived_from,model,event_type,confidence,confidence_isprobability,\
            event_date,location,population,operate_time,version,comments,description) values (?,?,?,?,?,?,?,?,?,?,?,?,?)"
            
            derivedFrom = json.dumps(warningMessage["derivedFrom"])
            model = warningMessage["model"]
            eventType =  warningMessage["eventType"]
            confidence = warningMessage["confidence"]
            confidenceIsProbability = warningMessage["confidenceIsProbability"] 
            eventDate = warningMessage["eventDate"]
            population = warningMessage["population"] 
            location = warningMessage["location"]
            comments = warningMessage["comments"]
            description = warningMessage["description"]
            operateTime = warningMessage["dateProduced"]
            version = warningMessage["version"]
            
            cur.execute(insertSql,(embersId,derivedFrom,model,eventType,confidence,confidenceIsProbability,eventDate,location,population,
                                   operateTime,version,comments,description))
            conn.commit()
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
    cur = conn.cursor()
    
    try:
        pClusster = trend_type
        table_name = "t_enriched_bloomberg_prices"  
        
        sql = "select current_value from {} where name='{}' and post_date < '{}' order by post_date desc limit 1".format(table_name,stock_index,date)
        cur.execute(sql)
        result = cur.fetchone()
        current_val = float(result[0])
        
        querySql = "select one_day_change from {} where name='{}' and post_date <'{}' order by post_date desc limit 30 ".format(table_name,stock_index,date)
        cur.execute(querySql)
        rows = cur.fetchall()
        moving30 = []
        for row in rows:
            moving30.append(row[0])
        
        querySql = "select one_day_change from {} where name='{}' and post_date <'{}' order by post_date desc limit 90 ".format(table_name,stock_index,date)
        cur.execute(querySql)
        rows = cur.fetchall()
        moving90 = []
        for row in rows:
            moving90.append(row[0])
        
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
        derivedFrom = surObj["embersId"]
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
        operateTime = datetime.now().isoformat()
        warningMessage["dateProduced"] = operateTime
        comObj["trendVersion"] = CONFIG["trendRange"]["version"]
        warningMessage["comments"] = json.dumps(comObj)
        warningMessage["description"] = "Use Bayesian to predict stock sigma events"
        
        embersId = hashlib.sha1(json.dumps(warningMessage)).hexdigest()
        warningMessage["embersId"] = embersId
        
        if not regeFlag:
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
    cur = conn.cursor()
    sql = "select comments from t_warningmessage where event_date = '{}' limit 1"
    cur.execute(sql)
    result = cur.fetchone()
    return json.loads(result[0])
        
     
def parse_args():
    ap = argparse.ArgumentParser("Apply the bayesian model to predict stock warning")
    ap.add_argument('-c',dest="model_cfg",metavar="MODEL CFG",default="./data/bayesian_model.cfg",type=str,nargs='?',help='the config file')
    ap.add_argument('-tf',dest="trend_file",metavar="TREND RANGE FILE",default="./data/trendRange.json", type=str,nargs='?',help="The trend range file")
    ap.add_argument('-z',dest="port",metavar="ZMQ PORT",default="tcp://*:30115",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The sqlite database file')
    default_day = datetime.strftime(datetime.now() + timedelta(days =1),"%Y-%m-%d")
    ap.add_argument('-d',dest="predict_date",metavar="PREDICT DATE",type=str,default=default_day,nargs="?",help="The day to be predicted")
    ap.add_argument('-s',dest="stock_list",metavar="Stock List",type=str,nargs="+",help="The list of stock to be predicted")
    ap.add_argument('-rg',dest="rege_date",metavar="Regenerate Date",type=str,help="The date need to be regerated")
    return ap.parse_args()

def main():
    global CONFIG,VOCABULARY_FILE,PORT,TREND_FILE,__version__
    "Get the input args"
    args = parse_args()
    rege_date = args.rege_date
    "if rege_date is not none, it means to regenerate the past day's prediction"
    if not rege_date:
        "Normal predict"
        predict_date = args.predict_date
        model_cfg = args.model_cfg
        TREND_FILE = args.trend_file
        PORT = args.port
        db_file = args.db_file
        stock_list = None
        if args.stock_list:
            stock_list = json.loads(args.stock_list)
            
        conn = lite.connect(db_file)
        "Get the Latest version of Config Object"
        configObj = json.load(open(model_cfg))
        con_versionNum = max([int(v) for v in configObj.keys()])
        CONFIG = configObj[str(con_versionNum)]
        
        "Get the Latest version of Trend Range Object"
        clusterTrends = json.load(open(TREND_FILE))
        trend_versionNum = max([int(v) for v in clusterTrends.keys()])
        CONFIG["trendRange"] = {"version":str(trend_versionNum),"range":clusterTrends[str(trend_versionNum)]}
        
        if not stock_list:
            stock_list = CONFIG["stocks"]
        
        "Process stock each by each"
        for stock in stock_list:
            surrogate = process_single_stock(conn,predict_date,stock)
            if surrogate:
                warning = warning_check(conn,surrogate)
                if warning:
                    "push warning message to zmq"
                    with queue.open(PORT, 'w', capture=False) as outq:
                        outq.write(warning)
    else:
        "regenerate the old prediction"
        model_cfg = args.model_cfg
        TREND_FILE = args.trend_file
        PORT = args.port
        db_file = args.db_file
        stock_list = None
        if args.stock_list:
            stock_list = json.loads(args.stock_list)
            
        conn = lite.connect(db_file)
        "Get the version of Config Object for the indicated prediction"
        versionObj = get_predicion_version(conn,rege_date)
        configVersionNum = versionObj["configVersion"]
        trendVersionNum = versionObj["trendVersion"]
        
        configObj = json.load(open(model_cfg))
        CONFIG = configObj[configVersionNum]
        
        "Get the Latest version of Trend Range Object"
        clusterTrends = json.load(open(TREND_FILE))
        CONFIG["trendRange"] = {"version":str(trend_versionNum),"range":clusterTrends[trendVersionNum]}
        
        if not stock_list:
            stock_list = CONFIG["stocks"]
        
        "Process stock each by each"
        for stock in stock_list:
            surrogate = process_single_stock(conn,predict_date,stock,True)
            if surrogate:
                warning = warning_check(conn,surrogate,True)
        
    if conn:
        conn.close()
        
CONFIG = {}
PORT = ""
TREND_FILE = ""

if __name__ == "__main__":
    main()
        