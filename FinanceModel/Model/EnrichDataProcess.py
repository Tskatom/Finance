# Define a class to abstract the raw data for prediction.
from datetime import date, timedelta, datetime
import json
import sqlite3 as sqlite
import math
import operator
import traceback
from Util import common
import time
import hashlib
from etool import queue,logs
# For test, import the dqueue data structure
__processor__ = 'EnrichedDataProcess'
log = logs.getLogger(__processor__)

class Enriched_Data():
    def __init__(self,cfgPath):
        common.init(cfgPath)
        logs.init()
        
    # Check if the predictive day for indicated stock is Holiday or Weekend
    def check_if_tradingday(self,predictiveDate,stockIndex):
        "Check if the day weekend"
        weekDay = datetime.strptime(predictiveDate,"%Y-%m-%d").weekday()
        if weekDay == 5 or weekDay == 6:
            log.info("%s For %s is Weekend, Just Skip!" %(predictiveDate,stockIndex))
            return False
        
        "Check if the day is holiday"
        con = common.getDBConnection()
        cur = con.cursor()
        sql = "select count(*) from s_holiday a,s_stock_country b where a.country = b.country\
        and b.stock_index=? and a.date = ?"
        cur.execute(sql,(stockIndex,predictiveDate))
        count = cur.fetchone()[0]
        if count == 0:
            return True
        else:
            log.info( "%s For %s is Holiday, Just Skip!" %(predictiveDate,stockIndex))
            return False
    
    #insert surrogate data to database
    def insert_surrogatedata(self,surrogateData):
        try:
            con = common.getDBConnection()
            cur = con.cursor()
            
            "If the surrogate data is already in database, do not need to insert"
            checkSql = "select count(*) from t_surrogatedata where embers_id = ?"
            embersId = surrogateData["embersId"]
            cur.execute(checkSql,(embersId,)) 
            count = cur.fetchone()[0]
            
            if count == 0:
                insertSql = "insert into t_surrogatedata (embers_id,derived_from,shift_date,shift_type,confidence,\
                strength,location,date,model,value_spectrum,confidence_isprobability,population) values \
                (?,?,?,?,?,?,?,?,?,?,?,?)"
                
                embersId = surrogateData["embersId"]
                derivedFrom = json.dumps(surrogateData["derivedFrom"])
                shiftDate = surrogateData["shiftDate"]
                shiftType = surrogateData["shiftType"]
                confidence = surrogateData["confidence"]
                strength = surrogateData["strength"]
                location = surrogateData["location"]
                date = surrogateData["date"]
                model = surrogateData["model"]
                valueSpectrum = surrogateData["valueSpectrum"]
                confidenceIsPrabability = surrogateData["confidenceIsProbability"]
                population = surrogateData["population"]
                
                cur.execute(insertSql,(embersId,derivedFrom,shiftDate,shiftType,confidence,strength,location,date,model,valueSpectrum,confidenceIsPrabability,population))
                con.commit()
        except Exception as e:
            log.info( "Error: %s" %e.args[0])
        finally:
            if con:
                con.close()
                
    # Iterate all stocks and get the predictions
    def enrich_all_stock( self, predictiveDate ):
        try:
            stockProbabilityList = []
            stockIndexList = self.enumberate_stock_index()
            for stockIndex in stockIndexList:
                stockProbability = self.enrich_single_stock( predictiveDate, stockIndex )
                if stockProbability is not None:
                    stockProbabilityList.append( stockProbability )
            return  stockProbabilityList  
        except Exception as e:
            log.info( "Error: %s" % e.args)
            log.info( traceback.format_exc())
                
     
    # generate the predicted date's type and probability
    def enrich_single_stock( self, predictiveDate , stockIndex ):
        try:
            "Check if the predictive Day is trading day, if so continue, otherwise just return None"
            ifTradingDay = self.check_if_tradingday(predictiveDate,stockIndex) 
            if ifTradingDay is False:
                return None
            
            predictiveResults = {}
            finalRatio = {}
            clusterProbability = {}
            predictiveProbability = 0
            stockDerived = []
            newsDerived = []
            "Iteratively compute the probabilty of each cluster for the stock "
            for clusterType in self.enumberate_clusters( stockIndex ):
                "compute the contribution of 3 past day's trend "
                stockIndexProbability,stockDerived = self.compute_stock_index_probability( predictiveDate, clusterType , stockIndex )
                "compute the contribution of 3 past day's news"
                newsProbability,newsDerived = self.compute_stock_news_probability( predictiveDate, clusterType , stockIndex )
                "combine two contribution together"
                predictiveProbability = math.exp( stockIndexProbability + newsProbability ) * float( 1e90 )
                predictiveResults[clusterType] = predictiveProbability
            
            sumProbability = sum( predictiveResults.itervalues() ) 
            
            "Get the maximum probability between the predictive values"
            for item_key, item_value in predictiveResults.iteritems():
                finalRatio[item_key] = item_value / sumProbability
            sorted_ratio = sorted( finalRatio.iteritems(), key = operator.itemgetter( 1 ), reverse = True )
            clusterProbability[stockIndex] = {}
            clusterProbability[stockIndex][predictiveDate] = sorted_ratio[0]
#            return clusterProbability
            
            "Construct the Surrogate data"
            surrogateData = {}
            date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
            "Merge News Derived and Stock Derived"
            derivedFrom = []
            for item in stockDerived:
                derivedFrom.append(item)
            for item in newsDerived:
                derivedFrom.append(item)
            model = 'Bayesian - Time serial Model'
            location = common.getLocationByStockIndex(stockIndex)
            population = stockIndex
            confidence = sorted_ratio[0][1]
            confidenceIsProbability = True
            shiftType = "Trend"
            valueSpectrum = "changePercent"
            strength = sorted_ratio[0][0]
            shiftDate = predictiveDate
            
            surrogateData["date"] = date
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
            
            "Generate Embers Id"
            jsonStr = json.dumps(surrogateData)
            embersId = hashlib.sha1(json.dumps(jsonStr)).hexdigest()
            surrogateData["embersId"] = embersId
            
            self.insert_surrogatedata(surrogateData)
            
            #push surrodate data into ZMQ
            port = common.get_configuration("info", "ZMQ_PORT")
            with queue.open(port, 'w', capture=True) as outq:
                outq.write(json.dumps(surrogateData, encoding='utf8'))
            
            return surrogateData
        except Exception as e:
            log.info( "Error: %s" % e.args)
            log.info( traceback.format_exc())
    
    # calculate the stock index contribution for the coming day
    def compute_stock_index_probability( self, predictiveDate, clusterType , stockIndex ):
        try:
            "Get the clusters List"
            stockIndexFile = open( common.get_configuration( "model", 'CLUSTER_PROBABILITY_PATH' ) )
            clusterProbability = json.load( stockIndexFile )
            clusterJson = {}
            clusterContributionJson = {}
            clusterJson = clusterProbability[stockIndex]
            "Get the contribution of each cluster"
            clusterContributionFile = open( common.get_configuration( "model", 'CLUSTER_CONTRIBUTION_PATH' ) )
            clusterContributionJson = json.load( clusterContributionFile )
            clusterTypesHistory,stockDerived = self.get_stock_index_cluster( predictiveDate, stockIndex )
            stockIndexProbability = 0
            for key in clusterContributionJson[stockIndex].keys():
                if key == str( clusterType ):
                    "Search from the Cluster contribution Matrix to get the contribution probability"
                    stockIndexProbability = stockIndexProbability + math.log( float( clusterContributionJson[stockIndex][key][int( clusterTypesHistory[0] ) - 1][2] ) ) + math.log( float( clusterContributionJson[stockIndex][key][int( clusterTypesHistory[1] ) - 1][1] ) ) + math.log( float( clusterContributionJson[stockIndex][key][int( clusterTypesHistory[2] ) - 1][0] ) ) + math.log( float( clusterJson[str( clusterType )] ) )
            
            return stockIndexProbability,stockDerived
        except Exception as e:
            log.info( traceback.format_exc())
            log.info( "Error in computing stock index probability: %s" % e.args)
    
    # calculate the stock news contribution for the coming day
    def compute_stock_news_probability( self, predictiveDate, clusterType , stockIndex ):
        try:
            termContributionFile = open( common.get_configuration( "model", 'TERM_CONTRIBUTION_PATH' ) )
            termContributionJson = json.load( termContributionFile )
            terms,newsDerived = self.get_stock_news_data( predictiveDate , stockIndex )
            termContributionProbability = 0
            if stockIndex in termContributionJson:
                for termClusterType in termContributionJson[stockIndex].keys():
                    if termClusterType == str( clusterType ):    
                        stermlist = termContributionJson[stockIndex][termClusterType]
                        #print stermlist                            
                        for word, count in terms.iteritems():                    
                            if word in stermlist:                        
                                #print word
                                termContributionProbability =  count * math.log( float( termContributionJson[stockIndex][termClusterType][word] ) )
                                del stermlist[word]
            
            return termContributionProbability,newsDerived
        except IOError:
            log.info( "Can't open the file:stock_raw_data.json.")
        except Exception as e:
            log.info( traceback.format_exc())
            log.info( "Error in computing stock news probability: %s" % e.message)    
        return None
        
    
    "This function used to collect past 3 day's news from database group by stock"
    def get_stock_news_data( self, predictiveDate , stockIndex ):
        con = None
        try:
            con = common.getDBConnection()
            cur = con.cursor()
            
            "Get past 3 day's news before Predictive Day "
            predictiveDate = datetime.strptime( predictiveDate, "%Y-%m-%d" )
            startDay = ( predictiveDate - timedelta( days = 3 ) ).strftime( "%Y-%m-%d" )
            endDay = ( predictiveDate - timedelta( days = 1 ) ).strftime( "%Y-%m-%d" )
            sqlquery = "select content,embers_id from t_daily_enrichednews where post_date>=? and post_date<=? and stock_index=?"
            
            cur.execute( sqlquery, ([startDay, endDay , stockIndex]))
            articleRecords = cur.fetchall()
            
            "Initiate the words List"
            vocabularyFile = open(common.get_configuration( "training", 'VOCABULARY_FILE'))
            wordLines = vocabularyFile.readlines()
            termList = {}
            for line in wordLines:
                line = line.replace("\n","").replace("\r","")
                termList[line] = 0
                
            newsDerived = []
            "Merge all the term in each record"
            for record in articleRecords:
                jsonRecord = json.loads(record[0])
                newsDerived.append(record[1])
                for curWord in jsonRecord:
                    if curWord in termList:
                        termList[curWord] = termList[curWord] + jsonRecord[curWord]
            
            return termList,newsDerived
        except sqlite.Error, e:
            log.info( traceback.format_exc())
            log.info( "Error: %s" % e.args[0])
        finally:
            if con:
                con.close()
    

    # This function will retrieve 3 past trading day' clusters for each predictive date
    def get_stock_index_cluster( self, predictiveDate, stockIndex ):
        
        con = None
        try:
            con = common.getDBConnection()
            cur = con.cursor()
            sqlquery = "select trend_type,embers_id from t_daily_enrichedIndex where date < ? and stock_index = ? order by date desc limit 3"
            cur.execute( sqlquery, ( predictiveDate, stockIndex ) )
            
            rows = cur.fetchall()
            trendTypeList = []
            derivedFrom = []
            for row in rows:
                trendTypeList.append( row[0] )
                derivedFrom.append(row[1])
            return trendTypeList,derivedFrom
                
        except sqlite.Error, e:
            log.info( traceback.format_exc())
            log.info( "Error: %s" % e.args[0])
        finally:
            if con:
                con.close()
    
    # This function will retrieve the list of stock symbol 
    def enumberate_stock_index( self ):
        try:
            clustersFile = open( common.get_configuration( "model", 'CLUSTER_PROBABILITY_PATH' ) )
            clusterJson = json.load( clustersFile )
            stockIndexList = []
            for stockIndex in clusterJson.keys():
                stockIndexList.append( stockIndex )
            return stockIndexList 
        except Exception as e:
            log.info( traceback.format_exc())
            log.info( "Error: %s" % e.args[0])
    
    # This function will retrieve list of clusters for each stock
    def enumberate_clusters( self , stockIndex ):
        try:
            clusterFile = open( common.get_configuration( "model", 'CLUSTER_PROBABILITY_PATH' ) )
            clusterJson = json.load( clusterFile ) 
            clustersList = []
            clusterProbability = {}
            for clusterKey in clusterJson.keys():
                if clusterKey == stockIndex:
                    clusterProbability = clusterJson[clusterKey]
                    break
            for clusterKey in clusterProbability.keys():
                clustersList.append( clusterKey )
            return clustersList 
        except Exception as e:
            log.info( traceback.format_exc())
            log.info( "Error: %s" % e.args)

def test():
    "This method used to test the function"
    data = Enriched_Data()
    data.enrich_all_stock("2011-08-08")

"Main Function used to Test"
if __name__ == "__main__":
    test()