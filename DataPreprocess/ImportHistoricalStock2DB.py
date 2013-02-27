from Util import common,calculator
import os
from datetime import datetime

def init(cfgPath):
    common.init(cfgPath)
    
def import_historical_stock():
    #get the historical stock dir
    stockFileDir = common.get_configuration("training", "HISTORICAL_STOCK")
    fileNames = os.listdir(stockFileDir)
    con = common.getDBConnection()
    cur = con.cursor()
    
    #clear the database
    clearSql = "delete from t_daily_stockindices"
    cur.execute(clearSql)
    con.commit()
    
    sql = "insert into t_daily_stockindices (sub_sequence,stock_index,date,last_price,one_day_change) values (?,?,?,?,?)";
    
    for filename in fileNames:
        fpath = stockFileDir + "/" + filename
        stock = filename.split(".")[0]
        subSequence = 0
        with open(fpath,"r") as stockFile:
            lines = stockFile.readlines()[2:]
            for line in lines:
                line = line.replace("\r","").replace("\n","")
                date = line.split(",")[0]
                lastPrice = line.split(",")[1]
                previousLastPrice = line.split(",")[2]
                
                if lastPrice == "#N/A N/A" or previousLastPrice == "#N/A N/A":
                    continue
                
                lastPrice = float(lastPrice)
                previousLastPrice = float(previousLastPrice)
                date = datetime.strptime(date,"%m/%d/%Y").strftime("%Y-%m-%d")
                oneDayChange = round(lastPrice - previousLastPrice,4)
                subSequence = subSequence + 1
                cur.execute(sql,(subSequence,stock,date,lastPrice,oneDayChange,))
                if subSequence % 300 == 0:
                    con.commit()
            con.commit() 

def getZscore(curDate,stockIndex,curDiff,duration):
    con = common.getDBConnection()
    cur = con.cursor()
    scores = []
    sql = "select one_day_change from t_daily_stockindices where date<? and stock_index = ? order by date desc limit ?"
    cur.execute(sql,(curDate,stockIndex,duration))
    rows = cur.fetchall()
    for row in rows:
        scores.append(row[0])
    zscore = calculator.calZscore(scores, curDiff)
    return zscore           

def compute_zscore():
    
    con = common.getDBConnection()
    cur = con.cursor()
    sql = "select sequence_id,stock_index,date,one_day_change from t_daily_stockindices order by sequence_id asc "
    cur.execute(sql)
    rows = cur.fetchall()
    
    updateCur = con.cursor()
    updateSql = "update t_daily_stockindices set zscore30 = ? ,zscore90 = ? where stock_index=? and sequence_id = ?"
    i = 0
    for row in rows:
        sequenceId = row[0]
        stockIndex = row[1]
        date = row[2]
        oneDayChange = row[3]
        
        zscore30 = getZscore(date,stockIndex,oneDayChange,30)
        zscore90 = getZscore(date,stockIndex,oneDayChange,90)
        
        updateCur.execute(updateSql,(zscore30,zscore90,stockIndex,sequenceId))
        i = i + 1
        
        if i % 300 == 0:
            con.commit()
            print "commit"
    con.commit()
    
if __name__ == "__main__":
    init("../Config/config.cfg")
    import_historical_stock()
    compute_zscore()
            