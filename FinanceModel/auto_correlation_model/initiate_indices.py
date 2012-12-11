import xlrd
import json
import datetime
import hashlib
import os
import argparse
import sys
from etool import logs
from Util import calculator
import sqlite3 as lite

"""
    Initiate the Json data for all the Indices
"""
logs.init()
__processor__ = os.path.basename(__file__.split(".")[0])
log =  logs.getLogger(__processor__)

def transcsv2json(xlsFile):
    wb = xlrd.open_workbook(xlsFile)
    sh = wb.sheet_by_name('Sheet1')
    #read the stock index from file
    stockIndex = sh.row_values(0,0)[0].split(" ")[0]
    stockPrices = []
    for rownum in range(2,sh.nrows):
        try:
            time_tuple = xlrd.xldate_as_tuple(sh.row_values(rownum,0)[0],0)
            post_date = datetime.datetime.strftime(datetime.date(time_tuple[0],time_tuple[1],time_tuple[2]),"%Y-%m-%d")
            lastPrice = float(sh.row_values(rownum,0)[1])
            previousCloseValue = float(sh.row_values(rownum,0)[2])
           
            dayValue = {}
            dayValue["previousCloseValue"] = previousCloseValue
            updateTime = datetime.datetime.strftime(datetime.datetime.strptime(post_date,"%Y-%m-%d"),"%m/%d/%Y") + " 16:00:00"
            dayValue["updateTime"] = updateTime
            dayValue["name"] = stockIndex
            dayValue["type"] = "currency"
            dayValue["feed"] = "Bloomberg - Currency"
            dayValue["date"] = post_date
            dayValue["queryTime"] = "10/01/2012 04:00:03"
            dayValue["currentValue"] = lastPrice
            embersId = hashlib.sha1(json.dumps(dayValue)).hexdigest()
            dayValue["embersId"] = embersId
            stockPrices.append(dayValue)
        except:
            message = sys.exc_info()
            log.info(message)
            continue
    
    return stockIndex,stockPrices

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
    
def check_if_existed(conn,raw_data):
    cur = conn.cursor()
    ifExisted = True
    sql = "select count(*) from t_bloomberg_prices where name = ? and post_date = ?"
    stock_index =  raw_data["name"]  
    tmpUT =  raw_data["updateTime"].split(" ")[0]
    update_time = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1] 
    cur.execute(sql,(stock_index,update_time))
    count = cur.fetchone()[0]
    if count == 0:
        ifExisted = False
    return ifExisted    

def process(conn,raw_data):
    try:
        "Check if current data already in database, if not exist then insert otherwise skip"
        ifExisted = check_if_existed(conn,raw_data)
        if not ifExisted:
            sql = "insert into t_bloomberg_prices (embers_id,type,name,current_value,previous_close_value,update_time,query_time,post_date,source) values (?,?,?,?,?,?,?,?,?) "
            embers_id = raw_data["embersId"]
            ty = raw_data["type"]
            name = raw_data["name"]
            tmpUT =  raw_data["updateTime"].split(" ")[0]
            update_time = raw_data["updateTime"]
            last_price = float(raw_data["currentValue"])
            pre_last_price = float(raw_data["previousCloseValue"])
            one_day_change = round(last_price - pre_last_price,4)
            query_time = raw_data["queryTime"]
            source = raw_data["feed"]
            post_date = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1]
            
            cur = conn.cursor()
            cur.execute(sql,(embers_id,ty,name,last_price,pre_last_price,update_time,query_time,post_date,source))
            
            "Initiate the enriched Data"
            enrichedData = {}
            
            "calculate zscore 30 and zscore 90"
            zscore30 = getZscore(conn,post_date,name,one_day_change,30)
            zscore90 = getZscore(conn,post_date,name,one_day_change,90)
            
            derived_from = "[" + embers_id + "]"
            enrichedData["derivedFrom"] = derived_from
            enrichedData["type"] = ty
            enrichedData["name"] = name
            enrichedData["postDate"] = post_date
            enrichedData["currentValue"] = last_price
            enrichedData["previousCloseValue"] = pre_last_price
            enrichedData["oneDayChange"] = one_day_change
            enrichedData["changePercent"] = round((last_price - pre_last_price)/pre_last_price,4)
            enrichedData["zscore30"] = zscore30
            enrichedData["zscore90"] = zscore90
            enrichedData["operateTime"] = datetime.datetime.now().isoformat()
            enrichedDataEmID = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
            enrichedData["embersId"] = enrichedDataEmID
           
            insert_enriched_data(conn,enrichedData)
    except:
        log.exception(sys.exc_info())
            
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

def import2db(conn,stockFile):
    with open(stockFile,"r") as s_file:
        lines = s_file.readlines()
        for line in lines:
            line = line.strip().replace("\r","").replace("\n","")
            day_json = json.loads(line)
            process(conn, day_json)
    return len(lines)

def parse_args():
    ap = argparse.ArgumentParser("The Program to initiate stock prices from xls to json")
    ap.add_argument('-dir',dest='price_dir',type=str,help='The directory storing the stock prices')
    ap.add_argument('-od',dest='out_dir',type=str,help='The directory used to store the output json')
    ap.add_argument('-db',dest='db_path',type=str,help='The Sqlite database File')
    return ap.parse_args()


def main():
    #read the stock prices files from directory
    args = parse_args()
    print args
    price_dir = args.price_dir
    out_dir = args.out_dir
    db_path = args.db_path
    
    assert db_path," We need db_path to store our data"
    conn = lite.connect(db_path)
    
    files = os.listdir(price_dir)
    for f in files:
        f = os.path.join(price_dir,f)
        if os.path.isfile(f):
            stock, stockjson = transcsv2json(f)
            name = stock + ".json"
            out_file = os.path.join(out_dir,name)
            with open(out_file,"w") as out_w:
                for day_value in stockjson:
                    out_w.write(json.dumps(day_value))
                    out_w.write("\n")
    
    "Import the data to database"
    jsonFiles = os.listdir(out_dir)
    for jf in jsonFiles:
        jf = os.path.join(out_dir,jf)
        numbers = import2db(conn,jf)
        conn.commit()
        print numbers
    if conn:
        conn.close()
if __name__ == "__main__":
    main()

        