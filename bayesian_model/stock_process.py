#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys
import json
import hashlib
import calculator
from etool import logs,queue
import argparse
from datetime import datetime,timedelta
import boto
import pytz
import time

#rawData = {'feed': 'Bloomberg - Stock Index', 'updateTime': '09/28/2012 16:01:02', 'name': 'MERVAL', 'currentValue': '2451.73', 'queryTime': '10/01/2012 03:00:03', 'previousCloseValue': '2494.18', 'date': '2012-10-01T03:00:03', 'type': 'stock', 'embersId': '971752f23223c344e8732f32922e3f8e75ebd3ff'}
#EnrichedData = 

"""
Description: 
    input Parameters:
    ap.add_argument('-t',dest="trend_file",metavar="TREND RANGE FILE",default="./trendRange.json", type=str,nargs='?',help="The trend range file")
    ap.add_argument('-ze',dest="port",metavar="ZMQ PORT",default="tcp://*:30113",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
    ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
    utc_dt = T_UTC.localize(datetime.utcnow())
    eas_dt = utc_dt.astimezone(T_EASTERN)
    default_day = datetime.strftime(eas_dt,"%Y-%m-%d")
    ap.add_argument('-d',dest="operate_date",metavar="OPERATE DATE",type=str,default=default_day,nargs="?",help="The day to be processed")
    ap.add_argument('-sd',dest="start_date",metavar="START OPERATE DATE",type=str,nargs="?",help="The day to be processed")
    ap.add_argument('-ed',dest="end_date",metavar="END OPERATE DATE",type=str,nargs="?",help="The day to be processed")
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

def get_domain(conn,domain_name):
    conn.create_domain(domain_name)
    return conn.get_domain(domain_name)

def getZscore(conn,cur_date,stock_index,cur_diff,duration):
    scores = []
    t_domain = get_domain(conn,'t_enriched_bloomberg_prices')
    sql = "select oneDayChange from t_enriched_bloomberg_prices where postDate<'{}' and name = '{}' order by postDate desc".format(cur_date,stock_index)
    rows = t_domain.select(sql,max_items=duration)
    for row in rows:
        scores.append(float(row['oneDayChange']))
    zscore = calculator.calZscore(scores, cur_diff)
    return zscore
    
        
def get_trend_type(raw_data):
    """
    Computing current day's trend changeType, compareing change percent to the trend range,
    Choose the nearnes trend as current day's changeType    
    """
    
    "Get the indicated stock range"
    stockIndex = raw_data["name"]
    tJson = TREND_RANGE[stockIndex]
    
    "Computing change percent"
    lastPrice = float(raw_data["currentValue"].replace(",",""))
    preLastPrice = float(raw_data["previousCloseValue"].replace(",",""))
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
    
    TREND_RANGE[stockIndex][trendType][0] = bottom
    TREND_RANGE[stockIndex][trendType][1] = top
    
    return trendType
    

def process(conn, port, raw_data):
    "Check if current data already in database, if not exist then insert otherwise skip"
    ifExisted = check_if_existed(conn,raw_data)
    if not ifExisted:
        embers_id = raw_data["embersId"]
        ty = raw_data["type"]
        name = raw_data["name"]
        last_price = float(raw_data["currentValue"].replace(",",""))
        pre_last_price = float(raw_data["previousCloseValue"].replace(",",""))
        one_day_change = round(last_price - pre_last_price,4)
        source = raw_data["feed"]
        post_date = raw_data["date"][0:10]
        raw_data['postDate'] = post_date
        
#        t_domain = get_domain(conn,'t_bloomberg_prices')
#        t_domain.put_attributes(embers_id,raw_data)
        "Initiate the enriched Data"
        enrichedData = {}
        
        "calculate zscore 30 and zscore 90"
        zscore30 = getZscore(conn,post_date,name,one_day_change,30)
        zscore90 = getZscore(conn,post_date,name,one_day_change,90)
        
        if ty == "stock":
            trend_type = get_trend_type(raw_data)
        else:
            trend_type = "0"
        derived_from = {"derivedIds":[embers_id]}
        
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
        enrichedData["operateTime"] = datetime.utcnow().isoformat()
        enrichedDataEmID = hashlib.sha1(json.dumps(enrichedData)).hexdigest()
        enrichedData["embersId"] = enrichedDataEmID
       
        insert_enriched_data(conn,enrichedData)
        
        #push data to ZMQ
        with queue.open(port, 'w', capture=False) as outq:
            outq.write(enrichedData)
            
def insert_enriched_data(conn,enrichedData):
    
    t_domain = get_domain(conn,'t_enriched_bloomberg_prices')
    embers_id = enrichedData["embersId"]
    t_domain.put_attributes(embers_id,enrichedData)
    
def check_if_existed(conn,raw_data):
    ifExisted = True
    t_domain = get_domain(conn,'t_enriched_bloomberg_prices')
    stock_index =  raw_data["name"]  
    update_time = raw_data["date"][0:10]
    sql = "select count(*) from t_enriched_bloomberg_prices where name = '{}' and postDate = '{}'".format(stock_index,update_time)
    rs = t_domain.select(sql)
    count = 0
    for r in rs:
        count = int(r['Count'])
    if count == 0:
        ifExisted = False
    return ifExisted    

def get_raw_data(conn,operate_date):
    t_domain = get_domain(conn,"bloomberg_prices")
    t_format = "%Y-%m-%d"
    end_date = datetime.strftime(datetime.strptime(operate_date,t_format) + timedelta(days=1),t_format)
    sql = "select * from bloomberg_prices where date >'{}' and date <'{}' and type = 'stock' order by date asc".format(operate_date,end_date)
    rs = t_domain.select(sql)
    return rs

def parse_args():
    ap = argparse.ArgumentParser("Process the raw stock index data")
#    ap.add_argument('-f',dest="bloomberg_price_file",metavar="STOCK PRICE",type=str,help='The stock price file')
    # read stdin, write stdout ap.add_argument('-t',dest="trend_file",metavar="TREND RANGE FILE",default="./trendRange.json", type=str,nargs='?',help="The trend range file")
    ap.add_argument('-ze',dest="port",metavar="ZMQ PORT",default="tcp://*:30113",type=str,nargs="?",help="The zmq port")
    ap.add_argument('-kd',dest="key_id",metavar="KeyId for AWS",type=str,help="The key id for aws")
    ap.add_argument('-sr',dest="secret",metavar="secret key for AWS",type=str,help="The secret key for aws")
    utc_dt = T_UTC.localize(datetime.utcnow())
    eas_dt = utc_dt.astimezone(T_EASTERN)
    default_day = datetime.strftime(eas_dt,"%Y-%m-%d")
    ap.add_argument('-d',dest="operate_date",metavar="OPERATE DATE",type=str,default=default_day,nargs="?",help="The day to be processed")
    ap.add_argument('-sd',dest="start_date",metavar="START OPERATE DATE",type=str,nargs="?",help="The day to be processed")
    ap.add_argument('-ed',dest="end_date",metavar="END OPERATE DATE",type=str,nargs="?",help="The day to be processed")
    return ap.parse_args() 

def main():
    #initiate parameters
    global TREND_RANGE,T_UTC,T_EASTERN
    "Initiate the TimeZone Setting"
    T_UTC = pytz.utc
    T_EASTERN = pytz.timezone("US/Eastern")
    
    args = parse_args()
    KEY_ID = args.key_id
    SECRET = args.secret
    conn = boto.connect_sdb(KEY_ID,SECRET)
    operate_date = args.operate_date
    start_date = args.start_date
    end_date = args.end_date
    
    port = args.port
    #trend_file = args.trend_file
    # "Load the trend changeType range file"
    trendObject = None
    trendObject = json.load(sys.stdin)    
    
    # "Get the latest version of Trend Ranage"
    trend_versionNum = max([int(v) for v in trendObject.keys()])
    # "To avoid changing the initiate values, we first transfer the json obj to string ,then load it to create a news object"
    TREND_RANGE = json.loads(json.dumps(trendObject[str(trend_versionNum)]))
    
    # "If input a date range, then we will handle all the data query from those days"
    if start_date is None:
        #get raw price list
        raw_price_list = []
        rs = get_raw_data(conn,operate_date)
        for r in rs:
            raw_price_list.append(r)
        for raw_data in raw_price_list:
            process(conn, port, raw_data)
    else:
        t_format = "%Y-%m-%d"
        s_date = datetime.strptime(start_date,t_format)
        e_date = datetime.strptime(end_date,t_format)
        while s_date <= e_date:
            raw_price_list = []
            rs = get_raw_data(conn,datetime.strftime(s_date,t_format))
            for r in rs:
                raw_price_list.append(r)
            for raw_data in raw_price_list:
                process(conn, port, raw_data)
            s_date = s_date + timedelta(days=1)
            # "sleep 5 s to wait simpleDB to commit"
            time.sleep(5)
    
    
    #"Write back the trendFile"
    new_version_num = trend_versionNum + 1
    trendObject[str(new_version_num)] = TREND_RANGE
    json.dump(trendObject, sys.stdout)
    
if __name__ == "__main__":
    main()

