#-*- coding:utf8 -*-
import json
import sqlite3 as lite
import argparse
from Util import common

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
TREND_RANGE = {}   

def process(conn,raw_data):
    "Check if current data already in database, if not exist then insert otherwise skip"
    sql = "insert into t_bloomberg_prices (embers_id,type,name,current_value,previous_close_value,update_time,query_time,post_date,source) values (?,?,?,?,?,?,?,?,?) "
    embers_id = raw_data["embersId"]
    ty = raw_data["type"]
    name = raw_data["name"]
    tmpUT =  raw_data["updateTime"].split(" ")[0]
    update_time = raw_data["updateTime"]
    last_price = float(raw_data["currentValue"])
    pre_last_price = float(raw_data["previousCloseValue"])
    query_time = raw_data["queryTime"]
    source = raw_data["feed"]
    post_date = tmpUT.split("/")[2] + "-" +  tmpUT.split("/")[0] + "-" + tmpUT.split("/")[1]
    
    cur = conn.cursor()
    cur.execute(sql,(embers_id,ty,name,last_price,pre_last_price,update_time,query_time,post_date,source))


def parse_args():
    ap = argparse.ArgumentParser("Process the raw stock index data")
    ap.add_argument('-f',dest="bloomberg_price_file",metavar="STOCK PRICE",type=str,help='The stock price file')
    ap.add_argument('-db',dest="db_file",metavar="Database",type=str,help='The stock price file')
    return ap.parse_args() 

def import_history():
    hisFile = common.get_configuration("training", "HISTORICAL_STOCK_JSON")
    raw_price_list = []
    with open(hisFile,'r') as raw_file:
        lines = raw_file.readlines()
        for line in lines:
            raw_data = json.loads(line.replace("\n","").replace("\r",""))
            raw_price_list.append(raw_data)
    conn = common.getDBConnection()
    #process data one by one
    for raw_data in raw_price_list:
        process(conn,raw_data)
    
    if conn:
        conn.commit()

def main():
    #initiate parameters
    args = parse_args()
    bloomberg_price_file = args.bloomberg_price_file
    conn = lite.connect(args.db_file)
    #get raw price list
    raw_price_list = []
    with open(bloomberg_price_file,'r') as raw_file:
        lines = raw_file.readlines()
        for line in lines:
            raw_data = json.loads(line.replace("\n","").replace("\r",""))
            raw_price_list.append(raw_data)
            
    #process data one by one
    for raw_data in raw_price_list:
        process(conn,raw_data)
    
    if conn:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()

    
    