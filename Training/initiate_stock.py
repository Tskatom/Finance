import json
import sqlite3 as lite
from Util import common
import argparse
import cProfile
import os

def arg_parser():
    ap = argparse.ArgumentParser("Initiate the Parameters")
    ap.add_argument('-r',dest="r_dir",type=str,help="The input path of news file")
    ap.add_argument('-e',dest="e_dir",type=str,help="The input path of news file")
    return ap.parse_args()

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
    

def insert_raw_stock(conn,raw_data):
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

def initiate(r_dir,e_dir,conn):
    r_files = os.listdir(r_dir)
    for f in r_files:
        f_name = r_dir + "/" + f
        for line in open(f_name):
            r_stock = json.loads(line)
            insert_raw_stock(conn,r_stock)
    
    e_files = os.listdir(e_dir)
    for f in e_files:
        f_name = e_dir + "/" + f
        for line in open(f_name):
            e_stock = json.loads(line)
            insert_enriched_data(conn,e_stock)
    
    conn.commit()

def clear(con):
    cur = con.cursor()
    "clear the stock index raw data"
    clearSql = "delete from t_bloomberg_prices"
    cur.execute(clearSql)
    con.commit()
    
    "clear the stock index enriched data"
    clearSql = "delete from t_enriched_bloomberg_prices"
    cur.execute(clearSql)
    con.commit()
    
def main():
    args = arg_parser()
    r_dir = args.r_dir
    e_dir = args.e_dir
    conn = common.getDBConnection()
    clear(conn)
    initiate(r_dir,e_dir,conn)
    if conn:
        conn.close()

if __name__ == "__main__":
    cProfile.run("main()")
    
"""
{"postTime": "2011-05-09T16:44:20", "postDate": "2011-05-09", "author": " Blake Schmidt ", 
"url": "http://www.bloomberg.com/news/2011-05-09/colombia-stocks-occidente-nutresa-fabricato-ecopetrol.html", 
"embersParentId": "4bd889d070b2e0a0071aa023cbf03520dcf75b6f", "company": "", 
"title": "Colombia Stocks: Occidente, Nutresa, Fabricato, Ecopetrol", "content": {"help": 1, "producer": 2, "-": 11, "global": 1, "soon": 1, "month": 2, "andre": 1, "jimenez": 2, "09t20:44:20z": 1, "weekly": 1, "synthetic": 1, "blake": 2, "based": 2, "$": 2, "la": 1, "rose": 1, "fix": 1, "send": 1, "lender": 1, "ecopetl": 1, "schmidt": 2, "local": 1, "worth": 1, "@": 2, "within": 1, "finance": 1, "food": 1, "big": 4, "rebound": 1, "de": 2, "crude": 1, "inflation": 1, "five": 1, "international": 1, "report": 1, "trade": 1, "republica": 1, "bank": 2, "medellin": 1, "signal": 1, "rubiales": 1, "large": 1, "bloomberg.net": 2, "security": 1, "occidente": 2, "bond": 2, "bogota": 2, "energy": 1, "pacific": 1, "rate": 1, "fiber": 1, "trading": 1, "expect": 1, "echeverry": 2, "occid": 1, "canacol": 1, "2.1": 1, "190.45": 1, "carlo": 2, "enka": 2, "index": 2, "since": 2, "p.m.": 1, "13977.61": 1, "re": 1, "state": 1, "cne": 1, "ecopetrol": 2, "new": 3, "price": 3, "accord": 1, "latin": 1, "run": 1, "igbc": 1, "1670.42": 1, "u.s.": 2, "million": 1, "agreement": 1, "free": 1, "manuel": 1, "base": 1, "york": 2, "problem": 1, "change": 1, "david": 1, "april": 1, "climb": 1, "maker": 1, "south": 1, "1.4": 1, "cb": 2, "manage": 1, "two": 1, "colombia": 7, "vote": 1, "banco": 1, "tomorrow": 1, "market": 1, "decline": 1, "unusual": 1, "america": 2, "parenthesizeis": 1, "due": 2, "prec": 1, "cali": 1, "much": 1, "editor": 1, "bschmidt16": 1, "calgary": 1, "proposal": 1, "tell": 1, "today": 1, "head": 1, "offer": 1, "colcap": 1, "company": 8, "oil": 4, "fund": 1, "link": 1, "gain": 3, "despite": 1, "interbank": 1, "reporter": 1, "fabricato": 2, "bill": 1, "peso": 8, "economic": 1, "following": 1, "ibr": 1, "remains": 1, "acquisition": 1, "procedural": 1, "0.9": 1, "0.8": 1, "1.1": 1, "1.2": 1, "1.3": 1, "ltd.": 1, "share": 2, "0.4": 1, "say": 5, "interbolsa": 1, "enrique": 1, "story": 2, "linked": 1, "tejicondor": 1, "sell": 3, "santos": 1, "intact": 1, "recovery": 1, "papadopoulos": 2, "juan": 2, "responsible": 1, "percent": 10, "nutresa": 3, "textile": 3, "field": 1, "contact": 2, "chairman": 1, "stock": 2, "rise": 8, "'s": 6, "fabri": 1, "congress": 2, "may": 2, "symbol": 1, "plan": 2, "ultrabursatiles": 2, "president": 1, "grupo": 1, "stocks": 1, "barrel": 1, "billion": 1, "piedrahita": 1, "sale": 2, "daily": 1, "48.1": 1, "withdraw": 1, "time": 1, "sa": 7, "operate": 1, "minister": 1}, 
"source": "Bloomberg News", "updateDate": "2012-10-02", "date": "2012-10-02T14:30:13", "stockIndex": "COLCAP", "embersId": "39be6d31a9aaf5a2bf2a0828b3ed919670e5d05a"}
"""        
    
