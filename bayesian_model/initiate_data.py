import boto
import argparse
import sys
import sqlite3 as lite
import hashlib

"""
Tables need to be initiated
    1> t_enriched_bloomberg_prices
    2> bloomberg_keywords
"""

def arg_parse():
    ap = argparse.ArgumentParser("Initiate the tables in simpleDB")
    ap.add_argument('-k',dest="key_id",metavar="KEY ID",type=str,help="The AWS key ID")
    ap.add_argument('-s',dest="secret",type=str,help="The AWS secret")
    ap.add_argument('-db',dest="db_file",type=str,help="The AWS secret")
    return ap.parse_args()

def get_domain(conn,d_name):
    conn.create_domain(d_name)#you can create repeatly
    return conn.get_domain(d_name)

def store(message,domain):
    assert message, 'Message is empty, cannot store it'    
    domain.put_attributes(message['embersId'],message)

def store_eprice_messages(sql_conn,domain):
    cur = sql_conn.cursor()
    sql = "select embers_id,derived_from,type,name,post_date,operate_time,current_value,previous_close_value,one_day_change\
            , change_percent,zscore30,zscore90,trend_type from t_enriched_bloomberg_prices where type='stock' "
    cur.execute(sql)
    rs = cur.fetchall()
    for r in rs:
        message = {}
        message["embersId"] = r[0]
        message["derivedFrom"] = r[1]
        message["type"] = r[2]
        message["name"] = r[3]
        message["postDate"] = r[4]
        message["operateTime"] = r[5]
        message["currentValue"] = r[6]
        message["previousCloseValue"] = r[7]
        message["oneDayChange"] = r[8]
        message["changePercent"] = r[9]
        message["zscore30"] = r[10]
        message["zscore90"] = r[11]
        message["trendType"] = r[12] 
        
        store(message,domain)

def store_holiday_message(sql_conn,domain):
    cur = sql_conn.cursor()
    sql = " select a.country,stock_index,holiday_name,date from s_holiday a,s_stock_country b where a.country = b.country "
    cur.execute(sql)
    rs = cur.fetchall()
    for r in rs:
        message = {}
        message["country"] = r[0]
        message["stockIndex"] = r[1]
        message["holidayName"] = r[2]
        message["date"] = r[3]
        embersId = hashlib.sha1(str(message)).hexdigest()
        message["embersId"] = embersId
        
        store(message,domain)

def store_curency_data(sql_conn,domain):
    cur = sql_conn.cursor()
    sql = "select embers_id,derived_from,type,name,post_date,operate_time,current_value,previous_close_value,one_day_change\
            , change_percent,zscore30,zscore90,trend_type from t_enriched_bloomberg_prices where type='currency' and name in ('USDARS','USDBRL','USDCLP','USDCOP','USDCRC','USDMXN','USDPEN')"
    cur.execute(sql)
    rs = cur.fetchall()
    for r in rs:
        message = {}
        message["embersId"] = r[0]
        message["derivedFrom"] = r[1]
        message["type"] = r[2]
        message["name"] = r[3]
        message["postDate"] = r[4]
        message["operateTime"] = r[5]
        message["currentValue"] = r[6]
        message["previousCloseValue"] = r[7]
        message["oneDayChange"] = r[8]
        message["changePercent"] = r[9]
        message["zscore30"] = r[10]
        message["zscore90"] = r[11]
        message["trendType"] = r[12] 
        
        store(message,domain)
            
def main():
    args = arg_parse()
    print args
    key_id = args.key_id
    secret = args.secret
    sql_db_f = args.db_file
    print key_id,secret,sql_db_f
    sql_conn = lite.connect(sql_db_f)
    sim_conn = boto.connect_sdb(key_id,secret)
    
    
    sim_conn.delete_domain("t_enriched_bloomberg_prices")
    price_domain = get_domain(sim_conn,"t_enriched_bloomberg_prices")
    store_eprice_messages(sql_conn,price_domain)
    
#    holiday_domain = get_domain(sim_conn,"s_holiday")
#    store_holiday_message(sql_conn,holiday_domain)

#    currency_domain = get_domain(sim_conn,"t_enriched_bloomberg_prices")
#    store_curency_data(sql_conn,currency_domain)
    

if __name__ == "__main__":
    main()        