import sqlite3 as lite
from datetime import datetime
import json

conn = lite.connect('d:/embers/financeModel/auto-regression model/embers_ar.db')
cur = conn.cursor()
stock_list = ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC']
for stock in stock_list:
    sql = "select embers_id,type,name,update_time,current_value,query_time,previous_close_value,post_date,source from t_bloomberg_prices where post_date>='2002-01-01' and name='{}' order by post_date asc".format(stock)
    {"previousCloseValue":"21736.07","updateTime":"09/28/2012 16:10:05","name":"IGBVL","feed":"Bloomberg - Stock Index","date":"2012-10-01T03:00:03","queryTime":"10/01/2012 03:00:03","currentValue":"21674.79","type":"stock","embersId":"adc866f44487f212d44ee27af63936c523ef737c"}
    cur.execute(sql)
    results = cur.fetchall()
    file_name = "D:/DropBox/Embers_data/raw_stock_2002-01TO2012_11/"+stock+".json"
    with open(file_name,"w") as out:
        for result in results:
            
            embersId = result[0]
            ty = result[1]
            name = result[2]
            update_time = result[3]
            current_value = result[4]
            query_time = result[5]
            previous_close_value =  result[6]
            post_date = result[7]
            source = result[8]
            
            dayValue = {}
            dayValue["previousCloseValue"] = previous_close_value
            dayValue["updateTime"] = update_time
            dayValue["name"] = name
            dayValue["type"] = ty
            dayValue["feed"] = "Bloomberg - Stock Index"
            dayValue["date"] = post_date
            dayValue["queryTime"] = query_time
            dayValue["currentValue"] = current_value
            dayValue["embersId"] = embersId
            
            out.write(json.dumps(dayValue))
            out.write("\n")
print update_time

for stock in stock_list:
    sql = "select embers_id,derived_from,type,name,post_date,operate_time,current_value,previous_close_value,one_day_change,change_percent,zscore30,zscore90 from t_enriched_bloomberg_prices where post_date>='2002-01-01' and name='{}' order by post_date asc".format(stock)
    cur.execute(sql)
    results = cur.fetchall()
    file_name = "D:/DropBox/Embers_data/enriched_stock_2002-01TO2012_11/enriched_"+stock+".json"
    with open(file_name,"w") as out:
        for result in results:
            embersId = result[0]
            derived_from = result[1]
            ty = result[2]
            name = result[3]
            post_date = result[4]
            operate_time = result[5]
            current_value = result[6]
            previous_close_value =  result[7]
            one_day_change = result[8]
            change_percent = result[9]
            zscore30 = result[10]
            zscore90 = result[11]
            
            dayValue = {}
            dayValue["embersId"] = embersId
            dayValue["derivedFrom"] = derived_from
            dayValue["type"] = ty
            dayValue["name"] = name
            dayValue["postDate"] = post_date
            dayValue["operateTime"] = operate_time
            dayValue["currentValue"] = current_value
            dayValue["previousCloseValue"] = previous_close_value
            dayValue["oneDayChange"] = one_day_change
            dayValue["changePercent"] = change_percent
            dayValue["zscore30"] = zscore30
            dayValue["zscore90"] = zscore90
            
            
            out.write(json.dumps(dayValue))
            out.write("\n")
#    dayValue["updateTime"] = updateTime