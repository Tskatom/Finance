import sqlite3 as lite
import csv

def out_put(stock_list,conn):
    cur = conn.cursor()
    for stock in stock_list:
        sql = "select name,post_date,current_value,previous_close_value,one_day_change,change_percent,zscore30,zscore90 from t_enriched_bloomberg_prices where name='{}' and post_date >='2003-01-01' and post_date <='2012-10-31' ".format(stock)
        
        cur.execute(sql)
        rs = cur.fetchall()
        file_name = "/home/vic/workspace/data/stock/" + stock + ".csv"
        with open(file_name,"wb") as oq:
            spamwriter = csv.writer(oq, delimiter=',')
            title = ["name","postDate","currentValue","previousValue","dayChange","changePercent","zscore30","zscore90"]
            spamwriter.writerow(title)
            for r in rs:
                spamwriter.writerow(r)

def main():
    conn = lite.connect("/home/vic/workspace/data/embers_ar.db")
    stock_list = ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC','AEX','AS51','CAC','CCMP','DAX','FTSEMIB','HSI','IBEX','INDU','NKY','OMX','SMI','SPTSX','SX5E','UKX']
    out_put(stock_list,conn)

if __name__ == "__main__":
    main()    