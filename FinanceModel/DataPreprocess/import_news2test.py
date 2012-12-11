import sqlite3 as lite

def import_news(db1,db2):
    cur1 = db1.cursor()
    cur2 = db2.cursor()
    
    sql = "select embers_id,derived_from,title,author,post_time,post_date,content,stock_index,source,raw_update_time,update_time\
           from t_daily_enrichednews where post_date >'2012-10-02' order by post_date"
    
    cur1.execute(sql)
    rs = cur1.fetchall()
    iSql = "insert into t_daily_enrichednews(embers_id,derived_from,title,author,post_time,post_date,content,stock_index,source,raw_update_time,update_time) values(?,?,?,?,?,?,?,?,?,?,?)"
    
    for r in rs:
        cur2.execute(iSql,(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[10],))
    
    db2.commit()
    
def import_prices(db1,db2):
    cur1 = db1.cursor()
    cur2 = db2.cursor()
    
    sql = "select embers_id,type,name,update_time,current_value,query_time,previous_close_value,post_date,source from t_bloomberg_prices\
          where post_date>'2012-10-16' order by post_date asc"
    cur1.execute(sql)
    rs = cur1.fetchall()
    iSql = "insert into t_bloomberg_prices (embers_id,type,name,update_time,current_value,query_time,previous_close_value,post_date,source) values (?,?,?,?,?,?,?,?,?)"
    for r in rs:
        cur2.execute(iSql,(r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],))
    
    db2.commit()
    
def main():
    db1 = lite.connect("d:/embers/financeModel/embers_GSR.db")
    db2 = lite.connect("../Config/data/embers_v2.db")
    
    import_prices(db1,db2)

if __name__ == "__main__":
    main()