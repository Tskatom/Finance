import sqlite3 as lite
import datetime

def import_gsr():
    conn = lite.connect('/home/qianzou/data/embers_ar.db')
    cur = conn.cursor()
    sql = "select name,post_date,zscore30,zscore90 from t_enriched_bloomberg_prices where post_date>='2011-01-01' and (abs(zscore30)>=4 or abs(zscore90)>=3)"
    cur.execute(sql)
    rs = cur.fetchall()
    
    iSql = "insert into t_gsr_events (stock_index,post_date,event_type,zscore30,zscore90) values (?,?,?,?,?)"
    
    for r in rs:
        stock_index = r[0]
        post_date = r[1]
        z30 = r[2]
        z90 = r[3]
        event_type = ""
        if z30 > 0:
            event_type = "0411"
        else:
            event_type = "0412"
            
        cur.execute(iSql,(stock_index,post_date,event_type,z30,z90,))
    
    conn.commit()
    conn.close()

def get_day_range(post_date,duration):
    max_date = datetime.datetime.strptime(post_date,"%Y-%m-%d")
    min_date = datetime.datetime.strptime(post_date,"%Y-%m-%d")
    du = duration
    
    while du > 0:
        max_date = max_date + datetime.timedelta(days = 1)
        week_day = max_date.weekday()
        if week_day == 5 or week_day == 6:
            continue
        du = du - 1
    
    du = duration
    while du > 0:
        min_date = min_date + datetime.timedelta(days = -1)
        week_day = max_date.weekday()
        if week_day == 5 or week_day == 6:
            continue
        du = du - 1
        
    return datetime.datetime.strftime(min_date,'%Y-%m-%d'),datetime.datetime.strftime(max_date,'%Y-%m-%d')


def compare_gsr_pd():
    conn = lite.connect('/home/qianzou/data/embers_ar.db')
    cur = conn.cursor()
    sql = "select stock_index, post_date, event_type from t_gsr_events"
    cur.execute(sql)
    rs = cur.fetchall()
    for r in rs:
        sql = "select count(*) from t_ar_prediction where stock_index = ? and post_date >= ? and post_date <= ? and event_type = ? and ord=4"
        stock_index = r[0]
        post_date = r[1]
        event_type = r[2]
        
        start, end = get_day_range(post_date,1)
        cur.execute(sql,(stock_index,start,post_date,event_type))
        r_c = cur.fetchone()
        count = int(r_c[0])
        if count > 0:
            print stock_index,post_date,event_type
    
    conn.close()

compare_gsr_pd()