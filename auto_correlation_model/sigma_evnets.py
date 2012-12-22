import sqlite3 as lite 
import argparse
import datetime
import json

def arg_parser():
    ap = argparse.ArgumentParser("find the cluster of the sigma events")
    
    ap.add_argument('-db',dest="db_path",metavar="DATABASE",type=str,help="The path of database")
    ap.add_argument('-du',dest="duration",metavar="DURATION",type=int,help="The duration of days to related")
    
    return ap.parse_args()

def find_relation(conn,predictor,duration,t_indices,direction):
    sql = "select post_date, name, zscore30 from t_enriched_bloomberg_prices where name='{}' and post_date>='2003-01-01' and (abs(zscore30)>=4 or abs(zscore90)>=3) ".format(predictor)
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    events = []
    for result in results:
        p_stock = result[1]
        p_post_date = result[0]
        zscore30 = result[2]
        event_type = ""
        if zscore30 > 0:
            event_type = "0411"
        else:
            event_type = "0412"
        
        p_event = {"p_stock":p_stock,"event_type":event_type,"post_date":p_post_date,"zscore30":zscore30,"duration":duration,}
        re_events = get_round_events(conn,p_event,duration,t_indices,direction)
        p_event["related_events"] = re_events
        events.append(p_event)
    return events
    

def get_round_events(conn,p_event,duration,t_indices,direction):
    
    min_date, max_date = get_day_range(p_event["post_date"],duration)
    tar_str = "("
    for t_index in t_indices:
        tar_str = tar_str + "'" +t_index + "',"
    tar_str = tar_str[:len(tar_str)-1] + ")"
    sql = ""
    if direction == 1:
        min_date = p_event["post_date"]
        sql = "select name, post_date,zscore30,zscore90 from t_enriched_bloomberg_prices where name in {} and post_date>'{}' and post_date <= '{}' and (abs(zscore30)>=4 or abs(zscore90)>=3)".format(tar_str,min_date,max_date)
    elif direction == -1:
        max_date = p_event["post_date"]
        sql = "select name, post_date,zscore30,zscore90 from t_enriched_bloomberg_prices where name in {} and post_date>='{}' and post_date < '{}' and (abs(zscore30)>=4 or abs(zscore90)>=3)".format(tar_str,min_date,max_date)
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    related_events = []
    for result in results:
        t_stock = result[0]
        t_post_date = result[1]
        zscore30 = result[2]
        event_type = ""
        
        if zscore30 > 0:
            event_type = "0411"
        else:
            event_type = "0412"
        
        t_event = {"t_stock":t_stock,"t_post_date":t_post_date,"event_type":event_type,"zscore30":zscore30}
        related_events.append(t_event)
    
    return related_events
        
    pass

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


def main():
    t_indices = ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC']
    p_indices = ['AEX','AS51','CAC','CCMP','DAX','FTSMIB','HSI','IBEX','INDU','NKY','OMX','SMI','SPTSX','SX5E','UKX']
    
    args = arg_parser()
    db_path = args.db_path
    duration = args.duration
    
    conn = lite.connect(db_path)
    t_list_fd = {}
    for p_index in p_indices:
        events = find_relation(conn,p_index,duration,t_indices,1)
        t_list_fd[p_index] = events
    
    "get the reverse relation"
    t_list_bd = {}
    for t_index in t_indices:
        events = find_relation(conn,t_index,duration,p_indices,-1)
        t_list_bd[t_index] = events
    
    "Write result to file"
    with open("d:/embers/fd_relation_results_7.json","w") as out_q:
        out_q.write(json.dumps(t_list_fd))
        
    "Write result to file"
    with open("d:/embers/bd_relation_results_7.json","w") as out_q:
        out_q.write(json.dumps(t_list_bd))

    "Summary the statistic"
    for (k,v) in t_list_bd.items():
        print k," : ", len(v)
            
    if conn:
        conn.close()

if __name__ == "__main__":
    main()        