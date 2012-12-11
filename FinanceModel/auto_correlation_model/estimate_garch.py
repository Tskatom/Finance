import sqlite3 as lite
from Util import calculator
import math
import argparse
from scipy import stats

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

def estimate(conn,t_index,p_date,l_mu,l_sigma):
    #get the range from which will trigger the sigma event
    table_name = "t_enriched_bloomberg_prices"
    cur = conn.cursor()
    querySql = "select one_day_change from {} where name='{}' and post_date <'{}' order by post_date desc limit 30 ".format(table_name,t_index,p_date)
    cur.execute(querySql)
    rows = cur.fetchall()
    moving30 = []
    for row in rows:
        moving30.append(row[0])
    
    querySql = "select one_day_change from {} where name='{}' and post_date <'{}' order by post_date desc limit 90 ".format(table_name,t_index,p_date)
    cur.execute(querySql)
    rows = cur.fetchall()
    moving90 = []
    for row in rows:
        moving90.append(row[0])

    querySql  = "select current_value from {} where name='{}' and post_date <'{}' order by post_date desc limit 1 ".format(table_name,t_index,p_date)
    cur.execute(querySql)
    rows = cur.fetchall()
    previous_value = 0.0
    for row in rows:
        previous_value = float(row[0])
        
       
    m30 = sum(moving30)/len(moving30)
    m90 = sum(moving90)/len(moving90)
    std30 = calculator.calSD(moving30)
    std90 = calculator.calSD(moving90)
    
    s4Bottom = m30 - 4*std30
    s4Upper = m30 + 4*std30
    s3Bottom = m90 - 3*std90
    s3Upper = m90 + 3*std90
    
    bottom = s4Bottom
    upper = s4Upper
    if s4Bottom >= s3Bottom:
        bottom = s3Bottom
    if s3Upper <= s4Upper:
        upper = s3Upper
    
    #get the log range
    bottom = math.log((bottom+previous_value)/previous_value)
    upper = math.log((upper+previous_value)/previous_value)
    
    n_dis = stats.norm(l_mu,l_sigma)
    #compute the probility of negative sigama event(0412)
    p_0412 = n_dis.cdf(bottom)
    #compute the probility of positive sigama event(0411)
    p_0411 = 1 - n_dis.cdf(upper)
    
        
    "Insert into the prediction model"
#    sql = "insert into t_ar_prediction (post_date,stock_index,zscore30,zscore90,change_percent,price,event_type,ord) values (?,?,?,?,?,?,?,?)"
#    cur.execute(sql,(p_date,t_index,zscore30,zscore90,p_l,p_price,event_type,99))
    sql = "insert into t_ar_garch_prediction(post_date,name,type,p_mu,p_sigma,p_0411,p_0412,ord) values (?,?,?,?,?,?,?,?)"
    cur.execute(sql,(p_date,t_index,"Stock",l_mu,l_sigma,p_0411,p_0412,99))
                
def arg_parser():
    ap = argparse.ArgumentParser("The auto_correlation model")
    ap.add_argument('-db',dest='db_file',metavar='DATA BASE',type=str,help='The Database path')
    ap.add_argument('-f',dest='p_file',metavar='PREDICTION FILE',type=str,help='Prediction file')
    return ap.parse_args()

def main():
    args = arg_parser()
    db_file = args.db_file
    p_file = args.p_file
    
    conn = lite.connect(db_file)
    p_f = open(p_file,'r')
    lines = p_f.readlines()[1:]
    for line in lines:
        line = line.strip().replace("\r","").replace("\n","").replace("\"","")
        info = line.split("\t")
        p_date = info[1]
        t_index = info[2]
        l_mu = float(info[3])
        l_sigma = float(info[4])
        estimate(conn,t_index,p_date,l_mu,l_sigma)
    
    if conn:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    main()        
    
    