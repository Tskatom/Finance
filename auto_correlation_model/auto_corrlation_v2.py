import statsmodels.tsa.api as tsa
import numpy as np
import sqlite3 as lite
import argparse
from Util import calculator
import math
"""
One idea is that may be the current relation should have more reasonal correlation between two different data
So the training data may be better if we choose the better
"""
def arg_parser():
    ap = argparse.ArgumentParser("The auto_correlation model")
    ap.add_argument('-db',dest='db_file',metavar='DATA BASE',type=str,help='The Database path')
    ap.add_argument('-o',dest='order',metavar='ORDER',type=int,help='The order of VAR')
    return ap.parse_args()

def initiate_data(conn,start,end,target_indices):
    cur = conn.cursor()
    datas = {}
    for stock_index in target_indices:
        sql = "select name,post_date,current_value/previous_close_value from t_enriched_bloomberg_prices where post_date>='{}' and post_date<'{}' and name='{}' order by post_date asc".format(start,end,stock_index)
        cur.execute(sql)
        datas[stock_index] = {}
        results = cur.fetchall()
        for result in results:
            d_value = {"post_date":result[1],"change_percent":math.log(result[2]),"name":result[0]}
            datas[stock_index][result[1]] = d_value
    return  datas   

def get_cor_data(datas,t_index,v_indices):
    t_datas = datas[t_index]
    
    c_t_datas = []
    c_p_datas = []
    
    t_keys = t_datas.keys()
    common_days = []
    "Get the common dayList"
    for t_k in t_keys:
        flag = True
        for v_i in v_indices:
            d = datas[v_i]
            if t_k not in d:
                flag = False
                break
        if flag:
            common_days.append(t_k)
    
    "get the data array"
    for day in common_days:
        c_t_datas.append(t_datas[day])
        
    c_t_datas.sort(key = lambda x:x['post_date'])
    
    for v_i in v_indices:
        d = datas[v_i]
        d_v = []
        for day in common_days:
            d_v.append(d[day])
        
        d_v.sort(key = lambda x:x['post_date'])
        c_p_datas.append(d_v)
    
    return c_t_datas,c_p_datas

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

def fit_model(data,order):
    var_model = tsa.VAR(data)
    var_model_fit = var_model.fit(maxlags=order)
    return var_model_fit

def test_phase(training_start,test_start,test_end,t_index,v_indices,conn,order,target_indices):
    "Get the list of day to predict"
    cur = conn.cursor()
    date_list = []
    sql = "select post_date from t_enriched_bloomberg_prices where name='{}' and post_date>='{}' and post_date<='{}' order by post_date asc".format(t_index,test_start,test_end)
    cur.execute(sql)
    rs = cur.fetchall()
    for r in rs:
        date_list.append(r[0])
    
    "forcast the stock index day by day"
    for p_date in date_list:
        "For each day, we update the data set to contain the latest data"
        datas = initiate_data(conn,training_start,p_date,target_indices)
        c_t_datas,c_p_datas = get_cor_data(datas,t_index,v_indices)
        print "real_value: ",c_t_datas[-1]['change_percent']    
        w_d = []
        w_d.append([i['change_percent'] for i in c_t_datas])
        for da in c_p_datas:
            w_d.append([i['change_percent'] for i in da])
        
        "start to fit the model"
        data_matrix = np.array(w_d).T
        print data_matrix[-1]
        
        with open('/home/qianzou/data/mexbol_merval.txt',"w") as out_q:
            for d in data_matrix:
                out_q.write(str(d[0])+"\t"+str(d[1])+"\n")
        
        var_model_fit = fit_model(data_matrix[:],order)
        
        "Make predicton"
        prediction = forcast(var_model_fit,data_matrix,1)
        
        print "predict value: ",prediction[0]
        p_1 = math.exp(prediction[0])
        
        "compute the one_day_change"
        sql = "select current_value from t_enriched_bloomberg_prices where name=? and post_date<? order by post_date desc limit 1"
        cur.execute(sql,(t_index,p_date,))
        r = cur.fetchone()
        last_price = r[0]
        p_1_change = last_price * (p_1 - 1)
        p_price = last_price * (p_1)
        
        "compute the predicting zscore"
        zscore30 = getZscore(conn,p_date,t_index,p_1_change,30)
        zscore90 = getZscore(conn,p_date,t_index,p_1_change,90)
        event_type = "0000"
        
        if zscore30 >= 4 or zscore90 >= 3:
            event_type = "0411"
        elif zscore30 <= -4 or zscore90 <= -3:
            event_type = "0412"
        
        
        "Insert into the prediction model"
        sql = "insert into t_ar_prediction (post_date,stock_index,zscore30,zscore90,change_percent,price,event_type,ord) values (?,?,?,?,?,?,?,?)"
        cur.execute(sql,(p_date,t_index,zscore30,zscore90,p_1,p_price,event_type,order))
    conn.commit()
        
    
def forcast(var_model_fit,p_values,day):
    prediction = var_model_fit.forecast(p_values,day)
    return prediction[:][0]

def clear(conn):
    sql = "delete from t_ar_prediction"
    conn.cursor().execute(sql)
    conn.commit()

#def all_test_stage(start,end,conn,order,var_model_fit):
#    cur = conn.cursor()
#    ""
    
def main():
    args = arg_parser()
    db_file = args.db_file
#    m_order = args.order
    conn = lite.connect(db_file)
    
    "clear the prediction"
    clear(conn)
    
#    target_list = ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC','AEX','AS51','CAC','CCMP','DAX','FTSEMIB','HSI','IBEX','INDU','NKY','OMX','SMI','SPTSX','SX5E','UKX']
#    target_list = ['MERVAL','MEXBOL','CHILE65','AEX','INDU','NKY','OMX','SPTSX','SX5E','UKX']
    target_list = ['MEXBOL','MERVAL']
    training_start = '2003-01-01'
#    v_indices = ['AEX','AS51','CAC','CCMP','DAX','FTSEMIB','HSI','IBEX','INDU','NKY','OMX','SMI','SPTSX','SX5E','UKX']
    
    for order in range(1,2):
#        for t_index in ['MERVAL','MEXBOL','IBOV','CHILE65','COLCAP','CRSMBCT','BVPSBVPS','IGBVL','IBVC']:
        for t_index in ['MEXBOL']:   
            v_indices = [index for index in target_list if index != t_index]
            
            "Move to Test stage"
            t_start = "2011-01-02"
            t_end = "2012-10-31"
            test_phase(training_start,t_start,t_end,t_index,v_indices,conn,order,target_list)
        
    if conn:
        conn.close()
    

if __name__ == "__main__":
    main()

