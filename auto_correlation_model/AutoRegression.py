import matplotlib.pyplot as plt
import numpy as np
from Util import common
import os
from scipy.stats.stats import pearsonr
import json


def main():
    #get stock index values
    cfgPath = "../Config/config.cfg"
    common.init(cfgPath)
    
    con = common.getDBConnection()
    cur = con.cursor()
    sql = "select stock_index,date,zscore30,zscore90,one_day_change/(last_price-one_day_change) from t_daily_stockindex where date<'2011-01-01' order by date asc"
    cur.execute(sql)
    rows = cur.fetchall()
    stockBasket = {}
    tmpStockBasket = {}
    for row in rows:
        stockIndex = row[0]
        date = row[1]
        zscore30 = row[2]
        zscore90 = row[3]
        lastPrice = row[4]
        if stockIndex not in stockBasket:
            stockBasket[stockIndex] = []
            tmpStockBasket[stockIndex] = {}
        dayValue = {}
        dayValue["date"] = date
        dayValue["zscore30"] = zscore30
        dayValue["zscore90"] = zscore90
        dayValue["lastPrice"] = lastPrice
        stockBasket[stockIndex].append(dayValue)
        tmpStockBasket[stockIndex][date] = [zscore30,zscore90,lastPrice]
    
    con.close()
    
    stockList = []
    #compute the pearson for each two stock
    for stockIndex in stockBasket:
        stockList.append(stockIndex)
    print stockList  
    
    for i in range(0,9):
        for j in range(i+1,9):
            print stockList[i],stockList[j]
            stock1 = tmpStockBasket[stockList[i]]
            stock2 = tmpStockBasket[stockList[j]]
            compare_stock(stock1,stock2)
                     
    for i in range(0,9):
        for j in range(0,9):
            print stockList[i],stockList[j]
            stock1 = stockBasket[stockList[i]]
            stock2 = stockBasket[stockList[j]]
            compare_stock_shift(stockList[i],stockList[j],stock1,stock2,0)

def compare_stock_shift(stockN1,stockN2,stock1,stock2,duration):
    newStock1 = {}
    newStock2 = {}
    combine = []
    for dayValue in stock1:
        newStock1[dayValue["date"]] =  dayValue
    for dayValue in stock2:
        newStock2[dayValue["date"]] =  dayValue
    for date in newStock1:
        if date in newStock2:
            element = {}
            element["date"] = date
            element["stock1_z30"] = newStock1[date]["zscore30"]
            element["stock1_z90"] = newStock1[date]["zscore90"]
            element["stock1_price"] = newStock1[date]["lastPrice"]
            element["stock2_z30"] = newStock2[date]["zscore30"]
            element["stock2_z90"] = newStock2[date]["zscore90"]
            element["stock2_price"] = newStock2[date]["lastPrice"]
            combine.append(element)
    print "______________________________________________"
    combine.sort(key = lambda x:x['date'])
    stock1Z30 = [ele["stock1_z30"] for ele in combine]
    stock1Z90 = [ele["stock1_z90"] for ele in combine]
    stock1Price = [ele["stock1_price"] for ele in combine]
    stock2Z30 = [ele["stock2_z30"] for ele in combine]
    stock2Z90 = [ele["stock2_z90"] for ele in combine]
    stock2Price = [ele["stock2_price"] for ele in combine]
    
    #moving the duration
    stock1Z30 = stock1Z30[0:len(stock1Z30) - duration]
    stock2Z30 = stock2Z30[duration:]
    stock1Z90 = stock1Z90[0:len(stock1Z90) - duration]
    stock2Z90 = stock2Z90[duration:]
    stock1Price = stock1Price[0:len(stock1Price) - duration]
    stock2Price = stock2Price[duration:]
    
    print pearsonr(stock1Z30,stock2Z30)
    print pearsonr(stock1Z90,stock2Z90)
    print pearsonr(stock1Price,stock2Price)
    
    z30m, z30c = least_square_fit(stock1Z30,stock2Z30)
    z90m, z90c = least_square_fit(stock1Z90,stock2Z90) 
    print "Zscore30 Least Square results:",z30m, z30c
    print "Zscore90 Least Square results:",z90m, z90c  
    if stockN1!=stockN2 or duration != 0:
        testPhase(stockN1,stockN2,z30m, z30c,z90m, z90c)

def least_square_fit(indepValues,depValues):
    x = np.array(indepValues)
    y = np.array(depValues)
    A = np.vstack([x,np.ones(len(x))]).T
    m,c = np.linalg.lstsq(A, y)[0]
    return m,c    

def compare_stock(stock1,stock2):
    stock1Z30 = []
    stock1Z90 = []
    stock2Z30 = []
    stock2Z90 = []
    stock1Price = []
    stock2Price = []
    for date in stock1:
        if date in stock2:
            stock1Z30.append(stock1[date][0])
            stock1Z90.append(stock1[date][1])
            stock1Price.append(stock1[date][2])
            stock2Z30.append(stock2[date][0])
            stock2Z90.append(stock2[date][1])
            stock2Price.append(stock2[date][2])
    print len(stock1Z30)
    print pearsonr(stock1Z30,stock2Z30)
    print pearsonr(stock1Z90,stock2Z90)
    print pearsonr(stock1Price,stock2Price)

def testPhase(stock1,stock2,z30m,z30c,z90m,z90c):
    cfgPath = "../Config/config.cfg"
    common.init(cfgPath)
    
    con = common.getDBConnection()
    cur = con.cursor()
    sql = "select stock_index,date,zscore30,zscore90,one_day_change/(last_price-one_day_change) from t_daily_stockindex where date>='2011-01-01' and stock_index=? order by date asc"
    cur.execute(sql,(stock1,))
    rows = cur.fetchall()
    for row in rows:
        date = row[1]
        zscore30 = float(row[2])
        zscore90 = float(row[3])
        predictZ30 = z30m * zscore30 + z30c
        predictZ90 = z90m * zscore90 + z90c
        
        
        if abs(predictZ30) >= 4 or abs(predictZ90) >= 3:
            print "{} : using {} to predict {} z30: {} z90: {}".format(date,stock1,stock2,predictZ30,predictZ90)
    con.close()     
                  
        
if __name__ == "__main__":
    main()
