import json
import string
import datetime
import time
"""
This program is used to group the articles by clusters
"""

articles = json.load(open("d:/embers/financemodel/bloombergNewsGroupByStock.json"))
finalStockClusterNews = {}
"Iterately read the news"
for index in articles:
    indexNews = articles[index]
    dayNews = {}
    #group the news by date
    for articleId in indexNews:
        day = articleId[0:8]
        if day not in dayNews:
            dayNews[day] = []
        dayNews[day].append(indexNews[articleId])
    
    print dayNews

    #read the day cluster file to group the date
    clusterDays = {}
    trendFile = open("d:/embers/financeModel/output/TrainingSetRecords.json")
    trendJson = json.load(trendFile)
    for trend in trendJson:
        if index == trend[6]:
            cluster = trend[7]
            structDate = time.strptime(trend[2],"%Y-%m-%d")
            dtDay = datetime.datetime(structDate[0],structDate[1],structDate[2])
            for i in range(1,4):
                day = dtDay - datetime.timedelta(days=i)
                dayStr = day.strftime("%Y%m%d")
                if cluster not in clusterDays:
                    clusterDays[cluster] = []
                if dayStr not in clusterDays[cluster]:
                    clusterDays[cluster].append(dayStr)

    print clusterDays

    clusterNews = []
    for cluster in clusterDays:
        cNews = {}
        cNews["cluster"] = cluster;
        docs = []
        for day in clusterDays[cluster]:
            if day in dayNews:
                for doc in dayNews[day]:
                    docs.append(doc)
        cNews["articles"] =  docs;
        clusterNews.append(cNews)
    finalStockClusterNews[index] = clusterNews
    
with open("d:/embers/financeModel/TrainingSet.txt","w") as output:
    output.write(json.dumps(finalStockClusterNews))     
            
    
    
    
    
    