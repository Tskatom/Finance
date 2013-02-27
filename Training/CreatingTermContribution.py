from __future__ import division
import json
from datetime import datetime,timedelta
import time
from Util import common
import cProfile
"""
This script used to calculate the contribution of each term to the individual clusters
"""
def group_by_index(t_file):
    stock_news = {}
    for line in open(t_file,"r"):
        news = json.loads(line)
        stock = news["stockIndex"]
        post_date = news["postDate"]
        if stock not in stock_news:
            stock_news[stock] = {}
        if post_date not in stock_news[stock]:
            stock_news[stock][post_date] = []
        stock_news[stock][post_date].append(news)
    return stock_news

def group_news_by_cluster(days_back):
    "Load the Traing news File"
    trainingNewsFile = common.get_configuration("training", "TRAINING_NEWS_FILE")
    articles = group_by_index(trainingNewsFile)
    
    finalStockClusterNews = {}
    "Iterately read the news"
    for index in articles:
        indexNews = articles[index]
    
        #read the day cluster file to group the date
        clusterDays = {}
        trendFilePath = common.get_configuration("training", "TRAINING_TREND_RECORDS")
        trendFile = open(trendFilePath)
        trendJson = json.load(trendFile)
        for trend in trendJson:
            if index == trend[6]:
                cluster = trend[7]
                structDate = time.strptime(trend[1],"%Y-%m-%d")
                dtDay = datetime(structDate[0],structDate[1],structDate[2])
                for i in range(1,days_back+1):
                    day = dtDay - timedelta(days=i)
                    dayStr = day.strftime("%Y-%m-%d")
                    if cluster not in clusterDays:
                        clusterDays[cluster] = []
                    if dayStr not in clusterDays[cluster]:
                        clusterDays[cluster].append(dayStr)
    
        clusterNews = []
        for cluster in clusterDays:
            cNews = {}
            cNews["cluster"] = cluster;
            docs = []
            for day in clusterDays[cluster]:
                if day in indexNews:
                    for doc in indexNews[day]:
                        docs.append(doc)
            cNews["articles"] =  docs;
            clusterNews.append(cNews)
        finalStockClusterNews[index] = clusterNews
    
    trendFile.close()
    with open("D:/groupByCluster.json","w") as ot:
        ot.write(json.dumps(finalStockClusterNews))
    return finalStockClusterNews

def compute_term_contribution(days_back):
    "Read the Vocabulary File"
    vocabularyFilePath = common.get_configuration("training", "VOCABULARY_FILE")
    vocaList = json.load(open(vocabularyFilePath,"r"))
    
    print "StartTime: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    finalWordContribution = {}
    "Iteratively to access each Stock Index"
    trainingFile = group_news_by_cluster(days_back)
    
    print "Finish Group news by cluster"
    
    for index in trainingFile:
        stockNews = trainingFile[index]
        wordContribution = {}
        for cluster in stockNews:
            #computing the words count in each cluster
            print "Start Cluster ", cluster["cluster"], "For Stock ",index, "at ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
            articles = cluster["articles"]
            #initiate the wordFreq
            wordFreq = {}
            for term in vocaList:
                wordFreq[term] = 0
            for article in articles:
                fdist = article["content"]
                for term in wordFreq:
                    if term in fdist:
                        wordFreq[term] = wordFreq[term] + fdist[term]
            #computing the word contribution
            count = sum(wordFreq.values())
            contributions = {}
            for term in wordFreq:
                contribution = round(1.0*(wordFreq[term]+1)/(count + len(wordFreq)),4)
                contributions[term] = contribution
            
            # add the contributions to each cluster
            wordContribution[cluster["cluster"]] = contributions
            print "Finish Cluster ", cluster["cluster"], "For Stock ",index, "at ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
        finalWordContribution[index] = wordContribution    
    print "EndTime: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    
    "Write the Term Contribution To File"
    termContributionFile = common.get_configuration("model", "TERM_CONTRIBUTION_PATH")
    jsString = json.dumps(finalWordContribution)
    with open(termContributionFile,"w") as output:
        output.write(jsString)

if __name__=="__main__":
    print "group_news_by_cluster Start Time : ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    cProfile.run("compute_term_contribution()")
    print "group_news_by_cluster End Time : ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    