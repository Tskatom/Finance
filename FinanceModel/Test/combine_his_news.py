import os
import json

newsList = []
files = os.listdir('../Config/Data/DailyNews')
for file in files:
    print file
    fileName = '../Config/Data/DailyNews/'+file
    newsObj = json.load(open(fileName))
    for stock in newsObj:
        for news in newsObj[stock]:
            try:
                x = news["embersId"]
                y = news["stockIndex"]
                z = news["postTime"]
                z = news["postDate"]
                newsList.append(news)
            except:
                print news

print len(newsList)
with open("d:/embers/combine_news.json","w") as out:
    for news in newsList:
        out.write(json.dumps(news))
        out.write("\n")     