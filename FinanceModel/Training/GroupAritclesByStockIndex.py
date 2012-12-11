import json
from datetime import datetime
from Util import common
import os
import re
import cProfile

def create_match_rule(rule_name):
    comListFile = common.get_configuration("training", rule_name)
    comList = json.load(open(comListFile))
    rule = "("
    for stock in comList:
        for company in comList[stock]:
            company.replace("\\.","\\\\.")
            "check If the country name only contain one word, then we will add blank before and after the name to avoid the sub matching"
            if company.find(" ") < 0:
                eachRule = " " + company + " " + "|"
            else:
                eachRule = company + "|"
            rule += eachRule
    rule = rule[0:len(rule)-1] + ")"
    return rule.lower()

#def create_rule_by_country():
#    countryListFile = common.get_configuration("training","COUNTRY_LIST")
#    countryList = json.load(open(countryListFile))
#    rule = "("
#    for stock in countryList:
#        for country in countryList[stock]:
#            "check If the country name only contain one word, then we will add blank before and after the name to avoid the sub matching"
#            eachRule = ""
#            if country.find(" ") < 0:
#                eachRule = " " + country + " " + "|"
#            else:
#                eachRule = country + "|"
#            rule += eachRule
#    rule = rule[0:len(rule)-1] + ")"
#    return rule.lower()

def group_daily_articles(rule_name):
    
    stockArticles = {}
    
    archiveDir = common.get_configuration("training", "ARCHIVE_NEWS_DIR")
    dailyFileNames = os.listdir(archiveDir)
    matchRule = create_match_rule(rule_name)
    pattern = re.compile(matchRule,re.I)
    print matchRule
    "Construct company-stock object"
    comListFile = common.get_configuration("training", rule_name)
    comList = json.load(open(comListFile))
    comStock = {}
    for stock in comList:
        for company in comList[stock]:
            comStock[company.strip().lower()] = stock
            
    i = 0
    print "StartTime: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    for dailyFile in dailyFileNames:
        dailyNews = json.load(open(archiveDir+ "/" + dailyFile),encoding='ISO-8859-1')
        for news in dailyNews:
            content = news["content"].lower()
#            print content
            matchedList = pattern.findall(content)
            matchedGroup = []
            if matchedList:
                i = i + 1
                for item in matchedList:
                    matchedGroup.append(item)
            matchedGroup = {}.fromkeys(matchedGroup).keys()
            
            "Group the news to matched stock"
            for item in matchedGroup:
                item = item.strip()
                if item in comStock:
                    stockIndex = comStock[item]
                    if stockIndex not in stockArticles:
                        stockArticles[stockIndex] = {}
                    articleId = news["articleId"]
                    stockArticles[stockIndex][articleId] = news
    print i
    
    "Write the grouped articles to file"
    groupedFile = common.get_configuration("training","GROUP_STOCK_NEWS")
    with open(groupedFile,"w") as output:
        output.write(json.dumps(stockArticles))
    print "EndTime: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")   
    
def test():
    rule_name = "COMPANY_LIST"
    group_daily_articles(rule_name)

if __name__ == "__main__":
    cProfile.run("test()")        


