import json
import numpy
import nltk
import sys
from Util import calculator
import sqlite3 as lite


news = open('D:/filterBloombergArray.json')
jsonNews = json.load(news,encoding='ISO-8859-1')

print calculator.calZscore([1,1,1,1,1],1)

clusterDis = json.load(open("d:/embers/financemodel/clusterDistribution.json"))
print clusterDis

result = open("d:/embers/financemodel/TestPredict.txt")
lines = result.readlines()
for line in lines:
#    print line
    tokens = [token.replace("\t","") for token in line.strip().split("\t")]
    date = tokens[1]
    pClusster = tokens[4]
    print date,pClusster


con = None
try:
    con = lite.connect("d:/sqlite/embers.db")
    cur = con.cursor()
    cur.execute('select sqlite_version()')
    data = cur.fetchone()
    print "Sqlite Version: %s" % data
    
except lite.Error, e:
    print "Error %s:" % e.args[0]
    sys.exit(1)
finally:
    if con:
        con.close()    