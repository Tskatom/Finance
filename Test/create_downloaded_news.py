import sqlite3 as lite
import json
con = lite.connect('../Config/Data/embers_v2.db')
cur = con.cursor()
sql = "select title from t_daily_news"
cur.execute(sql)
result = cur.fetchall()
newsTitles = []
for re in result:
    newsTitles.append(re[0])
with open("d:/do.json","w") as out:
    out.write(json.dumps(newsTitles))
    
