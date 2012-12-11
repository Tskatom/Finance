import sqlite3 as lite

db1 = lite.connect('../Config/Data/embers.db')
db2 = lite.connect('../Config/Data/embers_v2.db')

sql = "select country,holiday_name,date from s_holiday"

cur1 = db1.cursor()
cur1.execute(sql)
results = cur1.fetchall()

insertSql = "insert into s_holiday(country,holiday_name,date) values (?,?,?)"
cur2 = db2.cursor()
for result in results:
    cur2.execute(insertSql, (result[0],result[1],result[2],))

db2.commit()

sql = "select stock_index,country from s_stock_country"
cur1.execute(sql)
results = cur1.fetchall()


insertSql = "insert into s_stock_country(stock_index,country) values (?,?)"
cur2 = db2.cursor()
for result in results:
    cur2.execute(insertSql, (result[0],result[1],))

db2.commit()

if db1 and db2:
    db1.close()
    db2.close()