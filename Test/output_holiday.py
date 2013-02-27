import sqlite3 as lite

#conn = lite.connect('d:/embers/financemodel/embers_v2.db')
#cur = conn.cursor()
#sql = "select country,date,holiday_name from s_holiday"
#cur.execute(sql)
#rs = cur.fetchall()
#holidays = {}
#
#conn2 = lite.connect("../Config/data/embers_v2.db")
#cur2 = conn2.cursor()
#sql2 = "insert into s_holiday (country,date,holiday_name) values (?,?,?)"
#
#for r in rs:
#    country = r[0]
#    if country not in holidays:
#        holidays[country] = {}
#    post_date = r[1]
#    holiday_name = r[2]
#    holidays[country][post_date] = holiday_name
#    
#    cur2.execute(sql2,[country,post_date,holiday_name])
#
#conn2.commit()

import sqlite3 as lite

conn = lite.connect('d:/embers/financemodel/embers_v2.db')
cur = conn.cursor()
sql = "select stock_index,country from s_stock_country"
cur.execute(sql)
rs = cur.fetchall()

conn2 = lite.connect("../Config/data/embers_v2.db")
cur2 = conn2.cursor()
sql2 = "insert into s_stock_country (stock_index,country) values (?,?)"

for r in rs:
    stock = r[0]
    country = r[1]
    
    cur2.execute(sql2,[stock,country])

conn2.commit()



if conn:
    conn.close()

if conn2:
    conn2.close()

