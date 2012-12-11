import sqlite3 as lite

conn = lite.connect('d:/embers/financemodel/embers_v2.db')
cur = conn.cursor()
sql = "select country,date,holiday_name from s_holiday"
cur.execute(sql)
rs = cur.fetchall()
holidays = {}
for r in rs:
    country = r[0]
    if country not in holidays:
        holidays[country] = {}
    post_date = r[1]
    holiday_name = r[2]
    holidays[country][post_date] = holiday_name

with open("d:\holidays.json","w") as out_q:
    out_q.write(str(holidays))
    


if conn:
    conn.close()