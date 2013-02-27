import sqlite3 as lite

conn = lite.connect('/home/qianzou/data/embers_ar.db')
cur = conn.cursor()
sql = "select post_date,current_value from t_enriched_bloomberg_prices where post_date>='2003-01-01' and name='MEXBOL' order by post_date asc"
cur.execute(sql)
rs = cur.fetchall()
mx = open('/home/qianzou/data/mexbol.txt','w')
mx.write('post_date\tcurrentvalue\n')
for r in rs:
    mx.write(r[0])
    mx.write("\t")
    mx.write(str(r[1]))
    mx.write("\n")

mx.close()
conn.close()