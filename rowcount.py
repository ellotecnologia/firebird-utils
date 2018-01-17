import sys
import fdb

con = fdb.connect(dsn=sys.argv[1])
cur = con.cursor()

cur.execute("""\
select rdb$relation_name
from rdb$relations
where rdb$view_blr is null 
and (rdb$system_flag is null or rdb$system_flag = 0)
""")

tabelas = {}

for tablename, in cur.fetchall(): 
    cur.execute('select count(*) from %s' % tablename)

    try:
        count = cur.fetchone()[0]
    except:
        print '--> %s' % tablename

    if count == 0: continue

    #print tablename, count
    tabelas[count] = tablename

keys = tabelas.keys()
keys.sort()

for k in keys:
    print tabelas[k], k

