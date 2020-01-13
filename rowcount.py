import sys
import fdb

con = fdb.connect(dsn=sys.argv[1])
cur = con.cursor()

tabelas = {}

for table in con.schema.tables: 
    tablename = table.name
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

