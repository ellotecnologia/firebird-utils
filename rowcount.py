import sys
import fdb

database = sys.argv[1]

con = fdb.connect(dsn=r'localhost:D:\dev\dados\%s.ello' % database, user='sysdba', password='masterkey')
cur = con.cursor()

cur.execute("""\
select rdb$relation_name
from rdb$relations
where rdb$view_blr is null 
and (rdb$system_flag is null or rdb$system_flag = 0)
""")

for tablename, in cur.fetchall(): 
    cur.execute('select count(*) from %s' % tablename)

    try:
        count = cur.fetchone()[0]
    except:
        print '--> %s' % tablename

    if count == 0: continue

    print tablename, count
