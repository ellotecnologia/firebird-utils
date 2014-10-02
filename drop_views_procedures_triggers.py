import sys
import fdb
import firebird

def main(database_path):
    conn = fdb.connect(database_path, 'sysdba', 'masterkey')
    cursor = conn.cursor()
    database = firebird.Database(conn)

    database.dropAllTriggers()
    database.dropAllViews()
    database.dropAllProcedures()

if __name__=="__main__":
    main(sys.argv[1])
