import sys
import fdb
import firebird
import optparse

def store_func(filep):
    def store_statement(sql_stmt):
        print >>filep, sql_stmt
    return store_statement

def main(database_path):
    conn = fdb.connect(database_path, 'sysdba', 'masterkey')
    cursor = conn.cursor()
    database = firebird.Database(conn)

    f = open('saida.sql', 'w')
    store_stmt = store_func(f)
    database.save_instruction_func = store_stmt

    database.dropAllTriggers()
    database.dropAllViews()
    database.dropAllProcedures()

    f.close()

if __name__=="__main__":
    parser = optparse.OptionParser(usage="%prog firebird_database_file")
    (opts, args) = parser.parse_args()
    if not args:
        parser.print_help()
        sys.exit(1)
    main(args[0])
