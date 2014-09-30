import fdb
import fdb.fbcore
from ello.ini import ini

database = ini.read('Dados', 'DataBase')
con = fdb.connect(database, 'sysdba', 'masterkey')
cursor = con.cursor()

chain = []

class Field(object):

    def __str__(self):
        nullable = 'NOT NULL' if self.nullable else ''
        default_value = self.default_value if self.default_value else ''

        return "%s %s %s %s" % (self.name, self.type_, default_value, nullable)

class Table:

    _field_types = {
        'LONG': 'INTEGER',
        'VARYING': 'VARCHAR',
        'INT64': 'DECIMAL',
        'DATE': 'DATE',
        'SHORT': 'SMALLINT',
        'TEXT': 'CHAR'
    }

    def __init__(self, db, name):
        self.fields = []
        cursor = db.cursor()
        cursor.execute("""
        select
            rel.rdb$field_id,
            rel.rdb$field_position AS field_position,
            rel.rdb$field_name as field_name,
            types.rdb$type_name as field_type,
            fields.rdb$character_length,
            fields.rdb$field_precision,
            fields.rdb$field_scale,
            -- fields.rdb$field_length,
            -- fields.rdb$field_type,
            -- fields.rdb$field_sub_type,
            rel.rdb$default_source as default_value,
            rel.rdb$null_flag as null_flag
        from rdb$relation_fields rel
        left join rdb$fields fields on (rel.rdb$field_source=fields.rdb$field_name)
        left join rdb$types types on (fields.rdb$field_type=types.rdb$type and types.rdb$field_name='RDB$FIELD_TYPE')
        where
            rdb$relation_name='%s'
        order by rdb$field_position
        """ % name)
        for field_id, position, name, ftype, length, precision, scale, default, null in cursor:
            type_name = self._field_types[ftype.strip()]
            if type_name in ['VARCHAR', 'CHAR']:
                type_name = "%s(%d)" % (type_name, length)
            elif type_name in ['DECIMAL']:
                type_name = "%s(%d, %d)" % (type_name, precision, abs(scale))

            field = Field()
            field.position = position
            field.name = name.strip()
            field.type_ = type_name
            field.default_value = default
            field.nullable = null

            self.fields.append(field)

def dropObject(name, ttype):
    pname = name.strip()
    object_types = { 1 : 'VIEW', 2 : 'TRIGGER', 5 : 'PROCEDURE' }
    print "  "*len(chain), 'DROP', object_types[ttype], pname
    sql = 'DROP %s %s' % (object_types[ttype], pname)
    cursor.execute(sql)
    try:
        con.commit()
        
        # gravar script
        f = open('d:/script.sql', 'a')
        f.write('%s;\n' % sql)
        f.close()

        return True
    except fdb.fbcore.DatabaseError, e:
        con.rollback()
        chain.append(pname)
        for dname, dtype in getDependencies(pname):
            dname = dname.strip()
            if dname==pname: continue
            if dname in chain: continue
            if dropObject(dname, dtype) and len(chain)>0:
                chain.pop()

def getDependencies(object_name):
    """ Retorna a lista de dependencias de um objeto
    """
    sql = ("select a.RDB$DEPENDENT_NAME, a.RDB$DEPENDENT_TYPE "
           "FROM RDB$DEPENDENCIES a                           "
           "WHERE                                             "
           "    a.RDB$DEPENDED_ON_TYPE IN (1,2,5)             "
           "    AND a.RDB$DEPENDED_ON_NAME='%s'               " % object_name)
    cursor.execute(sql)
    dependencies = cursor.fetchall()
    return dependencies


def procedures():
    """ Retorna a lista de procedures no banco de dados
    """
    cursor.execute('SELECT a.RDB$PROCEDURE_NAME FROM RDB$PROCEDURES a')
    return cursor.fetchall()

def triggers():
    cursor.execute("""\
SELECT 
    a.RDB$TRIGGER_NAME 
FROM RDB$TRIGGERS a
WHERE RDB$SYSTEM_FLAG=0
""")
    return cursor.fetchall()

def views():
    sql = """SELECT 
        a.RDB$RELATION_ID, 
        a.RDB$FIELD_ID, 
        a.RDB$RELATION_NAME
    FROM RDB$RELATIONS a
    WHERE
        RDB$SYSTEM_FLAG=0
        and ((RDB$RELATION_TYPE=1) 
            or ((RDB$RELATION_TYPE IS NULL) AND (RDB$RELATION_NAME LIKE 'V%')))
        """
    cursor.execute(sql)
    views = cursor.fetchall()
    views.append((0, 0, 'VSPDCONTADOR')) # estagiario aprova!
    return views

def deleteForeignKeys():
    cursor.execute( "select r.rdb$relation_name, r.rdb$constraint_name "
                    "from rdb$relation_constraints r "
                    "where (r.rdb$constraint_type='FOREIGN KEY') ")
    foreign_keys = cursor.fetchall()
    sql = "alter table %s drop constraint %s;"
    for table_name, constraint_name in foreign_keys:
        instruction = sql % (table_name.strip(), constraint_name.strip())
        print instruction
        cursor.execute(instruction)
    con.commit()

if __name__=="__main__":
    t = Table(con, 'TESTCARGA')
    for f in t.fields:
        print f
