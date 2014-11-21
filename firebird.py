import fdb.fbcore

class Database(object):

    def __init__(self, dbconn):
        self.dbconn = dbconn
        self.cursor = dbconn.cursor()

    def dropAllProcedures(self):
        for record in self.procedures():
            self.dropObject(record[0], 5)

    def dropAllTriggers(self):
        for record in self.triggers():
            self.dropObject(record[0], 2)

    def dropAllViews(self):
        for record in self.views():
            self.dropObject(record[2], 1)

    def tables(self):
        """ Returns a list of available tables.
        """
        self.cursor.execute("SELECT rdb$relation_name "
                            "FROM rdb$relations "
                            "WHERE "
                            "   rdb$view_blr is null "
                            "   AND (rdb$system_flag IS NULL OR rdb$system_flag=0) "
                            "ORDER by rdb$relation_name")
        for tablename, in self.cursor:
            yield Table(self.dbconn, tablename)

    def procedures(self):
        """ Retorna a lista de procedures no banco de dados
        """
        self.cursor.execute('SELECT a.RDB$PROCEDURE_NAME '
                            'FROM RDB$PROCEDURES a')
        return self.cursor.fetchall()

    def triggers(self):
        """ Retorna a lista de triggers no banco de dados
        """
        self.cursor.execute("SELECT a.RDB$TRIGGER_NAME "
                            "FROM RDB$TRIGGERS a "
                            "WHERE RDB$SYSTEM_FLAG=0")
        return self.cursor.fetchall()

    def views(self):
        """ Retorna a lista de views no banco de dados
        """
        self.cursor.execute("SELECT a.RDB$RELATION_ID, a.RDB$FIELD_ID, a.RDB$RELATION_NAME "
                            "FROM RDB$RELATIONS a "
                            "WHERE "
                            "    RDB$SYSTEM_FLAG=0"
                            "    and ((RDB$RELATION_TYPE=1) "
                            "    or ((RDB$RELATION_TYPE IS NULL) AND (RDB$RELATION_NAME LIKE 'V%')))")
        views = self.cursor.fetchall()
        return views

    def dropObject(self, name, ttype):
        object_name = name.strip()
        object_types = { 1 : 'VIEW', 2 : 'TRIGGER', 5 : 'PROCEDURE' }
        for dname, dtype in self.getDependencies(object_name):
            dname = dname.strip()
            if dname==object_name: continue
            self.dropObject(dname, dtype) 

        sql = 'DROP {0} {1}'.format(object_types[ttype], object_name)
        try:
            self.cursor.execute(sql)
            self.dbconn.commit()
        except fdb.fbcore.DatabaseError, e:
            self.dbconn.rollback()
            return False
        print "-> {0}".format(sql)
        self.do_save_instruction("{0};".format(sql))
        return True

    def do_save_instruction(self, stmt):
        if self.save_instruction_func:
            self.save_instruction_func(stmt)

    def getDependencies(self, object_name):
        """ Retorna a lista de dependencias de um objeto
        """
        sql = ("select a.RDB$DEPENDENT_NAME, a.RDB$DEPENDENT_TYPE "
               "FROM RDB$DEPENDENCIES a                           "
               "WHERE                                             "
               "    a.RDB$DEPENDED_ON_TYPE IN (1,2,5)             "
               "    AND a.RDB$DEPENDED_ON_NAME='%s'               " % object_name)
        self.cursor.execute(sql)
        dependencies = self.cursor.fetchall()
        return dependencies


class Field(object):

    def __str__(self):
        nullable = 'NOT NULL' if self.nullable else ''
        default_value = self.default_value if self.default_value else ''

        return "%s %s %s %s" % (self.name, self.type_, default_value, nullable)

class Table:

    _field_types = {
        'SHORT'     : 'SMALLINT',
        'LONG'      : 'INTEGER',
        'INT64'     : 'DECIMAL',
        'FLOAT'     : 'FLOAT',
        'VARYING'   : 'VARCHAR',
        'DATE'      : 'DATE',
        'TEXT'      : 'CHAR',
        'BLOB'      : 'BLOB',
        'TIME'      : 'TIME',
        'TIMESTAMP' : 'TIMESTAMP'
    }

    def __init__(self, db, name):
        self.name = name.strip()
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
            field.position = position+1 # firebird position is 1-based
            field.name = name.strip()
            field.type_ = type_name
            field.default_value = default
            field.nullable = null

            self.fields.append(field)

    def __str__(self):
        return "<firebird.Table: {0}>".format(self.name)

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
