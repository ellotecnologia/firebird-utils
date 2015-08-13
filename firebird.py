#coding: utf8
import fdb
import fdb.fbcore
import logging
import monkey_patch_fdb

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(message)s',
    datefmt= '%H:%M:%S')

class Database(object):

    def __init__(self, database_path, username='sysdba', password='masterkey'):
        self.db = fdb.connect(database_path, username, password)
        self.cursor = self.db.cursor()
        self.functions = self.db.schema.functions
        self.tables = self.db.schema.tables
        self.procedures = self.db.schema.procedures
        self.views = self.db.schema.views
        self.triggers = self.db.schema.triggers

    @property
    def foreign_keys(self):
        keys = {}
        for constraint in self.db.schema.constraints:
            if constraint.isfkey():
                keys[constraint.name] = constraint

        for k, constraint in keys.iteritems():
            yield constraint

    @property
    def indices(self):
        for index in self.db.schema.indices:
            if not index.isenforcer():
                yield index

    def drop_foreign_keys(self):
        """ Drop all database foreign keys
        """
        logging.info(u"Removendo Foreign Keys...")
        for constraint in self.foreign_keys:
            instruction = constraint.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_indices(self):
        """ Drop all databse indexes
        """
        logging.info(u"Removendo todos os índices...")
        for index in self.indices:
            instruction = "DROP INDEX {}".format(index.name)
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_triggers(self):
        logging.info(u"Removendo todas as triggers...")
        for trigger in self.triggers:
            instruction = trigger.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_views(self):
        logging.info(u"Removendo todas as views...")
        for view in self.views:
            self.drop_object_and_dependencies(view.name, 1)

    def drop_procedures(self):
        logging.info(u"Removendo todas as procedures...")
        for procedure in self.procedures:
            stmt = procedure.get_sql_for('drop')
            logging.debug(stmt)
            self.cursor.execute(stmt)
        self.db.commit()

    def drop_functions(self):
        logging.info(u"Removendo todas as functions...")
        for function in self.functions:
            stmt = function.get_sql_for('drop')
            logging.debug(stmt)
            self.cursor.execute(stmt)
        self.db.commit()

    def drop_object_and_dependencies(self, name, ttype):
        cursor = self.db.cursor()
        object_name = name.strip()
        object_types = { 1 : 'VIEW', 2 : 'TRIGGER', 5 : 'PROCEDURE', 99: 'EXTERNAL FUNCTION' }
        for dname, dtype in self._get_dependencies(object_name):
            dname = dname.strip()
            if dname==object_name: continue
            self.drop_object_and_dependencies(dname, dtype)

        sql = 'DROP {0} {1}'.format(object_types[ttype], object_name)
        try:
            cursor.execute(sql)
            self.db.commit()
        except fdb.fbcore.DatabaseError, e:
            self.db.rollback()
            return False
        logging.debug("-> {0}".format(sql))
        return True

    def _get_dependencies(self, object_name):
        """ Retorna a lista de dependencias de um objeto
        """
        sql = ("select a.RDB$DEPENDENT_NAME, a.RDB$DEPENDENT_TYPE "
               "FROM RDB$DEPENDENCIES a                           "
               "WHERE                                             "
               "    a.RDB$DEPENDED_ON_TYPE IN (1,2,5)             "
               "    AND a.RDB$DEPENDED_ON_NAME='%s'               " % object_name)
        cursor = self.db.cursor()
        cursor.execute(sql)
        dependencies = cursor.fetchall()
        return dependencies

    def table(self, tablename):
        the_table = self.db.schema.get_table(tablename.upper())
        the_table.conn = self.db
        return the_table

    def create(self, item):
        logging.info(u"Criando {}...".format(item))
        stmt = item.get_sql_for('create')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_empty_procedure(self, procedure):
        logging.info(u"Criando procedure {}...".format(procedure.name))
        stmt = procedure.get_sql_empty_definition()
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_procedure(self, procedure):
        logging.info(u"Criando procedure {}...".format(procedure.name))
        stmt = procedure.get_sql_for('create_or_alter')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def __contains__(self, item):
        """ Recebe uma tabela e verifica se o banco de dados possui esta tabela
        """
        for table in self.tables:
            if table.name==item.name:
                return True
        return False
