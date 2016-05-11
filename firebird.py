#coding: utf8
import fdb
import fdb.fbcore
import logging
import monkey_patch_fdb

logging.basicConfig(
    level=logging.INFO,
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
    def generators(self):
        for generator in self.db.schema.generators:
            yield generator

    @property
    def foreign_keys(self):
        keys = {}
        for constraint in self.db.schema.constraints:
            if constraint.isfkey():
                keys[constraint.name] = constraint

        for k, constraint in keys.iteritems():
            yield constraint

    @property
    def primary_keys(self):
        keys = {}
        for primary_key in self.db.schema.constraints:
            if primary_key.ispkey():
                keys[primary_key.name] = primary_key

        for k, primary_key in keys.iteritems():
            yield primary_key

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

    def drop_primary_keys(self):
        """ Drop all database primary keys
        """
        logging.info(u"Removendo Primary Keys...")
        for key in self.primary_keys:
            instruction = key.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def recreate_foreign_keys(self, foreign_keys):
        logging.info(u"Recriando Foreign Keys...")
        for foreign_key in foreign_keys:
            self.create(foreign_key)

    def recreate_primary_keys(self, keys):
        logging.info(u"Recriando Primary Keys...")
        for key in keys:
            # Pode ocorrer de a tabela já ter sido criada com a chave primária definida na DDL
            # Nestes casos irá acontecer um erro na recriação desta chave. Sendo assim é preciso ignorar esse erro.
            try:
                self.create(key)
            except fdb.fbcore.DatabaseError, e:
                if 'already exists' in e.args[0]:
                    logging.info(u'Erro ao tentar criar chave primária {0}. Chave já existe.'.format(key.name))
                else:
                    logging.info(u'Erro ao tentar criar chave primária {0}. ({1})'.format(key.name, e.args[0]))

    def recreate_empty_procedures(self, procedures):
        """ Procedures precisam ser criadas vazias primeiro para o caso de haver
            outras procedures/triggers/views que façam uso dela
        """
        logging.info(u"Recriando Procedures vazias...")
        for procedure in procedures:
            self.create_empty_procedure(procedure)

    def recreate_functions(self, functions):
        logging.info(u"Recriando Functions...")
        for function in functions:
            stmt = function.get_sql_for('declare')
            logging.debug(stmt)
            self.cursor.execute(stmt)
            self.db.commit()

    def recreate_views(self, views):
        logging.info(u"Recriando Views...")
        for view in views:
            self.create(view)

    def recreate_procedures(self, procedures):
        logging.info(u"Recriando Procedures...")
        for procedure in procedures:
            self.create_procedure(procedure)

    def recreate_triggers(self, triggers):
        logging.info(u"Recriando Triggers...")
        for trigger in triggers:
            self.create(trigger)

    def recreate_indices(self, indices):
        logging.info(u"Recriando Índices...")
        for index in indices:
            self.create(index)

    def create_generators(self, generators):
        logging.info(u"Recriando generators...")
        ours_generators = [gen.name for gen in self.generators]
        for gen in generators:
            if gen.name not in ours_generators:
                stmt = gen.get_sql_for('create')
                logging.debug(stmt)
                self.cursor.execute(stmt)
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
            try:
                self.cursor.execute(stmt)
            except fdb.fbcore.DatabaseError:
                continue
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
            logging.info("Erro ao remover {0} {1} ({2})".format(object_types[ttype], name, repr(e)))
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
        logging.debug(u"Criando {}...".format(item))
        stmt = item.get_sql_for('create')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_empty_procedure(self, procedure):
        logging.debug(u"Criando procedure vazia {}...".format(procedure.name))
        stmt = procedure.get_sql_empty_definition()
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_procedure(self, procedure):
        logging.debug(u"Criando procedure {}...".format(procedure.name))
        stmt = procedure.get_sql_for('create_or_alter')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_missing_tables(self, table_list):
        logging.info(u"Verificando se faltam tabelas...")
        for table in table_list:
            if table not in self:
                logging.info(u"Tabela {0} não existe no banco destino".format(table.name))
                self.create(table)
                continue

            logging.info(u"Ajustando campos da tabela {0}".format(table.name))

            dst_table = self.table(table.name)
            dst_table.equalize(table)

            # TODO: 
            #   - Remover tabelas que estão sobrando
            #   - Equalizar definição dos campos

    def __contains__(self, item):
        """ Recebe uma tabela e verifica se o banco de dados possui esta tabela
        """
        for table in self.tables:
            if table.name==item.name:
                return True
        return False

