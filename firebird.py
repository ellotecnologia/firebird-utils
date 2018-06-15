# encoding: utf8
from __future__ import unicode_literals

import fdb
import fdb.fbcore
import logging
import monkey_patch_fdb
from fb_foreign_keys import cria_chave_estrangeira
from progress import notify_progress


def create_connection(database_path, username='sysdba', password='masterkey'):
    return fdb.connect(database_path, username, password)


class Database(object):
    def __init__(self, connection):
        self.db = connection
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

    @property
    def column_comments(self):
        self.cursor.execute(
            "SELECT a.RDB$RELATION_NAME, a.RDB$FIELD_NAME, a.RDB$DESCRIPTION "
            "FROM RDB$RELATION_FIELDS a "
            "WHERE a.RDB$DESCRIPTION IS NOT NULL")
        rows = self.cursor.fetchall()
        return rows

    @property
    def table_comments(self):
        self.cursor.execute("SELECT RDB$RELATION_NAME, RDB$DESCRIPTION "
                            "FROM RDB$RELATIONS "
                            "WHERE RDB$DESCRIPTION IS NOT NULL", )
        rows = self.cursor.fetchall()
        return rows

    def drop_foreign_keys(self):
        """ Drop all database foreign keys
        """
        logging.info("Removendo Chaves Estrangeiras...")
        for constraint in self.foreign_keys:
            instruction = constraint.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_primary_keys(self):
        """ Drop all database primary keys
        """
        logging.info("Removendo Chaves Primárias...")
        for key in self.primary_keys:
            instruction = key.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def recreate_foreign_keys(self, foreign_keys):
        logging.info("Recriando Chaves Estrangeiras...")
        for foreign_key in foreign_keys:
            notify_progress()
            logging.debug("Recriando Foreign Key {0}".format(foreign_key.name))
            cria_chave_estrangeira(self.db, foreign_key.get_sql_for('create'))

    def recreate_primary_keys(self, keys):
        logging.info("Recriando Chaves Primárias...")
        for key in keys:
            notify_progress()
            # Pode ocorrer de a tabela já ter sido criada com a chave primária definida na DDL
            # Nestes casos irá acontecer um erro na recriação desta chave. Sendo assim é preciso ignorar esse erro.
            try:
                self.create(key)
            except fdb.fbcore.DatabaseError, e:
                if 'already exists' in e.args[0]:
                    logging.info(
                        'Erro ao tentar criar chave primária {0}. Chave já existe.'.
                        format(key.name))
                else:
                    logging.info(
                        'Erro ao tentar criar chave primária {0}. ({1})'.
                        format(key.name, e.args[0]))

    def recreate_empty_procedures(self, procedures):
        """ Procedures precisam ser criadas vazias primeiro para o caso de haver
            outras procedures/triggers/views que façam uso dela
        """
        logging.info("Recriando esqueleto das Procedures...")
        for procedure in procedures:
            notify_progress()
            self.create_empty_procedure(procedure)

    def recreate_functions(self, functions):
        logging.info("Recriando Functions...")
        for function in functions:
            notify_progress()
            stmt = function.get_sql_for('declare')
            logging.debug(stmt)
            self.cursor.execute(stmt)
            self.db.commit()

    def recreate_views(self, views):
        logging.info("Recriando Views...")
        for view in views:
            notify_progress()
            self.create(view)

    def recreate_procedures(self, procedures):
        logging.info("Recriando Procedures...")
        for procedure in procedures:
            notify_progress()
            self.create_procedure(procedure)

    def recreate_triggers(self, triggers):
        logging.info("Recriando Triggers...")
        for trigger in triggers:
            notify_progress()
            self.create(trigger)

    def recreate_indices(self, indices):
        logging.info("Recriando índices...")
        for index in indices:
            notify_progress()
            self.create(index)

    def create_generators(self, generators):
        logging.info("Recriando Generators...")
        ours_generators = [gen.name for gen in self.generators]
        for gen in generators:
            notify_progress()
            if gen.name not in ours_generators:
                stmt = gen.get_sql_for('create')
                logging.debug(stmt)
                self.cursor.execute(stmt)
        self.db.commit()

    def drop_indices(self):
        """ Drop all database indexes
        """
        logging.info("Removendo todos os índices...")
        for index in self.indices:
            notify_progress()
            instruction = "DROP INDEX {}".format(index.name)
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_triggers(self):
        logging.info("Removendo todas as Triggers...")
        for trigger in self.triggers:
            notify_progress()
            instruction = trigger.get_sql_for('drop')
            logging.debug(instruction)
            self.cursor.execute(instruction)
        self.db.commit()

    def drop_views(self):
        logging.info("Removendo todas as Views...")
        for view in self.views:
            notify_progress()
            self.drop_object_and_dependencies(view.name, 1)

    def drop_procedures(self):
        logging.info("Removendo todas as Procedures...")
        for procedure in self.procedures:
            notify_progress()
            stmt = procedure.get_sql_for('drop')
            try:
                self.cursor.execute(stmt)
            except fdb.fbcore.DatabaseError as e:
                if (e.args[1] == -607) and (e.args[2] == 335544351):
                    logging.error("  procedure {} já foi removida.".format(procedure.name))
                else:
                    logging.error("Erro ao remover procedure {} ({})".format(procedure.name, repr(e)))
                continue
        self.db.commit()

    def drop_functions(self):
        logging.info("Removendo todas as Functions...")
        for function in self.functions:
            notify_progress()
            stmt = function.get_sql_for('drop')
            logging.debug(stmt)
            self.cursor.execute(stmt)
        self.db.commit()

    def drop_object_and_dependencies(self, name, ttype):
        cursor = self.db.cursor()
        object_name = name.strip()
        object_types = {
            1: 'VIEW',
            2: 'TRIGGER',
            5: 'PROCEDURE',
            99: 'EXTERNAL FUNCTION'
        }
        for dname, dtype in self._get_dependencies(object_name):
            notify_progress()
            dname = dname.strip()
            if dname == object_name: continue
            self.drop_object_and_dependencies(dname, dtype)

        sql = 'DROP {0} {1}'.format(object_types[ttype], object_name)
        try:
            cursor.execute(sql)
            self.db.commit()
        except fdb.fbcore.DatabaseError, e:
            logging.debug("Erro ao remover {0} {1}".format(object_types[ttype], name))
            logging.debug('  ' + repr(e))
            self.db.rollback()
            return False
        logging.debug("-> {0}".format(sql))
        return True

    def _get_dependencies(self, object_name):
        """ Retorna a lista de dependencias de um objeto
        """
        sql = (
            "SELECT a.RDB$DEPENDENT_NAME, a.RDB$DEPENDENT_TYPE "
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
        logging.debug("Criando {}...".format(item))
        stmt = item.get_sql_for('create')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_empty_procedure(self, procedure):
        logging.debug("Criando esqueleto da procedure {}".format(procedure.name))
        stmt = procedure.get_sql_empty_definition()
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_procedure(self, procedure):
        logging.debug("Criando procedure {}".format(procedure.name))
        stmt = procedure.get_sql_for('create_or_alter')
        logging.debug(stmt)
        self.cursor.execute(stmt)
        self.db.commit()

    def create_missing_tables(self, table_list):
        logging.info("Sincronizando campos das tabelas, aguarde...")
        for table in table_list:
            notify_progress()
            if table not in self:
                logging.info("Tabela {0} não existe no banco destino".format(
                    table.name))
                self.create(table)
                continue

            logging.debug("Ajustando campos da tabela {0}".format(table.name))
            dst_table = self.table(table.name)
            dst_table.equalize(table)
        self.db.commit()

    def remove_unused_tables(self, source_database):
        for table in self.tables:
            notify_progress()
            if table not in source_database:
                logging.info("Removendo tabela {}".format(table.name))
                self.cursor.execute("DROP TABLE {}".format(table.name))
        self.db.commit()

    def synchronize_comments(self, src):
        self._synchronize_column_comments(src.column_comments)
        self._synchronize_table_comments(src.table_comments)

    def _synchronize_column_comments(self, column_comments):
        logging.info("Sincronizando comentários dos campos")
        for tablename, fieldname, comment in column_comments:
            tablename = tablename.strip()
            fieldname = fieldname.strip()
            comment = comment.strip().decode('latin1', 'ignore')
            #logging.info("Adicionando comentário no campo {} da tabela {}".format(fieldname, tablename))
            notify_progress()
            self.cursor.execute("COMMENT ON COLUMN {}.{} IS '{}'".format(tablename, fieldname, comment))
        self.db.commit()

    def _synchronize_table_comments(self, table_comments):
        logging.info("Sincronizando comentários das tabelas")
        for tablename, comment in table_comments:
            tablename = tablename.strip()
            comment = comment.strip().decode('latin1', 'ignore')
            #logging.info("Adicionando comentário na tabela {}".format(tablename))
            notify_progress()
            self.cursor.execute("COMMENT ON TABLE {} IS '{}'".format(tablename, comment))
        self.db.commit()

    def __contains__(self, item):
        """ Recebe uma tabela e verifica se o banco de dados possui esta tabela
        """
        for table in self.tables:
            if table.name.strip() == item.name.strip():
                return True
        return False


# TODO: Equalizar definição dos campos
