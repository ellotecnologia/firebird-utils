import fdb
import fdb.fbcore
import fdb.schema
import logging
import monkey_patch_fdb
from fb_foreign_keys import cria_chave_estrangeira
from progress import notify_progress


def create_connection(database_path, username='sysdba', password='masterkey'):
    return fdb.connect(database_path, username, password)


def fields_are_different(field_a, field_b):
    """ Returns True if fields are different """
    return (field_a.datatype != field_b.datatype) \
           or (field_a.isnullable() != field_b.isnullable()) \
           or (field_a.has_default() != field_b.has_default()) \
           or (str(field_a.default).strip() != str(field_b.default).strip())


class Database(object):
    def __init__(self, connection):
        self.db = connection
        self.cursor = self.db.cursor()
        self.functions = self.db.schema.functions
        self.tables = self.db.schema.tables
        self.procedures = self.db.schema.procedures
        self.views = self.db.schema.views
        self.triggers = self.db.schema.triggers
        self.schema = self.db.schema
        self.sync_fielddefs = True

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

        for k, constraint in keys.items():
            yield constraint

    @property
    def primary_keys(self):
        keys = {}
        for primary_key in self.db.schema.constraints:
            if primary_key.ispkey():
                keys[primary_key.name] = primary_key

        for k, primary_key in keys.items():
            yield primary_key

    @property
    def indices(self):
        for index in self.db.schema.indices:
            if not index.isenforcer():
                yield index

    @property
    def constraints(self):
        for constraint in self.db.schema.constraints:
            if constraint.constraint_type == 'UNIQUE':
                yield constraint

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
        logging.info("Removendo Chaves Estrangeiras")
        for constraint in self.foreign_keys:
            instruction = constraint.get_sql_for('drop')
            self.execute(instruction)
        self.db.commit()

    def drop_primary_keys(self):
        """ Drop all database primary keys
        """
        logging.info("Removendo Chaves Primárias")
        for key in self.primary_keys:
            instruction = key.get_sql_for('drop')
            self.execute(instruction)
        self.db.commit()

    def recreate_foreign_keys(self, foreign_keys):
        logging.info("Recriando Chaves Estrangeiras")
        for foreign_key in foreign_keys:
            notify_progress()
            logging.debug("Recriando Foreign Key {0}".format(foreign_key.name))
            cria_chave_estrangeira(self.db, foreign_key.get_sql_for('create'))

    def recreate_primary_keys(self, keys):
        logging.info("Recriando Chaves Primárias")
        for key in keys:
            notify_progress()
            # Pode ocorrer de a tabela já ter sido criada com a chave primária definida na DDL
            # Nestes casos irá acontecer um erro na recriação desta chave. Sendo assim é preciso ignorar esse erro.
            try:
                self.create(key)
            except fdb.fbcore.DatabaseError as e:
                #if 'already exists' in e.args[0]:
                #    logging.info(
                #        'Erro ao tentar criar chave primária {0}. Chave já existe.'.
                #        format(key.name))
                if 'Attempt to define a second PRIMARY KEY for the same table' in e.args[0]:
                    continue
                else:
                    logging.info(
                        'Erro ao tentar criar chave primária {0}. ({1})'.
                        format(key.name, e.args[0]))

    def recreate_empty_procedures(self, procedures):
        """ Procedures precisam ser criadas vazias primeiro para o caso de haver
            outras procedures/triggers/views que façam uso dela
        """
        logging.info("Recriando esqueleto das Procedures")
        for procedure in procedures:
            notify_progress()
            self.create_empty_procedure(procedure)

    def recreate_functions(self, functions):
        logging.info("Recriando Functions")
        for function in functions:
            notify_progress()
            stmt = function.get_sql_for('declare')
            self.execute(stmt)
            self.db.commit()

    def recreate_views(self, views):
        logging.info("Recriando Views")
        for view in views:
            notify_progress()
            self.create_view(view)

    def create_view(self, view):
        """ Creates a View
            Creates dependent views if necessary.
        """
        if self.view_exists(view):
            return
        logging.debug('Criando view {}'.format(view.name))
        for dependency in view.get_dependencies():
            if isinstance(dependency.depended_on, fdb.schema.ViewColumn):
                self.create_view(dependency.depended_on.view)
        self.create(view)

    def view_exists(self, view):
        self.schema.reload() # tem que recarregar o schema
        return self.db.schema.get_view(view.name) is not None

    def recreate_procedures(self, procedures):
        logging.info("Recriando Procedures")
        for procedure in procedures:
            notify_progress()
            self.create_procedure(procedure)

    def recreate_triggers(self, triggers):
        logging.info("Recriando Triggers")
        for trigger in triggers:
            notify_progress()
            self.create(trigger)

    def recreate_indices(self, indices):
        logging.info("Recriando índices")
        for index in indices:
            notify_progress()
            self.create(index)

    def recreate_constraints(self, constraints):
        logging.info("Recriando Restrições (constraints)")
        for constraint in constraints:
            notify_progress()
            logging.debug('Recriando constraint {}'.format(constraint.name))
            self.create(constraint)

    def create_generators(self, generators):
        logging.info("Recriando Generators")
        ours_generators = [gen.name for gen in self.generators]
        for gen in generators:
            notify_progress()
            if gen.name not in ours_generators:
                stmt = gen.get_sql_for('create')
                self.execute(stmt)
        self.db.commit()

    def drop_indices(self):
        """ Drop all database indexes
        """
        logging.info("Removendo todos os índices")
        for index in self.indices:
            notify_progress()
            instruction = "DROP INDEX {}".format(index.name)
            self.execute(instruction)
        self.db.commit()

    def drop_constraints(self):
        """ Drop all UNIQUE constraints
        """
        logging.info("Removendo todas as restrições (constraints)")
        for constraint in self.constraints:
            notify_progress()
            logging.debug('Removendo constraint {}'.format(constraint.name))
            instruction = "ALTER TABLE {} DROP CONSTRAINT {}".format(constraint.table.name, constraint.name)
            self.execute(instruction)
        self.db.commit()

    def drop_triggers(self):
        logging.info("Removendo Triggers")
        for trigger in self.triggers:
            notify_progress()
            instruction = trigger.get_sql_for('drop')
            self.execute(instruction)
        self.db.commit()

    def drop_views(self):
        logging.info("Removendo Views")
        for view in self.views:
            notify_progress()
            self.drop_object_and_dependencies(view.name, 1)

    def drop_procedures(self):
        logging.info("Removendo Procedures")
        for procedure in self.procedures:
            notify_progress()
            stmt = procedure.get_sql_for('drop')
            try:
                self.execute(stmt)
            except fdb.fbcore.DatabaseError as e:
                if (e.args[1] == -607) and (e.args[2] == 335544351):
                    logging.error("  procedure {} já foi removida.".format(procedure.name))
                else:
                    logging.error("Erro ao remover procedure {} ({})".format(procedure.name, repr(e)))
                continue
        self.db.commit()

    def drop_functions(self):
        logging.info("Removendo Functions")
        for function in self.functions:
            notify_progress()
            stmt = function.get_sql_for('drop')
            self.execute(stmt)
        self.db.commit()

    def drop_object_and_dependencies(self, name, ttype):
        cursor = self.db.cursor()
        object_name = name.strip()
        object_types = {
            # 0: 'TABLE',
            1: 'VIEW',
            2: 'TRIGGER',
            # 3: 'computed column',
            # 4: 'CHECK constraint',
            5: 'PROCEDURE',
            99: 'EXTERNAL FUNCTION'
        }
        for dname, dtype in self._get_dependencies(object_name):
            notify_progress()
            dname = dname.strip()
            if dname == object_name: continue
            if dtype == 3: continue
            self.drop_object_and_dependencies(dname, dtype)

        sql = 'DROP {0} {1}'.format(object_types[ttype], object_name)
        try:
            cursor.execute(sql)
            self.db.commit()
        except fdb.fbcore.DatabaseError as e:
            logging.debug("Erro ao remover {0} {1}".format(object_types[ttype], name))
            logging.debug('  ' + repr(e))
            self.db.rollback()
            return False
        #logging.debug("-> {0}".format(sql))
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

    def get_table(self, tablename):
        table = self.schema.get_table(tablename.upper())
        table.conn = self.db
        return table

    def create(self, item):
        stmt = item.get_sql_for('create')
        self.execute(stmt)
        self.db.commit()

    def create_empty_procedure(self, procedure):
        logging.debug("Criando esqueleto da procedure {}".format(procedure.name))
        stmt = procedure.get_sql_empty_definition()
        self.execute(stmt)
        self.db.commit()

    def create_procedure(self, procedure):
        logging.debug("Criando procedure {}".format(procedure.name))
        stmt = procedure.get_sql_for('create_or_alter')
        self.execute(stmt)
        self.db.commit()

    def create_missing_tables(self, table_list):
        #logging.info("Sincronizando tabelas, aguarde...")
        for table in table_list:
            notify_progress()
            if table not in self:
                logging.info("Tabela {0} não encontrada no banco de dados, recriando...".format(table.name))
                self.create(table)
                continue
        self.db.commit()
        self.schema.reload()
        # This routine creates tables with their respective primary/foreign keys,
        # so we need to remove them to avoid errors at the end of the process,
        # when ALL constraints are recreated.
        self.drop_foreign_keys()
        self.drop_indices()
        self.drop_primary_keys()

    def remove_dangling_tables(self, source_database):
        for table in self.tables:
            notify_progress()
            if table not in source_database:
                logging.info("Removendo tabela {}".format(table.name))
                self.execute("DROP TABLE {}".format(table.name))
        self.db.commit()
        self.schema.reload()
        
    def sync_tables_structure(self, reference_tables):
        logging.info("Sincronizando campos das tabelas, aguarde...")
        for reference_table in reference_tables:
            notify_progress()
            table = self.get_table(reference_table.name)
            if self.sync_fielddefs:
                self.sync_table_fields(table, reference_table)
            self.drop_dangling_fields(table, reference_table)
            self.reorder_table_fields(table, reference_table.columns)
        self.db.commit()
    
    def sync_table_fields(self, table, reference_table):
        for reference_field in reference_table:
            if reference_field not in table:
                self.create_field(table, reference_field)
            else:
                # Compara o campo
                field = table.get_column(reference_field.name)
                if fields_are_different(field, reference_field):
                    self.sync_field(table, reference_field)
                    
    def sync_field(self, table, reference_field):
        """ Sincroniza a estrutura de um campo """
        logging.info('Campo {}.{} está diferente do banco de referência, ajustando...'.format(table.name, reference_field.name))
        fieldname = reference_field.name
        tmp_fieldname = fieldname + '_1'
        
        # Se o campo não aceitar NULL é necessário antes corrigir os registros
        # na tabela que estiverem com o valor null para este campo.
        if not reference_field.isnullable():
            default_value = reference_field.default or "''"
            logging.debug('Definindo valor default para o campo {}, aguarde...'.format(fieldname))
            self.execute('UPDATE {} SET {}={} WHERE {} IS NULL'.format(table.name, fieldname, default_value, fieldname))
            self.db.commit()
        
        logging.debug('Criando campo temporário {}.{}'.format(table.name, tmp_fieldname))
        self.create_field(table, reference_field, tmp_fieldname)
        
        logging.debug('Sincroniza valores do campo {} com o campo {}, aguarde...'.format(fieldname, tmp_fieldname))
        self.execute('UPDATE {} SET {}={}'.format(table.name, tmp_fieldname, fieldname))
        
        logging.debug('Removendo campo {}'.format(fieldname))
        self.execute('ALTER TABLE {} DROP {}'.format(table.name, fieldname))
        
        logging.debug('Renomeando campo {} para {}'.format(tmp_fieldname, fieldname))
        self.execute('ALTER TABLE {} ALTER {} TO {}'.format(table.name, tmp_fieldname, fieldname))
        self.db.commit()
                
    def execute(self, stmt):
        self.cursor.execute(stmt)
    
    def create_field(self, table, field, field_name=None):
        """ Cria um novo campo"""
        field_name = field_name or field.name
        logging.debug('Criando campo {}.{}'.format(table.name, field_name))
        stmt = "ALTER TABLE {} ADD {} {} ".format(table.name, field_name, field.datatype)
        if field.has_default():
            stmt += "DEFAULT {} ".format(field.default)
        if not field.isnullable():
            stmt += "NOT NULL "
        self.execute(stmt)
        self.db.commit()

    def drop_dangling_fields(self, table, reference_table):
        """ Remove campos que estiverem sobrando na tabela 'table' """
        for field in table:
            if field not in reference_table:
                logging.info("Removendo campo {}.{} pois não é mais utilizado".format(table.name, field.name))
                stmt = field.get_sql_for('drop')
                self.execute(stmt)
        self.db.commit()
    
    def reorder_table_fields(self, table, reference_columns):
        """ Reordena campos da tabela """
        logging.debug("Reordenando campos de {}".format(table.name))
        for column in reference_columns:
            stmt = column.get_sql_for('alter', position=column.position+1)
            self.execute(stmt)
        self.db.commit()
    
    def synchronize_comments(self, src):
        self._synchronize_column_comments(src.column_comments)
        self._synchronize_table_comments(src.table_comments)

    def _synchronize_column_comments(self, column_comments):
        logging.info("Sincronizando comentários dos campos")
        for tablename, fieldname, comment in column_comments:
            tablename = tablename.strip()
            fieldname = fieldname.strip()
            comment = comment.strip() #.decode('latin1', 'ignore')
            #logging.info("Adicionando comentário no campo {} da tabela {}".format(fieldname, tablename))
            notify_progress()
            try:
                self.cursor.execute("COMMENT ON COLUMN {}.{} IS '{}'".format(tablename, fieldname, comment))
            except fdb.fbcore.DatabaseError as e:
                logging.info('{} {} {}'.format(tablename, fieldname, comment))
        self.db.commit()

    def _synchronize_table_comments(self, table_comments):
        logging.info("Sincronizando comentários das tabelas")
        for tablename, comment in table_comments:
            tablename = tablename.strip()
            comment = comment.strip() #.decode('latin1', 'ignore')
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
