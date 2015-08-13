from fdb.schema import Table, Procedure
import logging

# Monkey patch
def __contains__(self, item):
    for column in self.columns:
        if column.name==item.name:
            return True
    return False

def __iter__(self):
    self._patched_columns_iterator = iter(self.columns)
    return self._patched_columns_iterator

def next(self):
    return self._patched_columns_iterator.next()

def equalize(self, table):
    for field in table:
        if field not in self:
            logging.info('Criando campo {}.{}'.format(table.name, field.name))
            self.create_field(table.name, field)

        #if field != self.get_column(field.name):
        #    self.equalize_field(field)

    logging.info(u"Reordenando campos de {}".format(table.name))
    self.reorder(table.columns)

def create_field(self, table_name, field):
    stmt = "ALTER TABLE {} ADD {} {}".format(table_name, field.name, field.datatype)
    cursor = self.conn.cursor()
    cursor.execute(stmt)
    self.conn.commit()

def reorder(self, columns):
    cursor = self.conn.cursor()
    for column in columns:
        cursor.execute(column.get_sql_for('alter', position=column.position+1))
    self.conn.commit()

Table.__contains__ = __contains__
Table.__iter__ = __iter__
Table.next = next
Table.equalize = equalize
Table.reorder = reorder
Table.create_field = create_field

def get_sql_empty_definition(self):
    in_params = ''
    out_params = ''

    if self.input_params:
        in_params = ',\n'.join([p.get_sql_definition() for p in self.input_params])
        in_params = '({}) '.format(in_params)

    if self.output_params:
        out_params = ',\n'.join([p.get_sql_definition() for p in self.output_params])
        out_params = "\nRETURNS ({})".format(out_params)

    stmt = 'CREATE PROCEDURE {} {} {}\nAS\nBEGIN SUSPEND; END'.format(self.name, in_params, out_params)
    return stmt

Procedure.get_sql_empty_definition = get_sql_empty_definition

