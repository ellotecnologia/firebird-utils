from fdb.schema import Table, TableColumn, Procedure
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
    for column in table:
        if column not in self:
            logging.info('Criando campo {}.{}'.format(table.name, column.name))
            self.create_column(table.name, column)

        #if column != self.get_column(column.name):
        #    self.equalize_column(column)

    for column in self:
        if column not in table:
            self.drop_column(column)

    logging.debug(u"Reordenando campos de {}".format(table.name))
    self.reorder(table.columns)

def create_column(self, table_name, field):
    stmt = "ALTER TABLE {} ADD {} {}".format(table_name, field.name, field.datatype)
    cursor = self.conn.cursor()
    cursor.execute(stmt)
    self.conn.commit()

def drop_column(self, column):
    logging.info(u"Removendo campo {}...".format(column.name))
    stmt = column.get_sql_for('drop')
    logging.debug(stmt)
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
Table.create_column = create_column
Table.drop_column = drop_column

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


def column_equals(self, other):
    return (
        self.name==other.name
        and self.datatype==other.datatype
        and self.default==other.default
        and self.isnullable()==other.isnullable()
    )

TableColumn.__eq__ = column_equals
