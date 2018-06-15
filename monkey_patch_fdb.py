# encoding: utf8
from __future__ import unicode_literals

from fdb.schema import Table, TableColumn, Procedure

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


Table.__contains__ = __contains__
Table.__iter__ = __iter__
Table.next = next


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