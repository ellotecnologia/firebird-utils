# coding: utf8

__VERSION__ = "0.6"

import sys
from firebird import Database

if len(sys.argv) > 2:
    src = Database(sys.argv[1])
    dst = Database(sys.argv[2])
else:
    src = Database(raw_input('Informe o caminho do banco de dados BOM: '))
    dst = Database(raw_input('Informe o caminho do banco de dados ZUADO: '))

dst.drop_foreign_keys()
dst.drop_indices()
dst.drop_primary_keys()
dst.drop_triggers()
dst.drop_views()
dst.drop_procedures()
dst.drop_functions()

dst.create_missing_tables(src.tables)
dst.remove_unused_tables(src.tables)

dst.create_generators(src.generators)

dst.recreate_primary_keys(src.primary_keys)
dst.recreate_foreign_keys(src.foreign_keys)

dst.recreate_empty_procedures(src.procedures)
dst.recreate_functions(src.functions)
dst.recreate_views(src.views)
dst.recreate_procedures(src.procedures)
dst.recreate_triggers(src.triggers)
dst.recreate_indices(src.indices)

dst.synchronize_column_comments(src.column_comments)

## Apply grants

raw_input("Processo finalizado com sucesso!")

