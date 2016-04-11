# coding: utf8
from firebird import Database

src = Database(raw_input('Informe o caminho do banco de dados BOM: '))
dst = Database(raw_input('Informe o caminho do banco de dados ZUADO: '))

dst.drop_foreign_keys()
dst.drop_indices()
dst.drop_primary_keys()
dst.drop_triggers()
dst.drop_views()
dst.drop_procedures()
dst.drop_functions()

print

dst.create_missing_tables(src.tables)

dst.create_generators(src.generators)
dst.recreate_primary_keys(src.primary_keys)
dst.recreate_foreign_keys(src.foreign_keys)
dst.recreate_empty_procedures(src.procedures)
dst.recreate_functions(src.functions)
dst.recreate_views(src.views)
dst.recreate_procedures(src.procedures)
dst.recreate_triggers(src.triggers)
dst.recreate_indices(src.indices)
## Apply grants

raw_input("Processo finalizado com sucesso!")

