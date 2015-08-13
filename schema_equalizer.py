# coding: utf8
from firebird import Database

src = Database('/Ello/Dados/versao-1229.ello')
dst = Database('/Ello/Dados/Maccari.ello')

dst.drop_foreign_keys()
dst.drop_indices()
dst.drop_triggers()
dst.drop_views()
dst.drop_procedures()
dst.drop_functions()

# Recreate tables which are not in the dst database
for table in src.tables:
    if table not in dst:
        print u"Tabela {} não existe no banco destino".format(table.name)
        dst.create(table)
        continue

    dst_table = dst.table(table.name)
    dst_table.equalize(table)

dst.recreate_foreign_keys(src.foreign_keys)
dst.recreate_empty_procedures(src.procedures)
dst.recreate_views(src.views)
dst.recreate_procedures(src.procedures)
dst.recreate_triggers(src.triggers)
dst.recreate_indices(src.indices)
## Apply grants

raw_input("Processo finalizado com sucesso!")

