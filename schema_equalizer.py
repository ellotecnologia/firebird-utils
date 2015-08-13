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

# Recreate foreign keys
for foreign_key in src.foreign_keys:
    dst.create(foreign_key)

# Recreate empty procedures
for procedure in src.procedures:
    dst.create_empty_procedure(procedure)

# Recreate views
for view in src.views:
    dst.create(view)

# Recreate full procedures
for procedure in src.procedures:
    dst.create_procedure(procedure)

# Recreate Trigges
for trigger in src.triggers:
    dst.create(trigger)

# Recreate indices

## Apply grants
#
