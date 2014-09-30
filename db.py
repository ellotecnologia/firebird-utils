import firebird

def dropAllProcedures():
    for record in firebird.procedures():
        firebird.dropObject(record[0], 5)

def dropAllTriggers():
    for record in firebird.triggers():
        firebird.dropObject(record[0], 2)

def dropAllViews():
    for record in firebird.views():
        firebird.dropObject(record[2], 1)

dropAllProcedures()
dropAllTriggers()
dropAllViews()

