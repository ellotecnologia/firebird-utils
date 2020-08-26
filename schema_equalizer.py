import sys
import argparse
import logging

import fdb

import config
import firebird


__VERSION__ = "1.2.2"


def main(args):
    """ Sincroniza a estrutura do banco 'src_database_file' de acordo com
        a estrutura de 'dst_database_file'.
    """
    src_connection = firebird.create_connection(args.ORIGEM)
    dst_connection = firebird.create_connection(args.DESTINO)
    
    src = firebird.Database(src_connection)
    dst = firebird.Database(dst_connection)
    
    dst.sync_fielddefs = not args.disable_fieldsync
    
    dst.drop_foreign_keys()
    dst.drop_indices()
    dst.drop_primary_keys()
    dst.drop_constraints()
    dst.drop_triggers()
    dst.drop_views()
    dst.drop_procedures()
    dst.drop_functions()
    
    dst.create_missing_tables(src.tables)
    dst.remove_dangling_tables(src)
    dst.sync_tables_structure(src.tables)
    
    dst.create_generators(src.generators)
    
    dst.recreate_constraints(src.constraints)
    dst.recreate_primary_keys(src.primary_keys)
    dst.recreate_foreign_keys(src.foreign_keys)
    
    dst.recreate_empty_procedures(src.procedures)
    dst.recreate_functions(src.functions)
    dst.recreate_views(src.views)
    dst.recreate_procedures(src.procedures)
    dst.recreate_triggers(src.triggers)
    dst.recreate_indices(src.indices)
    
    dst.synchronize_comments(src)
    # TODO: Apply grants
    
    sincroniza_sequencial_release(src_connection, dst_connection)
    
    logging.info("")
    logging.info("Processo finalizado com sucesso!")


def sincroniza_sequencial_release(src_connection, dst_connection):
    """ Sincroniza o valor do parâmetro GerIdScriptRelease nos bancos
    """
    src_cursor = src_connection.cursor()
    dst_cursor = dst_connection.cursor()
    
    src_cursor.execute("SELECT CAST(Valor AS VARCHAR(5)) "
                       "FROM TGerParametros "
                       "WHERE parametro='GERIDSCRIPTRELEASE'")
    ultimo_id = src_cursor.fetchone()[0]

    logging.info("Atualizando sequencial de Release para {}".format(ultimo_id))

    # Remove o parâmetro antes
    dst_cursor.execute("DELETE FROM TGerParametros WHERE Parametro='GERIDSCRIPTRELEASE'")
    
    # Pega o Id do próximo parâmetro
    dst_cursor.execute("SELECT COALESCE(MAX(IdParametro), 0) + 1 FROM TGerParametros")
    idparametro = dst_cursor.fetchone()[0]
    
    dst_cursor.execute("INSERT INTO TGERPARAMETROS (IDPARAMETRO, PARAMETRO, VALOR) "
                       "VALUES ({}, 'GERIDSCRIPTRELEASE', '{}')".format(idparametro, ultimo_id))
    dst_connection.commit()
    
    
def parse_args():
    parser = argparse.ArgumentParser(description="Utilitário de Equalização de Banco de Dados Firebird")
    parser.add_argument('--debug', action="store_true", default=False, help="Ativa modo debug")
    parser.add_argument('--version', action="store_true", dest="version", help="Mostra versão do utilitário")
    parser.add_argument('--disable-fieldsync', action="store_true", default=False, help="Desativa sincronização dos campos")
    parser.add_argument('ORIGEM', nargs='?')
    parser.add_argument('DESTINO', nargs='?')
    return parser.parse_args()

    
if __name__ == "__main__":
    args = parse_args()

    if args.version:
        print("Schema Equalizer versão {}".format(__VERSION__))
        sys.exit()

    config.setup_config(args.debug)
    
    if not (args.ORIGEM and args.DESTINO):
        args.ORIGEM = input('Informe o caminho do banco de dados BOM: ')
        args.DESTINO = input('Informe o caminho do banco de dados ZUADO: ')

    try:
        main(args)
    except fdb.DatabaseError as e:
        print(e.args[0])

    #raw_input('')
