# encoding: utf8
from __future__ import unicode_literals
from __future__ import print_function

import re
import logging
from fdb.fbcore import DatabaseError

def cria_chave_estrangeira(conn, ddl):
    """ Tenta recriar uma foreign key.
        Caso não consiga, vai excluindo os registros que estão impedindo
        a criação da mesma.
    """
    while True:
        try:
            conn.execute_immediate(ddl)
            conn.commit()
            break
        except DatabaseError, e:
            conn.rollback()
            logging.error(e[0])
            print("")
            print("Instrução que causou o problema:\n")
            print(ddl)
            print("")
            print("Faça a correção manual no banco de dados.")
            print("Após isso, pressione ENTER para tentar novamente")
            raw_input()
            #nome_tabela = extrai_nome_tabela(e[0])
            #clausula_where = extrai_clausula_where(e[0])
            #remove_registro(conn, nome_tabela, clausula_where)


def extrai_nome_tabela(error_msg):
   match = re.search('on table "(\w+)"', error_msg)
   if match:
      return match.groups()[0]
   else:
      return ""
   

def extrai_clausula_where(error_msg):
    match = re.search('key value is \((.+)\)', error_msg)
    return match.groups()[0].replace('"', '').replace(',', ' and')


def remove_registro(conn, nome_tabela, clausula_where):
    cursor = conn.cursor()
    sql = "delete from {0} where {1}".format(nome_tabela, clausula_where)
    logging.info(sql)
    while True:
        try:
            cursor.execute(sql)
            conn.commit()
            break;
        except DatabaseError, e:
            conn.rollback()
            nome_tabela2 = extrai_nome_tabela(e[0])
            clausula_where2 = extrai_clausula_where(e[0])
            remove_registro(conn, nome_tabela2, clausula_where2)


if __name__=="__main__":
    import fdb
    conn = fdb.connect('/ello/dados/global.ello', 'sysdba', 'masterkey')

    cria_chave_estrangeira(conn, '''\
ALTER TABLE TREGREGISTROFORMA ADD CONSTRAINT TREGREGISTROFORMA_FK6
FOREIGN KEY (EMPRESA,IDBAIXARECEBER) REFERENCES TRECBAIXA (EMPRESA,IDBAIXA)''')

    
