import re
import logging
from fdb.fbcore import DatabaseError
from fdb.fbcore import Connection

def cria_chave_estrangeira(conn_dst: Connection, conn_src: Connection, ddl: str):
    """ Tenta recriar uma foreign key.
        Caso não consiga, vai excluindo os registros que estão impedindo
        a criação da mesma.
    """
    while True:
        try:
            conn_dst.execute_immediate(ddl)
            conn_dst.commit()
            break
        except DatabaseError as e:
            conn_dst.rollback()
            #logging.error(e[0])
            #print("")
            #print("Instrução que causou o problema:\n")
            #print(ddl)
            #print("")
            #print("Faça a correção manual no banco de dados.")
            #print("Após isso, pressione ENTER para tentar novamente")
            #raw_input()
            nome_tabela = extrai_nome_tabela(e.args[0])
            campos = obtem_campos_origem(extrai_nome_fk(e.args[0]), conn_src)
            valores = extrai_clausula_where(e.args[0])
            clausula_where = compoe_clausula_where(campos, valores)
            remove_registro(conn_dst, conn_src, nome_tabela, clausula_where)


def extrai_nome_tabela(error_msg):
   match = re.search('on table "(\w+)"', error_msg)
   if match:
      return match.groups()[0]
   else:
      return ""
   

def extrai_clausula_where(error_msg) -> tuple:
    return re.findall('"\w+" = (\'?(?:\d*\w*-?)+\'?)', error_msg)
    # match = re.search('key value is \((.+)\)', error_msg)
    # if match:
    #     return match.groups()[0].replace('"', '').replace(',', ' and')
    # else:
    #     return ""

def remove_registro(conn_dst, conn_src, nome_tabela: str, clausula_where: str):
    cursor = conn_dst.cursor()
    sql = "delete from {0} where {1}".format(nome_tabela, clausula_where)
    logging.info(sql)
    while True:
        try:
            # cursor.execute_immediate(sql)
            conn_dst.execute_immediate(sql)
            conn_dst.commit()
            break
        except DatabaseError as e:
            conn_dst.rollback()
            nome_tabela2 = extrai_nome_tabela(e.args[0])
            campos2 = obtem_campos_origem(extrai_nome_fk(e.args[0]), conn_src)
            valores2 = extrai_clausula_where(e.args[0])
            clausula_where2 = compoe_clausula_where(campos2, valores2)
            remove_registro(conn_dst, conn_src, nome_tabela2, clausula_where2)

def compoe_clausula_where(campos: tuple, valores: tuple):
    i = 0
    clausula = ''
    while i < len(campos):
        nome_campo = campos[i]
        valor = valores[i]
        if clausula == '':
            clausula = nome_campo + '=' + valor
        else:
            clausula = clausula + ' and ' + nome_campo + '=' + valor
        i += 1
    return clausula

def extrai_nome_fk(erro_msg: str):
    match = re.search('constraint "(\w+)"', erro_msg)
    return match.groups()[0] 

def obtem_campos_origem(fk_name: str, conn: Connection) -> tuple:
    """Retorna os campos de origem que compõem uma Foreign key em ordem de criação"""
    cur = conn.cursor()
    select = """
        SELECT seg.RDB$FIELD_NAME AS campo_origem
        FROM RDB$RELATION_CONSTRAINTS rc
        JOIN RDB$INDEX_SEGMENTS seg ON rc.RDB$INDEX_NAME = seg.RDB$INDEX_NAME
        JOIN RDB$INDICES idx ON rc.RDB$INDEX_NAME = idx.RDB$INDEX_NAME
        WHERE
           rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
           AND rc.rdb$constraint_name = '{}'
        ORDER BY
           seg.RDB$FIELD_POSITION
    """.format(fk_name)
    cur.execute(select)
    dados = cur.fetchall()
    resultado = []
    
    for linha in dados:
        resultado.append(linha[0].strip())

    return tuple(resultado)

if __name__=="__main__":
    import fdb
    conn = fdb.connect('/ello/dados/global.ello', 'sysdba', 'masterkey')

    cria_chave_estrangeira(conn, '''\
ALTER TABLE TREGREGISTROFORMA ADD CONSTRAINT TREGREGISTROFORMA_FK6
FOREIGN KEY (EMPRESA,IDBAIXARECEBER) REFERENCES TRECBAIXA (EMPRESA,IDBAIXA)''')
