import re
import logging

from typing import Union

from fdb.fbcore import DatabaseError
from fdb.fbcore import Connection
from fdb.schema import Constraint


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
    sql = "delete from {0} where {1}".format(nome_tabela, clausula_where)
    logging.info(sql)
    while True:
        try:
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

def compoe_clausula_where(campos: Union[tuple, list], valores: Union[tuple, list]):
    i = 0
    clausula = ''
    while i < len(campos):
        nome_campo = campos[i]
        valor = valores[i]
        if clausula == '':
            clausula = nome_campo + '=' + valor
        else:
            clausula = clausula + ' AND ' + nome_campo + '=' + valor
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


def obtem_clausula_left_join(tabela_referenciada: str, campos_dependentes, campos_refenciados, alias_r) -> str:
    clausula = ''
    qtde_campos = len(campos_dependentes)
    i = 0
    while i < qtde_campos:
        if clausula == '':
            clausula  = 'LEFT JOIN {}\n   ON {} = {} '.format(tabela_referenciada + ' ' + alias_r, campos_dependentes[i], campos_refenciados[i])
        else: 
            clausula += '\nAND {} = {} '.format(campos_dependentes[i], campos_refenciados[i])
        i += 1

    return clausula

def compoe_clausula_select(campos) -> str:
    clausula = ''
    qtde_campos = len(campos)

    while qtde_campos > 0:
        if clausula == '':
            clausula += 'SELECT\n' + campos[-qtde_campos]
        else:
            clausula += ',\n' + campos[-qtde_campos]

        qtde_campos -= 1
    
    return clausula
    
def compoe_clausula_where_2(condicionais: dict) -> str:
    clausula = ''
    
    for campo, valor in condicionais.items():
        if (valor == None): 
            op = ' '
            valor = 'IS NULL'
        elif (valor == 'IS NULL') or (valor == 'IS NOT NULL'):
            op = ' '
        else:
            op = '='

        if clausula == '':
            clausula = campo + op + str(valor)
        else:
            clausula += '\n' + ' AND ' + campo + op + str(valor)

    return clausula

def remove_registros_orfas(fk: Constraint, conn: Connection):
    alias_r = 'ref'
    tabela_refenciada = fk.partner_constraint.table.get_quoted_name()
    campos_referenciados = fk.partner_constraint.index.segment_names.copy()
    
    alias_d = 'dep'
    tabela_dependente = fk.table.get_quoted_name()
    campos_dependentes = fk.index.segment_names.copy()

    for campo in campos_referenciados:
        campos_referenciados[campos_referenciados.index(campo)] =  alias_r + '.' + campo

    for campo in campos_dependentes:
        campos_dependentes[campos_dependentes.index(campo)] =  alias_d + '.' + campo

    select = """
        {clausula_select}
        FROM {dependente} {alias}
        {clausula_join}
        WHERE
            {clausula_where}
    """
    c_select = compoe_clausula_select(campos_dependentes)
    condicionais = {}
    for campo in campos_referenciados:
        condicionais[campo] = 'IS NULL'
    for campo in campos_dependentes:
        condicionais[campo] = 'IS NOT NULL'
    c_where = compoe_clausula_where_2(condicionais)

    select = select.format(clausula_select=c_select, dependente=tabela_dependente, alias=alias_d, \
                          clausula_join=obtem_clausula_left_join(tabela_refenciada, campos_dependentes, campos_referenciados, alias_r), \
                          clausula_where=c_where)
    
    cur = conn.cursor()
    cur.execute(select)
    dados = cur.fetchallmap()

    stmt = 'DELETE FROM ' + tabela_dependente + ' WHERE {}'
    i = 0
    j = 1
    condicionais = {}
    for linha in dados:
        for campo, valor in linha.items():
            tipo_campo = fk.table.get_column(campo).datatype
            if ('VARCHAR' in tipo_campo) or ('CHAR' in tipo_campo):
                valor = repr(valor)    

            if not campo in condicionais:
                condicionais[campo]=[str(valor)]
            else:
                condicionais[campo].append(str(valor))
            i += 1

        if (i == 1500) or (j == len(dados)) :
            c_where = ''
            for key, value in condicionais.items():
                if c_where == '':
                    c_where = key + ' IN ' + '({})'
                    c_where = c_where.format(','.join(set(value)))
                else:
                    c_where += '\n AND '+ key + ' IN ' + '({})'.format(','.join(set(value)))
            # Limpa os valores dos campos que já foram executados no banco
            for value in condicionais.values():
                value.clear()
            i = 0
            logging.info('Revomendo registros orfãs: ' + stmt.format(c_where))
            conn.execute_immediate(stmt.format(c_where))
            conn.commit()
        j += 1

if __name__=="__main__":
    import fdb
    conn = fdb.connect('/ello/dados/global.ello', 'sysdba', 'masterkey')

    cria_chave_estrangeira(conn, '''\
ALTER TABLE TREGREGISTROFORMA ADD CONSTRAINT TREGREGISTROFORMA_FK6
FOREIGN KEY (EMPRESA,IDBAIXARECEBER) REFERENCES TRECBAIXA (EMPRESA,IDBAIXA)''')
