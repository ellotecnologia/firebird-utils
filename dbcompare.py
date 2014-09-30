#
# O objetivo desse script é criar uma ferramenta
# para sincronizar as estruturas de dois bancos de dados
#
import fdb
con = fdb.connect(dsn='localhost:d:/dev/dados/araujo.ello', user='sysdba', password='masterkey')
cursor = con.cursor()



deleteForeignKeys()

