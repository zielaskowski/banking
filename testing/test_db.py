import os
from db import DB
from modules import FileSystem


fs = FileSystem()
db = DB()

path = './testing/history_20201123_133040.xls'
bank = 'ipko'
# dane.imp_data('./testing/Zestawienie operacji.xlsx', 'raifeisen')
# dane.imp_data('./testing/Zestawienie operacji (1).xlsx', 'raifeisen_kredyt')
fs.setIMP(path)

read_SQL = 0

if read_SQL:
    dane = db.DB(fs.getDB())
else:
    db.imp_data(fs.getIMP(), bank)
    db.imp_comit('ok')

fltr = {db.COL_NAME: 'opis_transakcji', db.FILTER: 'ITALKI', db.OPER: 'add'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
db.filter_commit('italki')


fltr = {db.COL_NAME: 'lokalizacja', db.FILTER: 'ITALKI', db.OPER: 'add'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
db.filter_commit('nauka')

db.get_filter_cat('italki')
db.filter_commit('nauka', nameOf='parent')

db.get_filter_cat('nauka')
db.filter_commit('nauka2')


db.get_filter_cat('italki')
db.filter_commit('Grandpa')


fltr = {db.COL_NAME: 'typ_transakcji', db.FILTER: 'Wypłata z bankomatu', db.OPER: 'add'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
db.filter_commit('bankomat')


fltr = {db.COL_NAME: 'lokalizacja', db.FILTER: 'PANEK', db.OPER: 'new'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
fltr = {db.COL_NAME: 'lokalizacja', db.FILTER: 'PANEK', db.OPER: 'add'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
db.filter_commit('panek')

db.filter_commit('opłaty')

db.get_filter_cat('panek')
db.filter_commit(name='opłaty', nameOf='parent')


db.get_filter_cat('panek')
db.filter_commit(name='kom')
# now move back to parent
db.get_filter_cat('kom')
db.filter_commit('opłaty', nameOf='parent')

# remove category
db.filter_commit('skasuj')
db.get_filter_cat('skasuj')
db.filter_commit('Grandpa')



#duplicated category names
db.filter_commit('opłaty')
print(db.msg)


db.get_filter_cat('kom')
db.filter_commit('panek')


db.filter_commit('komunikacja')

db.get_filter_cat('panek')
db.filter_commit('komunikacja', nameOf='parent')

fltr = {db.COL_NAME: 'lokalizacja', db.FILTER: 'INNOGYGO', db.OPER: 'add'}
db.filter_data(col=fltr[db.COL_NAME],
                cat_filter=fltr[db.FILTER],
                oper=fltr[db.OPER])
db.filter_commit('komunikacja')

db.get_filter_cat('komunikacja')
db.filter_commit('innogy')

db.get_filter_cat('innogy')
db.filter_commit('komunikacja', nameOf='parent')

db.filter_commit('motocykle')

db.get_filter_cat('motocykle')
db.filter_commit('komunikacja', nameOf='parent')

#remove cat with data
db.get_filter_cat('innogy')
db.filter_commit('Grandpa')

#move empty cat
db.get_filter_cat('motocykle')
db.filter_commit('komunikacja', nameOf='parent')

#move empty cat
db.get_filter_cat('motocykle')
db.filter_commit('Grandpa', nameOf='parent')


#remove empty cat
db.get_filter_cat('motocykle')
db.filter_commit('Grandpa')


db.write_db(fs.getDB())



    # dane = db.DB()
    # dane.trans_col(bank='raifeisen_kredyt', col_name='kwota', op='*', val1=-1)
    # dane.trans_col(bank='ipko', col_name='opis_transakcji', op='str.replace', val1='Tytuł: ', val2='')
    # dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1='Lokalizacja: ', val2='')
    # dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1='Kraj: ', val2='')
    # dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1=' Miasto:', val2=';')
    # dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1=' Adres:', val2=';')
    # dane.trans_col(bank='', col_name='typ_transakcji', op='str.replace', val1='Card transaction', val2='Płatność kartą')
    # # dane.trans_all()
    # print(dane.write_db(fs.db_file))




