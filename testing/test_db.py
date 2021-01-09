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

read_SQL = 1

if read_SQL:
    dane = db.open_db(fs.getDB())
    print(db.msg)
else:
    db.imp_data(fs.getIMP(), bank)
    print(db.msg)
    db.imp_comit('ok')
    print(db.msg)

print(db.cat.opers())

fltr = {db.COL_NAME: 'opis_transakcji', db.FILTER: 'ITALKI', db.OPER: 'add', db.CATEGORY: 'italki'}
db.cat.add(fltr)
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)

fltr = {db.COL_NAME: 'lokalizacja', db.FILTER: 'ITALKI', db.OPER: 'add' , db.CATEGORY: 'nauka'}
db.cat.add(fltr)
print(db.msg)
print(db.op.get('nauka'))
print(db.cat.cat)
print(db.tree.tree)

db.cat.rm(oper_n=1, category='nauka')
print(db.msg)
print(db.op.get('nauka'))
print(db.cat.cat)
print(db.tree.tree)

db.cat.rm(category='italki')
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)

fltr = [{db.COL_NAME: 'lokalizacja', db.FILTER: 'PANEK', db.OPER: 'add', db.CATEGORY: 'panek'}]
fltr.append({db.COL_NAME: 'lokalizacja', db.FILTER: 'PANEK', db.OPER: 'add', db.CATEGORY: 'panek'})
db.cat.add(fltr)
print(db.msg)
print(db.op.get('panek'))
print(db.cat.cat)
print(db.tree.tree)

fltr = [{db.COL_NAME: 'opis_transakcji', db.FILTER: 'ITALKI', db.OPER: 'add', db.CATEGORY: 'italki'}]
fltr.append({db.COL_NAME: 'lokalizacja', db.FILTER: 'ITALKI', db.OPER: 'add' , db.CATEGORY: 'italki'})
db.cat.add(fltr)
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)

db.cat.mv(oper_n=2, new_oper_n=1, category='panek')
print(db.msg)
print(db.op.get('panek'))
print(db.cat.cat)
print(db.tree.tree)

db.cat.mv(oper_n=2, new_oper_n=1, category='italki')
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)


db.cat.ren(new_category='nauka2', category='nauka')
print(db.msg)
print(db.op.get('nauka'))
print(db.op.get('nauka2'))
print(db.cat.cat)
print(db.tree.tree)

db.cat.ren(new_category='italki2', category='italki')
print(db.msg)
print(db.op.get('italki'))
print(db.op.get('italki2'))
print(db.cat.cat)
print(db.tree.tree)

db.tree.add(parent='panek', child='inny_panek')
print(db.op.get('panek'))
print(db.cat.cat)
print(db.tree.tree)


db.tree.ren(category='inny_panek', new_category='skasuj')
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)

db.tree.ren(category='panek', new_category='skasuj')
print(db.msg)
print(db.op.get('italki'))
print(db.cat.cat)
print(db.tree.tree)


db.tree.mov(new_parent='skasuj', child='italki2')
print(db.msg)
print(db.op.get('italki2'))
print(db.cat.cat)
print(db.tree.tree)


db.tree.rm(child='skasuj')
print(db.msg)
print(db.op.get('italki2'))
print(db.cat.cat)
print(db.tree.tree)


fltr = {db.COL_NAME: 'typ_transakcji', db.FILTER: 'Wypłata z bankomatu', db.OPER: 'add', db.CATEGORY: 'bankomat'}
db.cat.add(fltr=fltr)
print(db.msg)
print(db.op.get('bankomat'))
print(db.cat.cat)
print(db.tree.tree)


#db.write_db(fs.getDB())


print(db.trans.opers())


trans = [{'bank': 'bnp_kredyt', 'col_name': 'kwota', 'oper': '*', 'val1': -1}]
trans.append({'bank': 'ipko', 'col_name': 'opis_transakcji', 'oper': 'str.replace', 'val1': 'Tytuł: ', 'val2': ''})
trans.append({'bank': 'ipko', 'col_name': 'lokalizacja', 'oper': 'str.replace', 'val1': 'Lokalizacja: ', 'val2': ''})
trans.append({'bank': 'ipko', 'col_name': 'lokalizacja', 'oper': 'str.replace', 'val1': 'Kraj: ', 'val2': ''})
trans.append({'bank': 'ipko', 'col_name': 'lokalizacja', 'oper': 'str.replace', 'val1': ' Miasto:', 'val2': ''})
trans.append({'bank': 'ipko', 'col_name': 'lokalizacja', 'oper': 'str.replace', 'val1': ' Adres:', 'val2': ''})
trans.append({'bank': '', 'col_name': 'typ_transakcji', 'oper': 'str.replace', 'val1': 'Card transaction', 'val2': 'Płatność kartą'})

db.trans.add(trans)
print(db.trans.trans)

db.trans.rm(trans_n=2)
print(db.trans.trans)

db.trans.mv(trans_n=2, new_trans_n=3)
print(db.trans.trans)


print(db.write_db(fs.getDB()))




