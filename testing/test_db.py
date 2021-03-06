import os
from db import DB
from modules import FileSystem

#from: https://pycallgraph.readthedocs.io/en/master/guide/filtering.html
from pycallgraph import PyCallGraph
from pycallgraph import Config
from pycallgraph import GlobbingFilter
from pycallgraph.output import GraphvizOutput

config = Config()
config.trace_filter = GlobbingFilter(include=[
        'db.*',
        'DB.*',
        'modules.*'
])
# config.trace_filter = GlobbingFilter(include=[
#         'db.trans*',
#         'db.cat*',
#         'db.OP*',
#         'db.DB.__*',
#         'db.CAT.__',
#         'db.TRANS.add*'
# ])

#graphviz = GraphvizOutput(output_file='./testing/structure_PyCallGraph.dot', output_type='dot')
graphviz = GraphvizOutput(output_file='./testing/structure_PyCallGraph.png')

graph=False

def foo():
    fs = FileSystem()
    db = DB()

    path = './testing/history_20201123_133040.xls'
    bank = 'ipko'
    # dane.imp_data('./testing/Zestawienie operacji.xlsx', 'raifeisen')
    # dane.imp_data('./testing/Zestawienie operacji (1).xlsx', 'raifeisen_kredyt')
    fs.setIMP(path)

    read_SQL = 0

    if read_SQL:
        db.open_db(fs.getDB())
        print(db.msg)
    else:
        db.imp_data(fs.getIMP(), bank)
        print(db.msg)
        db.imp_commit('ok')
        print(db.msg)

    print(db.cat.opers())


    fltr = {db.COL_NAME: 'typ_transakcji',db.SEL: 'txt_match', db.FILTER: 'Wypłata z bankomatu', db.OPER: 'add', db.CATEGORY: 'bankomat'}
    db.cat.add(fltr=fltr)
    print(db.msg)
    print(db.op.get('bankomat'))
    print(db.cat.cat)
    print(db.tree.tree)
    
    print(db.op.sum_data('kwota','bankomat'))

    split = {db.START:'2020-08-28 00:00:00', db.END:'2020-11-20 00:00:00', db.COL_NAME: db.CATEGORY, db.FILTER: 'bankomat', db.VAL1: -20, db.DAYS: 3}
    db.split.add(split=split)

    print(db.op.sum_data('kwota','bankomat'))
    print(db.op.sum_data('kwota','split:bankomat'))

    fltr = {db.COL_NAME: 'opis_transakcji',db.SEL: 'txt_match', db.FILTER: 'ITALKI', db.OPER: 'add', db.CATEGORY: 'italki'}
    db.cat.add(fltr)
    print(db.msg)
    print(db.op.get('italki'))
    print(db.cat.cat)
    print(db.tree.tree)



    fltr = {db.COL_NAME: 'lokalizacja', db.SEL: 'txt_match',db.FILTER: 'ITALKI', db.OPER: 'add' , db.CATEGORY: 'nauka'}
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

    fltr = [{db.COL_NAME: 'lokalizacja',db.SEL: 'txt_match', db.FILTER: 'PANEK', db.OPER: 'add', db.CATEGORY: 'panek'}]
    fltr.append({db.COL_NAME: 'lokalizacja', db.SEL: 'txt_match',db.FILTER: 'PANEK', db.OPER: 'add', db.CATEGORY: 'panek'})
    db.cat.add(fltr)
    print(db.msg)
    print(db.op.get('panek'))
    print(db.cat.cat)
    print(db.tree.tree)

    fltr = [{db.COL_NAME: 'opis_transakcji',db.SEL: 'txt_match', db.FILTER: 'ITALKI', db.OPER: 'add', db.CATEGORY: 'italki'}]
    fltr.append({db.COL_NAME: 'lokalizacja',db.SEL: 'txt_match', db.FILTER: 'ITALKI', db.OPER: 'add' , db.CATEGORY: 'italki'})
    db.cat.add(fltr)
    print(db.msg)
    print(db.op.get('italki'))
    print(db.cat.cat)
    print(db.tree.tree)

    db.cat.mov(oper_n=2, new_oper_n=1, category='panek')
    print(db.msg)
    print(db.op.get('panek'))
    print(db.cat.cat)
    print(db.tree.tree)

    db.cat.mov(oper_n=2, new_oper_n=1, category='italki')
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


    fltr = {db.COL_NAME: 'typ_transakcji',db.SEL: 'txt_match', db.FILTER: 'Wypłata z bankomatu', db.OPER: 'add', db.CATEGORY: 'bankomat'}
    db.cat.add(fltr=fltr)
    print(db.msg)
    print(db.op.get('bankomat'))
    print(db.cat.cat)
    print(db.tree.tree)

    print(db.trans.opers())


    fltr = [{db.COL_NAME: 'nazwa_nadawcy',db.SEL: 'txt_match', db.FILTER: 'ALINA MAŁGORZATA OLENDER ZIELASKOWS KA', db.OPER: 'add', db.CATEGORY: 'zwrot'}]
    fltr.append({db.COL_NAME: 'kwota',db.SEL: 'greater >', db.FILTER: '1200', db.OPER: 'rem', db.CATEGORY: 'zwrot'})
    db.cat.add(fltr=fltr)
    print(db.msg)
    print(db.op.get('zwrot'))
    print(db.cat.cat)
    print(db.tree.tree)

    fltr = {db.COL_NAME: 'nazwa_nadawcy',db.SEL: 'txt_match', db.FILTER: 'ALINA MAŁGORZATA OLENDER ZIELASKOWS KA', db.OPER: 'add', db.CATEGORY: 'stypendium'}
    db.cat.add(fltr=fltr)
    print(db.msg)
    print(db.op.get('stypendium'))
    print(db.cat.cat)
    print(db.tree.tree)

    

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

if graph:
    with PyCallGraph(output=graphviz, config=config):
        foo()
else:
    foo()


