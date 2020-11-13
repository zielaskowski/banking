from sqlalchemy import create_engine
import os
import db
import gui

class FileSystem:
    ### File system management
    db_file = 'bank.s3db'
    op_file = 'history_20181114_220211_pko.xls'

    # op_file = 'TransactionHistory_20181017_221429_RaiOso.XLS'
    # op_file = 'TransactionHistory_20181114_220559RaiKr.XLS'

    def __init__(self):
        pass

fs = FileSystem()



read_SQL = 1

if read_SQL:
    dane = db.DB(fs.db_file)
else:
    dane = db.DB()
    dane.ins_data('history_20181114_220211_pko.xls', 'ipko')
    dane.ins_data('TransactionHistory_20181017_221429_RaiOso.XLS', 'raifeisen')
    dane.ins_data('TransactionHistory_20181114_220559RaiKr.XLS', 'raifeisen_kredyt')
    dane.trans_col(bank='raifeisen_kredyt', col_name='kwota', op='*', val1=-1)
    dane.trans_col(bank='ipko', col_name='opis_transakcji', op='str.replace', val1='Tytuł: ', val2='')
    dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1='Lokalizacja: ', val2='')
    dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1='Kraj: ', val2='')
    dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1=' Miasto:', val2=';')
    dane.trans_col(bank='ipko', col_name='lokalizacja', op='str.replace', val1=' Adres:', val2=';')
    dane.trans_col(bank='', col_name='typ_transakcji', op='str.replace', val1='Card transaction', val2='Płatność kartą')
    # dane.trans_all()
    print(dane.write_db(fs.db_file))

# print(dane.write_db())
# print(dane.error)

# kategoryzacja
# kiedy zaczynamy kopiujemy op do op_sub i dalej pracujemy na op_sub
# kiedy op_sub puste oznacza ze zaczynamy nowa kategorie
print(dane.count_unique())
col_name = input('col name')  # lista, dopisujemy kolejne kolumny
filter_str = input('filter_str')  # lista, dopisujemy kolejne filtry
print(dane.sub_op(col_name, filter_str))  # filtruje i zwraca op_sub po filtrowaniu
# input('ok?')
# #jesli nie
# continue
# else:
# input('cat_name')
# dane.add_cat(col_name, filter_str) #zapisuje cat DB
# input('search for sub category?")
# jesli tak to continue
# jesli nie to zeruje op_sub


# bedzie chyba potrzebne trans_restore....

# na pewno bedzie potrzebne delete

#  potrzebujemy dodatkowe DB: cat_hash(category, category_parent, hash_list)
#  nie ma chyba potrzeby jej zapisywac w SQL - zobaczy sie
#  przy otwieraniu SQL, parsuje op_DB przez cat_DB tylko raz i wypelnia tabele cat_hash

#  zawsze, dla kazdego poziomu musi byc kategoria other
#  raczej naturalnie jest od dolu, wiec musi byc latwo tworzyc kategorie z kategorii

#  po dopisaniu nowych wierszy do kategori musi dopisac wiersze rowniez do rodzicow

opłaty
-prąd
-media
--telefon
--internet
--other
transport
- moto
- rent