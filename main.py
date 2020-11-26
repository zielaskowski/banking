import os
import db
#import gui

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
    dane.ins_data('./testing/history_20201123_133040.xls', 'ipko')
    dane.ins_data('./testing/Zestawienie operacji.xlsx', 'raifeisen')
    dane.ins_data('./testing/Zestawienie operacji (1).xlsx', 'raifeisen_kredyt')
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
print(dane.group_data())
print(dane.group_data(show_cat=False))
print(dane.filter_data('nazwa_odbiorcy', 'VECTRA'))  # filtruje i zwraca op_sub po filtrowaniu
dane.filter_commit('internet') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- internet

print(dane.filter_data('nazwa_odbiorcy', "ADMUS"))  # filtruje i zwraca op_sub po filtrowaniu
print(dane.filter_data('nazwa_odbiorcy', "JMDI", 'add'))  # filtruje i zwraca op_sub po filtrowaniu
dane.filter_commit('opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- internet
#           opłaty            

print(dane.filter_data('category', "internet"))
dane.filter_commit('opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty <- internet
#              

print(dane.filter_data('category', "opłaty"))
print(dane.filter_data('nazwa_odbiorcy', "ADMUS",'lim'))
dane.filter_commit('czynsz') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty <- internet
#           czynsz
#           

print(dane.filter_data('category', "czynsz"))
dane.filter_commit('opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty <- internet
#                     czynsz
#           

dane.filter_commit('nowe opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty <- internet
#                     czynsz
#        <- nowe_opłaty


print(dane.filter_data('category', "internet"))
print(dane.filter_data('category', "czynsz", 'add'))
dane.filter_commit('nowe opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty 
#        <- nowe opłaty <- internet
#                          czynsz
#           

print(dane.filter_data('category', "internet"))
print(dane.filter_data('category', "czynsz", 'add'))
dane.filter_commit('opłaty') # tworzy nowa kategorie
# kategorie:
#GRANDPA <- opłaty <- internet
#                     czynsz





dane.show_tree()

print(dane.cat)

# bedzie chyba potrzebne trans_restore....

# na pewno bedzie potrzebne delete

#  potrzebujemy dodatkowe DB: cat_hash(category, category_parent, hash_list)
#  nie ma chyba potrzeby jej zapisywac w SQL - zobaczy sie
#  przy otwieraniu SQL, parsuje op_DB przez cat_DB tylko raz i wypelnia tabele cat_hash

#  zawsze, dla kazdego poziomu musi byc kategoria other
#  raczej naturalnie jest od dolu, wiec musi byc latwo tworzyc kategorie z kategorii

#  po dopisaniu nowych wierszy do kategori musi dopisac wiersze rowniez do rodzicow
