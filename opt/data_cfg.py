"""
COLUMNS MAPPING:
map columns from any bank to internal DB
if column not present in file from bank put None

                   ipko	             raifeisen	      raifeisen_kredyt	    operations DB
0         Data operacji	Data zlecenia operacji	Data zlecenia operacji	    data_operacji
1           Data waluty	       Data realizacji	       Data realizacji	      data_waluty
2        Typ transakcji	        Typ transakcji	        Typ transakcji	   typ_transakcji
3                 Kwota	                 Kwota	                 Kwota	            kwota
4                Waluta	                Waluta	                Waluta	           waluta
5   Saldo po transakcji		              None	                  None           saldo_po
6      Rachunek nadawcy	               Produkt	               Produkt	 rachunek_nadawcy
7         Nazwa nadawcy			          None                    None      nazwa_nadawcy
8         Adres nadawcy                   None			          None      adres_nadawcy
9     Rachunek odbiorcy			          None                    None  rachunek_odbiorcy
10       Nazwa odbiorcy	    Nadawca / odbiorca	    Nadawca / odbiorca	   nazwa_odbiorcy
11       Adres odbiorcy		              None	                  None     adres_odbiorcy
12      Opis transakcji	                  Opis	                  Opis	  opis_transakcji
13          Unnamed: 13	                  none	                  None	      lokalizacja

operations DB
    stores all operation imported from banks
    op_col <list> - column names
    op_col_type <list> - column types in op DB for store in SQLlite.

bank files translation <dictionary>
    Each key is a list with column names from bank import file. If NONE, the column will be empty (not existing in import file).
    Each list will be mapped against operation DB: op_col
"""


op_col = ["data_operacji", "data_waluty", "typ_transakcji", "kwota", "waluta", "saldo_po", "rachunek_nadawcy", "nazwa_nadawcy", "adres_nadawcy", "rachunek_odbiorcy", "nazwa_odbiorcy", "adres_odbiorcy", "opis_transakcji", "lokalizacja"]
op_col_type = ["TIMESTAMP", "TIMESTAMP", "TEXT", "REAL", "TEXT", "REAL", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]
bank = {
    'ipko': ['Data operacji', 'Data waluty', 'Typ transakcji', 'Kwota', 'Waluta', 'Saldo po transakcji', 'Rachunek nadawcy', 'Nazwa nadawcy', 'Adres nadawcy', 'Rachunek odbiorcy', 'Nazwa odbiorcy', 'Adres odbiorcy', 'Opis transakcji', 'Unnamed: 13'],
    'bnp': ['Data zlecenia operacji', 'Data realizacji', 'Typ transakcji', 'Kwota', 'Waluta', None, 'Produkt', None, None, None, 'Nadawca / odbiorca', None, 'Opis', None],
    'bnp_kredyt': ['Data zlecenia operacji', 'Data realizacji', 'Typ transakcji', 'Kwota', 'Waluta', None, 'Produkt', None, None, None, 'Nadawca / odbiorca', None, 'Opis', None]
}

if __name__ == '__main__':
    import pandas

    df = pandas.concat([pandas.DataFrame(bank), pandas.Series(op_col, name='operations DB')], axis=1)
    print(df)
