"""
COLUMNS MAPPING:
map columns from any bank to internal DB
if column not present in file from bank put None


                   ipko	             bnp	      	   operations DB
0         Data operacji		Data zlecenia operacji	   data_operacji
1           Data waluty		Data realizacji	       	   data_waluty
2        Typ transakcji		Typ transakcji			   typ_transakcji
3                 Kwota		Kwota	                   kwota
4                Waluta		Waluta	                   waluta
5   Saldo po transakcji		none				       saldo_po
6      Rachunek nadawcy		Produkt	               	   rachunek_nadawcy
7         Nazwa nadawcy		Nadawca / odbiorca         nazwa_nadawcy
8         Adres nadawcy 	none			           adres_nadawcy
9     Rachunek odbiorcy		none                       rachunek_odbiorcy
10       Nazwa odbiorcy		none	  				   nazwa_odbiorcy
11       Adres odbiorcy		none	                   adres_odbiorcy
12      Opis transakcji		Opis                  	   opis_transakcji
13          Unnamed: 13		none					   lokalizacja
14          Unnamed: 14		none					   data_czas
15          Unnamed: 15		none					   originalna_kwota
16          Unnamed: 16		none					   nr_karty

operations DB
    stores all operation imported from banks
    op_col <list> - column names
    op_col_type <list> - column types in op DB for store in SQLlite.

bank files translation <dictionary>
    Each key is a list with column names from bank import file. If NONE, the column will be empty (not existing in import file).
    Each list will be mapped against operation DB: op_col
"""

# first column MUST be a date!
op_col = ["data_operacji", "data_waluty", "typ_transakcji", "kwota", "waluta", "saldo_po", "rachunek_nadawcy", "nazwa_nadawcy", "adres_nadawcy", "rachunek_odbiorcy", "nazwa_odbiorcy", "adres_odbiorcy", "opis_transakcji", "lokalizacja", 'data_czas', 'originalna_kwota', 'nr_karty']
op_col_type = ["TIMESTAMP", "TIMESTAMP", "TEXT", "REAL", "TEXT", "REAL", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]
bank = {
    'ipko': ['Data operacji', 'Data waluty', 'Typ transakcji', 'Kwota', 'Waluta', 'Saldo po transakcji', 'Rachunek nadawcy', 'Nazwa nadawcy', 'Adres nadawcy', 'Rachunek odbiorcy', 'Nazwa odbiorcy', 'Adres odbiorcy', 'Opis transakcji', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16'],
    'bnp': ['Data zlecenia operacji', 'Data realizacji', 'Typ transakcji', 'Kwota', 'Waluta', None, 'Produkt', 'Nadawca / odbiorca', None, None, None, None, 'Opis', None, None, None, None],
    'paribas': ['Data transakcji', 'Data zaksięgowania', 'Typ transakcji', 'Kwota', 'Waluta', None, 'Produkt', 'Nadawca / odbiorca', None, None, None, None, 'Opis', None, None, None, None]
}

if __name__ == '__main__':
    import pandas

    df = pandas.concat([pandas.DataFrame(bank), pandas.Series(op_col, name='operations DB')], axis=1)
    print(df)
