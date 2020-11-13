"""
CONFIGURATION FILE
Define column names and types for DB.
Define column names for import files from each bank.

operations DB
    stores all operation imported from banks
    op_col <list> - column names
    op_col_type <list> - column types in op DB for store in SQLlite.

categories DB
    stores filters for each category
    cat_col <list> - column names
    cat_col_type <list> - column types in cat DB for store in SQLlite.

transformation DB
    stores transformation (+, -, *, /, str.replace) parameters during import
    trans_col <list> - column names
    trans_col_type <list> - column types in trans DB for store in SQLlite.

bank files translation <dictionary>
    Each key is a list with column names from bank import file. If NONE, the column will be empty (not existing in import file).
    Each list will be mapped against operation DB op_col

COLUMNS MAPPING:
mi@hommie:~/Dropbox/prog/python/banking> python3 data_cfg.py
                   ipko            raifeisen            raifeisen_kredyt      operations DB
0         Data operacji  Date of transaction         Date of transaction      data_operacji
1           Data waluty      Date of booking             Date of booking        data_waluty
2        Typ transakcji  Type of transaction         Type of transaction     typ_transakcji
3                 Kwota               Amount                      Amount              kwota
4                Waluta             Currency                      waluta             waluta
5   Saldo po transakcji              Balance                    saldo_po           saldo_po
6      Rachunek nadawcy     rachunek_nadawcy            rachunek_nadawcy   rachunek_nadawcy
7         Nazwa nadawcy     Sender/Recipient                        User      nazwa_nadawcy
8         Adres nadawcy        adres_nadawcy               adres_nadawcy      adres_nadawcy
9     Rachunek odbiorcy    rachunek_odbiorcy           rachunek_odbiorcy  rachunek_odbiorcy
10       Nazwa odbiorcy     Sender/Recipient              nazwa_odbiorcy     nazwa_odbiorcy
11       Adres odbiorcy       adres_odbiorcy              adres_odbiorcy     adres_odbiorcy
12      Opis transakcji                Title  Description of transaction    opis_transakcji
13          Unnamed: 13          lokalizacja                 lokalizacja        lokalizacja
14                 bank                 bank                        bank               bank
15                 hash                 hash                        hash               hash

"""
import parse_cfg as cfg

op_col = ["data_operacji", "data_waluty", "typ_transakcji", "kwota", "waluta", "saldo_po", "rachunek_nadawcy", "nazwa_nadawcy", "adres_nadawcy", "rachunek_odbiorcy", "nazwa_odbiorcy", "adres_odbiorcy", "opis_transakcji", "lokalizacja"]
op_col_type = ["TEXT", "TEXT", "TEXT", "REAL", "TEXT", "REAL", "REAL", "TEXT", "TEXT", "REAL", "TEXT", "TEXT", "TEXT", "TEXT"]
bank = {
    'ipko': ['Data operacji', 'Data waluty', 'Typ transakcji', 'Kwota', 'Waluta', 'Saldo po transakcji', 'Rachunek nadawcy', 'Nazwa nadawcy', 'Adres nadawcy', 'Rachunek odbiorcy', 'Nazwa odbiorcy', 'Adres odbiorcy', 'Opis transakcji', 'Unnamed: 13'],
    'raifeisen': ['Date of transaction', 'Date of booking', 'Type of transaction', 'Amount', 'Currency', 'Balance', None, 'Sender/Recipient', None, None, 'Sender/Recipient', None, 'Title', None],
    'raifeisen_kredyt': ['Date of transaction', 'Date of booking', 'Type of transaction', 'Amount', None, None, None, 'User', None, None, None, None, 'Description of transaction', None]
}

cat_col = ["typ_transakcji", "rachunek_nadawcy", "nazwa_nadawcy", "adres_nadawcy", "rachunek_odbiorcy", "nazwa_odbiorcy", "adres_odbiorcy", "opis_transakcji", "lokalizacja", "category", 'op_hash']
cat_col_type = ["TEXT", "REAL", "TEXT", "TEXT", "REAL", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]

trans_col = ["bank", "kolumna", "operacja", "val1", "val2"]
trans_col_type = ["TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]

extra_cols = ['bank', 'hash']
extra_cols_type = ['TEXT', 'TEXT']


# parse configuration
# map banks to operation DB
bank = cfg.bank(op_col, bank)

# add extra cols (for bank name and hash)
op_col = cfg.extra_col(extra_cols, op_col)
op_col_type = cfg.extra_col(extra_cols_type, op_col_type)
bank = cfg.extra_col(extra_cols, bank)

# create SQL query for tables
op_col_sql = cfg.sql_table(op_col, op_col_type)
cat_col_sql = cfg.sql_table(cat_col, cat_col_type)
trans_col_sql = cfg.sql_table(trans_col, trans_col_type)

if __name__ == '__main__':
    import pandas

    df = pandas.concat([pandas.DataFrame(bank), pandas.Series(op_col, name='operations DB')], axis=1)
    print(df)
