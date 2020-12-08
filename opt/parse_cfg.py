import opt.data_cfg as data_cfg

"""CONFIGURATION FILE
Define tables to store in DB ['op', 'cat', 'trans']
Define column names and types for each table in DB.
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
"""

def map_bank(bank):
    """
    Map bank col names to operation_DB col names.
    Into empty cols(None) insert op_col name. DataFrame.reindex will fill these cols with NaN.
    It's important to have the same number of cols between imported file and operation _DB for easy append.
    :type op_col: list
    :type bank: dict
    :return dict
    """
    n_col = len(op_col)
    for key in bank.keys():
        cols = []
        for i in range(n_col):           ## wstawia nazwy do brakujacych kolumn dla wybranego banku
            col_name = bank[key][i]      ## podmienia None na nazwy kolumn z glownej bazy
            if col_name is None:         ## ale moze zamieniac na dowolny string
                col_name = op_col[i]
            cols.append(col_name)
        bank[key] = cols
    return bank


def add_extra_col(extra_cols, dest):
    """
    Add columns to operation_DB and bank lists
    :type extra_cols: list
    :type dest: list or dict
    :return list of lists
    """
    if type(dest) == list:
        dest.extend(extra_cols)
    if type(dest) == dict:
        for i in range(len(dest)):
            i_list = list(dest.values())[i]
            i_list.extend(extra_cols)
            key = list(dest.keys())
            dest[key[i]] = i_list
    return dest


def sql_table(col_names, types=''):
    """
    define table for SQL querry
    keep type of column equal among dbs (don't provide types for db other than op)
    :type col_names: list
    :type types: list, if missing use types from op
    :return str
    """
    if not types:
        col_type(col_names)
    sql = []
    for i in range(len(col_names)):
        sql.append(col_names[i] + ' ' + types[i])
    sql = ",".join(sql)
    return sql


# top level category (root)
GRANDPA = 'Grandpa'

# SQL DB tables names
DB_tabs = ['op', 'cat', 'trans']

op_col = data_cfg.op_col
op_col_type = data_cfg.op_col_type

# map banks col names to operation DB col names
bank = map_bank(data_cfg.bank)

extra_col = ['bank', 'hash', 'kategoria', 'cat_parent']
extra_col_type = ['TEXT', 'TEXT', 'TEXT', 'TEXT']

# add extra cols (for bank name and hash)
op_col = add_extra_col(extra_col, op_col)
op_col_type = add_extra_col(extra_col_type, op_col_type)
bank = add_extra_col(extra_col, bank)

# DB used for categorize
cat_col = ["col_name", "filter", 'filter_n', "oper", "oper_n", "kategoria"]
cat_col_type = ["TEXT", "TEXT", 'INT', "TEXT", "INT", "TEXT", "TEXT"]

cat_col_names = ["typ_transakcji", "rachunek_nadawcy", "nazwa_nadawcy", "adres_nadawcy", "rachunek_odbiorcy", "nazwa_odbiorcy", "adres_odbiorcy", "opis_transakcji", "lokalizacja", "kategoria"]

# DB used for transform operations
trans_col = ["bank", "col_name", "oper", "val1", "val2"]
trans_col_type = ["TEXT", "TEXT", "TEXT", "TEXT", "TEXT"]

# create SQL query for tables
op_col_sql = sql_table(op_col, op_col_type)
cat_col_sql = sql_table(cat_col, cat_col_type)
trans_col_sql = sql_table(trans_col, trans_col_type)

if __name__ == '__main__':
    import pandas

    df = pandas.concat([pandas.DataFrame(bank), pandas.Series(op_col, name='operations DB')], axis=1)
    print(df)
