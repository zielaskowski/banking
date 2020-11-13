"""
Functions to parse data_cfg.py file
"""


def bank(op_col, bank):
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


def extra_col(extra_cols, dest):
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


def sql_table(col_names, types):
    """
    define table for SQL querry
    :type col_names: list
    :type types: list
    :return str
    """
    sql = []
    for i in range(len(col_names)):
        sql.append(col_names[i] + ' ' + types[i])
    sql = ",".join(sql)
    return sql




