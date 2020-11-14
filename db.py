import pandas
import sqlite3
import os
import hashlib

import opt.data_cfg as data_cfg


class DB:
    # manage data
    # store in SQlite


    op = pandas.DataFrame(columns=data_cfg.op_col)  # table of operations
    op_sub = pandas.DataFrame()  # table of operation during categorization process
    cat = pandas.DataFrame(columns=data_cfg.cat_col)  # table of categories
    trans = pandas.DataFrame(columns=data_cfg.trans_col)  # table of transformations
    db_file = ""
    error = ''

    def __init__(self, file=''):
        if file:
            self.db_file = file
            self.__open_db__(file)

    def count_unique(self, show_cat=False):
        """Grupuje wartości w kazdej kolumnie i podlicza, np:\n
        typ_transakcji\n
        Płatność kartą              148\n
        Przelew z rachunku           13\n
        Zlecenie stałe                6\n
        Wypłata z bankomatu           6\n
        pokaz tez elementy ktore sa skategoryzowane, jako opcja
        """
        if self.op_sub.empty:
            self.op_sub = self.op
            if not show_cat:  # pokazuje tylko wiersze nieskategoryzowane, dziala tylko na poczatku kategoryzacji
                rows = pandas.merge(self.cat['op_hash'], self.op['hash'], how='right', left_on='op_hash',
                                    right_on='hash', indicator=True)
                rows = rows['_merge'] == 'right_only'
                self.op_sub = self.op_sub[rows]
        df_dict = {}
        for col in self.cat.columns[:-2]:  # ostatnie dwie kolumny to op_hash i category
            col_grouped = self.op_sub.groupby(by=col).count().loc[:, 'data_operacji']
            col_grouped = col_grouped.sort_values(ascending=False)
            col_grouped.name = col
            df_dict.update({col: col_grouped})
        return df_dict

    def sub_op(self, col, filtr):
        rows = self.op_sub[col] == filtr  # powinno byc ''.find()
        self.op_sub = self.op_sub[rows]
        return self.op_sub

    def ins_data(self, file, bank):
        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=self.data_cfg.bank[bank])
        xls.columns = self.data_cfg.op_col
        xls.bank = bank
        xls.hash = self.hash(xls)
        xls.replace(r'^\s+$', pandas.np.np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)
        self.op = self.op.append(xls, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.op, subset='hash', inplace=True)

    def hash(self, df):
        vec = []
        for index, row in df.iterrows():
            str_b = str(list(row)).encode()
            md5 = self.hashlib.md5(str_b)
            vec.append(md5.hexdigest())
        return vec

    def __correct_col_types__(self, df):
        n_col = len(df.columns)
        for i in range(n_col):
            num_type = self.data_cfg.op_col_type[i]
            if num_type in ['INT', 'REAL']:
                df.iloc[:, i] = self.__str2num__(df.iloc[:, i], num_type)
        return df

    @staticmethod
    def __str2num__(col, num_type):
        col_digit = []
        for i in col:
            i = str(i)
            try:
                i = float(i)
                col_digit.append(i)
            except:
                i = ''.join(x for x in i if x.isdigit() or x == '.' or x == ',')
                i = i.replace(",", ".")
                if num_type == "INT":
                    i = int(i)
                else:
                    i = float(i)
                col_digit.append(i)
        return pandas.Series(col_digit)

    def __open_db__(self, file):
        self.db_file = file
        engine = sqlite3.connect(file)
        try:
            for tab in data_cfg.DBtabs:
                query = f'SELECT * FROM {tab}'
                exec(f'self.{tab} = pandas.read_sql(query, engine)')
        except:
            self.error = 'Not correct DB <DB.__open_db__>'

    def __create_db__(self, file):
        if os.path.isfile(file):
            os.remove(file)
        self.db_file = file
        db_file = sqlite3.connect(file)
        db = db_file.cursor()
        db.execute(f'''CREATE TABLE op ({self.data_cfg.op_col_sql})''')
        db_file.commit()
        db.execute(f'''CREATE TABLE cat ({self.data_cfg.cat_col_sql})''')
        db_file.commit()
        db.execute(f'''CREATE TABLE trans ({self.data_cfg.trans_col_sql})''')
        db_file.commit()

    def write_db(self, file=''):
        if file:
            self.__create_db__(file)
            msg = f'written new DB: {file}. File overwritten if existed'
        elif not self.db_file:
            return f'no DB {file}. Nothing written.'
        else:
            msg = f'DB {file} overwritten.'
        engine = sqlite3.connect(self.db_file)
        self.op.to_sql('op', engine, if_exists='replace', index=False)
        self.cat.to_sql('cat', engine, if_exists='replace', index=False)
        self.trans.to_sql('trans', engine, if_exists='replace', index=False)
        return msg

    def trans_col(self, bank, col_name, op, val1=1, val2=''):
        def multiply(vec, x, y):
            x = float(x)
            vec_new = []
            for i in vec:
                if type(i) in [float, int]:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i * x)  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        def div(vec, x, y):
            pass

        def add(vec, x, y):
            pass

        def sub(vec, x, y):
            pass

        def rep(vec, x, y):
            vec_new = []
            for i in vec:
                if type(i) == str:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i.replace(x, y))  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        ops = {'*': multiply, '/': div, '+': add, '-': sub, 'str.replace': rep}
        if bank:  # wybrany bank
            rows = self.op['bank'] == bank
        else:
            rows = [True] * len(self.op)  # wszystkie banki
        ser = self.op.loc[rows, col_name]
        ser = ops[op](ser, val1, val2)
        self.op.loc[rows, col_name] = ser
        trans = pandas.DataFrame([[bank, col_name, op, val1, val2]], columns=self.trans.columns)
        self.trans = self.trans.append(trans)
        pandas.DataFrame.drop_duplicates(self.trans, inplace=True)

    def trans_all(self):
        for row_id in range(len(self.trans)):
            row = self.trans.iloc[row_id, :]
            self.trans_col(row['bank'], row['kolumna'], row['operacja'], row['val1'], row['val2'])