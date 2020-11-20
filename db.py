import pandas
import sqlite3
import os

import opt.parse_cfg as cfg


class DB:
    # manage data
    # store in SQlite

    def __init__(self, file=''):
        self.op = pandas.DataFrame(columns=cfg.op_col)  # table of operations
        self.op_sub = pandas.DataFrame(columns=cfg.op_col)  # table of operation during categorization process
        self.cat = pandas.DataFrame(columns=cfg.cat_col)  # table of categories
        self.trans = pandas.DataFrame(columns=cfg.trans_col)  # table of transformations
        self.hash = pandasDataFrame(columns = cfg.hash_col)  # table to store hash vs category
        self.cat_temp = pandas.DataFrame(columns=cfg.cat_col) # temporary DF colecting filters before commiting cattegory {col: filter}
        db_file = ""
        error = ''
        if file:
            self.db_file = file
            self.__open_db__(file)

    def count_unique(self, show_cat=True):
        """Grupuje wartości w kazdej kolumnie i pokazuje licznosc:\n
        kazde grupowanie przez self.sub_op ogranicza grpowanie do wczesniej wybranych\n
        self.sub_op_commit resetuje, i znowu gropwanie pokazuje dla wszystkich danych w self.op\n
        Przyklad:\n
        typ_transakcji              count\n
        Płatność kartą              148\n
        Przelew z rachunku           13\n
        Zlecenie stałe                6\n
        Wypłata z bankomatu           6\n
        opcja show_cat pokazuje elementy tylko nieskategoryzowane lub wszystkie
        """
        if self.op_sub.empty:
            return
        temp_op = self.op_sub
        if not show_cat:  # pokazuje tylko wiersze nieskategoryzowane
            hash_i = []
            [hash_i.append(i) for i in self.hash['hash']]
            rows = temp['hash'] != hash_i
            temp = temp[rows]
        else:
            cat = []
            for row in temp_op:
                hash_li = self.hash['hash'] == row['hash']
                hash_i = hash_li.index(True)
                if hash_i:
                    cat.append(self.hash.iloc[hash_i,0])
                else:
                    cat.append('')
            temp_op['category'] = cat
        df_dict = {}
        for col in self.op.columns[1:-1]:
            col_grouped = temp.groupby(by=col).count().loc[:, 'data_operacji']
            col_grouped = col_grouped.sort_values(ascending=False)
            df_dict.update({col: col_grouped})
        return df_dict

    def sub_op(self, col, cat_filter, oper='new'):
        """wyswietla dane ograniczone do op_sub[col]=filtr\n
        wybrany col i filtr dodaje do cat_dict: jesli zdecydujemy sie stworzyc kategorie\n
        z wybranych danych, cat_dict zostanie zapisany do cat db\n
        Mozliwe operacje:\n
        'new'(domyslnie): filtruje z op DB i nadpisuje sub_op\n
        'add': filtruje z op DB i dopisuje do sub_op\n
        'lim': filtruje z op_sub\n
        'rem': filtruje z op_sub i usuwa z op_sub\n
        """
        def new():
            rows = [str(i).find(cat_filter) + 1 for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op[rows]
        def add():
            rows = [str(i).find(cat_filter) + 1 for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub.append(self.op[rows])
        def lim():
            rows = [str(i).find(cat_filter) + 1 for i in self.op_sub[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub[rows]
        def rem():
            rows = [str(i).find(cat_filter) + 1 for i in self.op_sub[col]]
            rows = [not bool(i) for i in rows]
            return self.op_sub[rows]

        ops = {'new': new, 'add': add, 'lim': lim, 'rem': rem}
        self.op_sub = ops[oper]
        self.cat_temp.append({'col_name': col, 'filter':cat_filter, 'oper': oper})
        return self.op_sub

    def sub_op_commit(self, cat_name):
        """zapisuje cat_dict do cat db\n
        pozwala to kategoryzowac przyszle dane\n
        dane aktualnie znajdujace sie w db kategoryzuja sie za pomoca hash
        resetuje self.sub_op co pozwala powtorzyc proces kategoryzacji jeszcze raz
        """
        cat_row = pandas.Series()
        #add filters for cols
        for cat in self.cat_temp:
            cat_row[cat] = self.cat_temp[cat]
        # add hash
        for hash_i in self.op_sub['hash']:
            self.hash.append({'cat': hash_i})
        # add category and parrent
        if 'category' in self.cat_temp:
            cat_row['cat_parent'] = self.cat_temp['category']
        cat_row['category'] = cat_name
        self.cat = self.cat.append(cat_row, ignore_index=True)
        self.op_sub = pandas.DataFrame()
        return None

    def ins_data(self, file, bank):
        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        xls.bank = bank
        xls.hash = pandas.util.hash_pandas_object(xls)
        xls.replace(r'^\s+$', pandas.np.np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)
        self.op = self.op.append(xls, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.op, subset='hash', inplace=True)

    def __correct_col_types__(self, df):
        n_col = len(df.columns)
        for i in range(n_col):
            num_type = cfg.op_col_type[i]
            if num_type in ['INT', 'REAL']:
                df.iloc[:, i] = self.__str2num__(df.iloc[:, i], num_type)
            elif num_type == 'TEXT':
                df.iloc[:,i] = df.iloc[:,i].astype('string')
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
            for tab in cfg.DBtabs:
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
        db.execute(f'''CREATE TABLE op ({cfg.op_col_sql})''')
        db_file.commit()
        db.execute(f'''CREATE TABLE cat ({cfg.cat_col_sql})''')
        db_file.commit()
        db.execute(f'''CREATE TABLE trans ({cfg.trans_col_sql})''')
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