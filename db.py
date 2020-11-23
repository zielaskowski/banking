import pandas
import numpy as np
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
        #self.hash = pandas.DataFrame(columns = cfg.hash_col)  # table to store hash vs category
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
            temp_op = self.op
        else:
            temp_op = self.op_sub
        if not show_cat:  # pokazuje tylko wiersze nieskategoryzowane
            temp_op = self.op[self.op.category.isna()]
        df_dict = {}
        for col in cfg.cat_col_names:
            col_grouped = temp_op.groupby(by=col).count().loc[:, 'data_operacji']
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
            # moze byc tylko jedno oper=new
            self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)

            rows = [str(i).lower().find(cat_filter.lower()) + 1 for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op[rows]
        def add():
            if self.op_sub.empty:
                self.sub_op = self.op.copy # zadziala jak oper=new, no ale jeszcze sie nic nie dzieje

            rows = [str(i).lower().find(cat_filter.lower()) + 1 for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub.append(self.op[rows])
        def lim():
            if self.op_sub.empty:
                return "Nothing to lim(it). First create new request with oper=new"

            rows = [str(i).lower().find(cat_filter.lower()) + 1 for i in self.op_sub[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub[rows]
        def rem():
            if self.op_sub.empty:
                return "Nothing to rem(ove). First create new request with oper=new"

            rows = [str(i).lower().find(cat_filter.lower()) + 1 for i in self.op_sub[col]]
            rows = [not bool(i) for i in rows]
            return self.op_sub[rows]

        ops = {'new': new, 'add': add, 'lim': lim, 'rem': rem}
        self.op_sub = ops[oper]()
        if not self.cat_temp.empty:
            oper_n = 1 + max(self.cat_temp.oper_n)  # numer operacji, kolejnosc jest istotna
        else:
            oper_n = 1
        self.cat_temp = self.cat_temp.append({'col_name': col, 'filter':cat_filter, 'oper': oper, 'oper_n': oper_n}, ignore_index=True)
        return self.op_sub

    def sub_op_commit(self, cat_name):
        """Tworzy kategorie na podstawie aktualnie wybranych danych (i filtrów użytych do otrzymania tych danych)\n
        Na podstawie kontekstu zarządza również przypisaniem do odpowiedniego rodzica.\n
        użyte filtry zapisują się do cat DB\n
        hash wybranych danych razem z actegoria zapisuje sie do hash DB\n

        tworzenie rodzicow musi byc traktowane specjalnie w zależności od kontekstu:\n
        rodzic moze miec wiele dzieci, ale dziecko może mieć tylko jdengo rodzica.\n
        """
        """
        jesli wszystkie col_name = category to znaczy ze spinamy je wszystkie pod nowym rodzicem\n
        nie musimy dopisywac ketgroii do op DB, zmieniamy tylko cat DB. nie ma hash.
        przyklad:
        GRANDPA <-  cat2 <- cat5\n
                            cat6\n
                            cat7\n
                    cat3\n
                    cat4\n
        1. new cat2
        2. rem cat7
        change parent of cat5&cat6; if provided cat_name != cat2 add new category and attach to grandpa, otherway attach to cat2

        1. new cat2
        2. lim cat5
        3. add cat6
        change parent of cat5&cat6; if provided cat_name != cat2 add new category and attach to grandpa, otherway attach to cat2

        1. new cat5
        2. add cat3
        change parent of cat5&cat3; if provided cat_name != cat2 add new category and attach to grandpa, otherway attach to cat2

        jesli oprocz col_name = category mamy jeszcze inne col_name, tworzymy dziecko\n
        ostatni col_name = category uzywamy jako rodzica,\n
        usuwamy op_temp z hash i wpisujemy nowe
        jesli nie mamy col_name = category to parrent = GRANDPA
        przyklad:
        GRANDPA <-  cat2 <- cat5\n
                            cat6\n
                            cat7\n
                    cat3\n
                    cat4\n
        1. new cat2
        2. rem cat7
        3. add data
        change parent of cat5&cat6; add new category do self.cat with cat2 as parent
        """
        op_sub_cat = self.op_sub.category.dropna() # ketegorie w wybranych danych
        if not op_sub_cat.empty:
            # parents w wybranych danych
            op_temp_parents = pandas.merge(left=self.cat, right=op_sub_cat, how='right', on='category').cat_parent.dropna()
            op_temp_parents.drop_duplicates(inplace=True)
        else:
            op_temp_parents = pandas.Series()
        if op_temp_parents.empty:
            op_temp_parents = ['GRANDPA']
        
        if len(op_temp_parents) > 1: # we need to choose correct parent
            if cat_name in op_temp_parents.to_list():
                op_temp_parents = [cat_name]
            else:
                op_temp_parents = ['GRANDPA']

        # if within data are other cat set new cat as parent for them
        if not op_sub_cat.empty:
            self.cat.loc[self.cat.category.isin(op_sub_cat), 'cat_parent'] = cat_name # change parent of selected cat

        # remove catgorized rows from self.op_sub
        self.op_sub = self.op_sub[self.op_sub.loc[:,'category'].isna()]
        self.op_sub.loc[:,'category'] = cat_name

        # add parrent and category
        # dosen't make sense if only categories becouse we already update parents
        if any(self.cat_temp.loc[:,'col_name'] != 'category'):
            self.cat_temp.category = cat_name
            self.cat_temp.cat_parent = op_temp_parents[0]
            self.cat = self.cat.append(self.cat_temp, ignore_index=True)

        self.op.loc[self.op.loc[:,'hash'].isin(self.op_sub.loc[:,'hash']),'category'] = cat_name

        #resetujemy tymczasowe DB
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)
        return None

    def ins_data(self, file, bank):
        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        xls.bank = bank
        xls.hash = pandas.util.hash_pandas_object(xls)
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)  # remove cells with whitespaces
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
            for tab in cfg.DB_tabs:
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