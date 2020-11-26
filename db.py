import pandas
import numpy as np
import sqlite3
import os

import opt.parse_cfg as cfg


class DB:
    """
    manage data
    store in SQlite
    przyklady:
    GRANDPA <-  cat2 <- cat5\n
                        cat6\n
                        cat7\n
                cat3\n
                cat4\n
    1. filter(dane, new)\n
    2. filter_commit(cat8)\n
    GRANDPA <-  cat2 <- cat5\n
                        cat6\n
                        cat7\n
                cat3\n
                cat4\n
                cat8\n
    1. filter(cat2,new)
    2. filter(cat5,lim)
    3. filter(cat3,add)
    4. filter_commit(cat9)
    GRANDPA <-  cat2 <- cat6\n
                        cat7\n
                cat4\n
                cat9 <- cat5\n
                        cat3\n
    """
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

    def group_data(self, show_cat=True):
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

    def filter_data(self, col, cat_filter, oper='new'):
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
            return self.op[rows].copy()
        def add():
            if self.op_sub.empty:
                self.sub_op = self.op.copy() # zadziala jak oper=new, no ale jeszcze sie nic nie dzieje

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

    def filter_commit(self, cat_name):
        """cat_name moze wskazywac rowniez rodzica dla ktorego nalezy stworzyc dziecko\n
        np.: 'opłaty/telefon'
        1. free select (not only categories): create new category for all data, if some data already categorized will change it's category
        3. category select (more than one): move categories or rename if cat_name dosen't exists
        """
        #TODO: pass for each cat_name if path given
        if all(self.cat_temp.col_name == 'category'): # only categories so rename or move them
            self.__category_commit__(cat_name)
        else: # free select
            self.__free_select_commit__(cat_name)
        
        #remove rows present in op_sub
        self.op = self.op.loc[~self.op.loc[:,'hash'].isin(self.op_sub.loc[:,'hash']),:]
        # and append op_sub
        self.op = self.op.append(self.op_sub, ignore_index=True)

        # add filters to self.cat
        self.cat_temp.category = cat_name
        self.cat_temp.filter_n = self.__max__(self.cat) + 1
        self.cat = self.cat.append(self.cat_temp, ignore_index=True)

        #resetujemy tymczasowe DB
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)

        self.__validate_tree__()

        return None

    def __free_select_commit__(self, cat_name):
        """Tworzy kategorie na podstawie aktualnie wybranych danych (i filtrów użytych do otrzymania tych danych)\n
        __free_select_commit__ jest wywoływane tylko jeśli wybrane dane były filtrowane nie tylko kategoriami\n
        Jesli wybrane dane posiadja juz kategoria, zostanie on zmieniona (w efekcie moze zniknac stara kategoria jesli nie jest niczym rodzicem i nie posiada juz danych)\n
        Na podstawie kontekstu zarządza również przypisaniem do odpowiedniego rodzica.\n
        użyte filtry zapisują się do cat DB\n

        rodzic moze miec wiele dzieci, ale dziecko może mieć tylko jdengo rodzica.\n
        1. Dla wybranych danych tworzy nowa kategorie,\n
        2. jesli wybrane sa rowniez dane juz zkategorywone, zmiania im rowniez kategorie\n
        3. Jesli nazwa nowej kategorii juz istnieje, podpinamy sie do niej\n
        """
        if not isinstance(self.op_sub,pandas.DataFrame):
            # create empty category under GRANDPA
            self.op = self.op.append({'category': cat_name, 'cat_parent': 'GRANDPA', 'hash': 'empty'}, ignore_index=True)
            return
        
        parent = ['GRANDPA']
        # kategorie we wszytskich danych
        op_cat = self.op.loc[:,'category'].dropna().drop_duplicates() 
        if cat_name in op_cat.to_list(): # if cat_name is one already existing category, take this category's parent, otherway leave grandpa
            parent = self.op.loc[self.op.category == cat_name, 'cat_parent'].drop_duplicates().to_list()
        else: # only when creating new category
            # new category must have empty entry in self.op, so not to loose tree structure during some operation
            self.op = self.op.append({'category': cat_name, 'cat_parent': parent[0], 'hash': 'empty'}, ignore_index = True)

        # add category 
        self.op_sub.loc[:, 'category'] = cat_name
        # add parent 
        self.op_sub.loc[:, 'cat_parent'] = parent[0]

        return None


    def __category_commit__(self, cat_name):
        """Zmienia nazwe kategorii lub przesuwa je w drzewie\n
        __one_cat_commit__ jest wywoływany tylko, jeśli wybrane dane należ do JEDNEJ kategorii i filtrowane były tylko kategoriami\n

        1. jeśli cat_name jest nowa nazwą (nie istnieje w drzewie), przenosi wybrane dane do nowej kategorii (rename) i podpina do GRANPDA\n
        2. jeśli cat_name istnieje, wybrane dane zmieniaja kategorie na cat_name (move)
        """
        if not isinstance(self.op_sub,pandas.DataFrame):
            # create empty category under GRANDPA
            self.op = self.op.append({'category': cat_name, 'cat_parent': 'GRANDPA', 'hash': 'empty'}, ignore_index=True)
            return
        
        parent = ['GRANDPA']
        op_cat = self.op.loc[:,'category'].dropna().drop_duplicates() 
        if cat_name in op_cat.to_list(): # if cat_name is one already existing category, take this category as parent, otherway leave grandpa
            # add parent 
            self.op_sub.loc[:, 'cat_parent'] = cat_name
        else: # cat_name dosen't exists so rename categories and set to GRANDPA
            # new category must have empty entry in self.op, so not to loose tree structure during some operation
            self.op = self.op.append({'category': cat_name, 'cat_parent': parent[0], 'hash': 'empty'}, ignore_index = True)
            # add category 
            self.op_sub.loc[:, 'category'] = cat_name
            # add parent 
            self.op_sub.loc[:, 'cat_parent'] = parent[0]
        
        return None

    def __validate_tree__(self):
        ##VALIDATE TREE##
        # 1. remove empty categories which are also not referenced
        empty_cat = self.op.loc[self.op.loc[:, 'hash'] == 'empty', 'category'].drop_duplicates()
        for cat in empty_cat:
            if len(self.op.loc[self.op.loc[:, 'category'] == cat]) == 1 and self.op.loc[self.op.loc[:, 'cat_parent'] == cat].empty:
                self.op.loc[self.op.loc[:, 'category'] == cat].drop(inplace=True)
        # 2. make sure the names are unique
        all_cat = self.op.loc[:, 'category'].drop_duplicates()
        for cat in all_cat:
            pars = self.op.loc[self.op.loc[:, 'category'] == cat, 'cat_parent'].drop_duplicates()
            if len(pars) > 1:
                n = 1
                for par in pars:
                    self.op.loc[self.op.loc[:, 'category'] == cat and self.op.loc[:, 'cat_parent'] == par, 'category'] = cat + str(n)
                    n += 1
        # 3. remove duplicated rows, may be more than one empty row for cat
        self.op.drop_duplicates(inplace=True)

    def __max__(self,db):
        """max function, but also handle empty db
        """
        if not db.empty:
            return max(db.filter_n)
        else:
            return 0

    def show_tree(self):
        """ show tree structure with no of elements:\n
        GRANDPA(634)
            cat1(0)
                cat5(5)
                cat6(10)
                    cat8(8)
                cat(9)()
            cat2(10)                              
        """
        def show_kids(parent, level):
            kids = self.op.loc[self.op.cat_parent == parent, 'category'].drop_duplicates()
            level += 1
            for kid in kids:
                print('\t'*level + kid + '(' + str(len(self.op.loc[self.op.category == kid])) + ')')
                show_kids(kid, level)
        
        level = 0
        parent = 'GRANDPA'
        l = len(self.op.loc[self.op.cat_parent.isna()])
        print(parent + '(' + str(l) + ')')
        show_kids(parent, level)

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