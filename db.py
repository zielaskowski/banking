import pandas
import numpy as np
import sqlite3
import os
import re

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
        self.cat_temp = pandas.DataFrame(columns=cfg.cat_col) # temporary DF colecting filters before commiting cattegory {col: filter}
        self.trans = pandas.DataFrame(columns=cfg.trans_col)  # table of transformations
        # DB column names refered by this class
        self.DATA_OPERACJI = cfg.op_col[0]
        self.COL_NAME = cfg.cat_col[0]
        self.FILTER = cfg.cat_col[1]
        self.OPER = cfg.cat_col[3]
        self.OPER_N = cfg.cat_col[4]
        self.HASH = cfg.extra_col[1]
        self.CATEGORY = cfg.extra_col[2]
        self.CAT_PARENT = cfg.extra_col[3]
        self.BANK = cfg.extra_col[0]
        self.VAL1 = cfg.trans_col[3]
        self.VAL2 = cfg.trans_col[4]

        error = ''
        if file:
            self.db_file = file
            self.open_db(file)
    
    def getOP(self, not_cat = False):
        """return op DB without category description rows\n
        if not_cat = True, returns only not categorized data and different from op_sub of present
        """
        db = self.op.loc[self.op.loc[:, self.HASH] != 'empty', :].reset_index(drop=True).copy()
        if not_cat:
            db = db.loc[db.loc[:, self.CATEGORY] == cfg.GRANDPA, :].reset_index(drop=True)
            db.drop(db.loc[db.loc[:, self.HASH].isin(self.op_sub.loc[:, self.HASH]),:].index, inplace=True)
            db = db.reset_index(drop=True)
        return db
    
    def getOPsub(self):
        """return op_sub DB without category description rows
        """
        return self.op_sub.loc[self.op_sub.loc[:, self.HASH] != 'empty', :].reset_index()

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
        #if self.op_sub.empty:
        temp_op = self.op
        #else:
        #    temp_op = self.op_sub
        if not show_cat:  # pokazuje tylko wiersze nieskategoryzowane
            temp_op = self.op[self.op.loc[:, self.CATEGORY] == cfg.GRANDPA]
        df_dict = {}
        for col in cfg.cat_col_names:
            col_grouped = temp_op.groupby(by=col).count().loc[:, self.DATA_OPERACJI]
            col_grouped = col_grouped.sort_values(ascending=False)
            df_dict.update({col: col_grouped})
        return df_dict

    def filter_data(self, col='', cat_filter='', oper=''):
        """wyswietla dane ograniczone do op_sub[col]=filtr\n
        dzialaja regex\n
        wybrany col i filtr dodaje do cat_dict: jesli zdecydujemy sie stworzyc kategorie\n
        z wybranych danych, cat_dict zostanie zapisany do cat db\n
        Mozliwe operacje:\n
        'new': filtruje z op DB i nadpisuje sub_op\n
        'add': filtruje z op DB i dopisuje do sub_op\n
        'lim': filtruje z op_sub\n
        'rem': filtruje z op_sub i usuwa z op_sub\n

        wywolujac funkce bez parametrow zwracamy mozliwe opareacje
        """
        def new():
            # moze byc tylko jedno oper=new
            self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)
            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op[rows].copy()
        def add():
            if self.op_sub.empty:
                self.sub_op = self.op.copy() # zadziala jak oper=new, no ale jeszcze sie nic nie dzieje
            
            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub.append(self.op[rows])
        def lim():
            if self.op_sub.empty:
                return "Nothing to lim(it). First create new request with oper=new"

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub[rows]
        def rem():
            if self.op_sub.empty:
                return "Nothing to rem(ove). First create new request with oper=new"

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [not bool(i) for i in rows]
            return self.op_sub[rows]

        ops = {'new': new, 'add': add, 'lim': lim, 'rem': rem}
        # wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        if not any([col, cat_filter, oper]):
            return ops.keys()
        self.op_sub = ops[oper]()
        # no need to look on hash=empty rows, it's just internal info
        self.op_sub.drop(self.op_sub[self.op_sub.hash == 'empty'].index, inplace=True)
        if not self.cat_temp.empty:
            oper_n = 1 + max(self.cat_temp.oper_n)  # numer operacji, kolejnosc jest istotna
        else:
            oper_n = 1
        self.cat_temp = self.cat_temp.append({self.COL_NAME: col, self.FILTER: cat_filter, self.OPER: oper, self.OPER_N: oper_n}, ignore_index=True)
        return self.op_sub

    def cat_temp_rm(self, oper_n):
        """remove selected filter operation\n
        and recalculate op_sub
        """
        if oper_n == 0:
            self.error = 'Not allowed to remove first filter'
            return
        self.cat_temp.drop(self.cat_temp.loc[self.cat_temp.loc[:, self.OPER_N] == oper_n + 1].index, inplace=True)
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        for row_i in range(len(self.cat_temp)):
            row = self.cat_temp.iloc[row_i, :]
            self.filter_data(col=row.loc[self.COL_NAME],
                            cat_filter=row.loc[self.FILTER],
                            oper=row.loc[self.OPER])

    def filter_commit(self, cat_name):
        """tworzy nowa kategorie
        1. free select (not only categories): create new category for all data, if some data already categorized will change it's category
        3. category select (more than one): move categories or rename if cat_name dosen't exists
        """
        if self.op_sub.empty:
            # create empty category under GRANDPA
            # will be deleted when filter_commit executed agin if not populated or reffered by child
            self.op = self.op.append({self.CATEGORY: cat_name, self.CAT_PARENT: cfg.GRANDPA, self.HASH: 'empty'}, ignore_index=True)
            self.show_tree()
            return self.op[self.op.loc[:,self.CATEGORY]==cat_name]

        #TODO: pass for each cat_name if path given
        if all(self.cat_temp.col_name == self.CATEGORY): # only categories so rename or move them
            self.__category_commit__(cat_name)
        else: # free select
            self.__free_select_commit__(cat_name)

        # remove rows present in op_sub
        self.op.drop(self.op.loc[self.op.loc[:, self.HASH].isin(self.op_sub.loc[:, self.HASH]),:].index, inplace=True)
        # and append op_sub
        self.op = self.op.append(self.op_sub, ignore_index=True)

        # add filters to self.cat
        self.cat_temp.loc[:, self.CATEGORY] = cat_name
        self.cat_temp.filter_n = self.__max__(self.cat) + 1
        self.cat = self.cat.append(self.cat_temp, ignore_index=True)

        #resetujemy tymczasowe DB
        self.reset_temp_DB()

        self.__validate_tree__()
        return self.op[self.op.loc[:, self.CATEGORY]==cat_name]

    def reset_temp_DB(self):
        """Reset temporary DBs
        """
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)

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
        parent = [cfg.GRANDPA]
        # kategorie we wszytskich danych
        op_cat = self.op.loc[:, self.CATEGORY].dropna().drop_duplicates()
        if cat_name in op_cat.to_list(): # if cat_name is one already existing category, take this category's parent, otherway leave grandpa
            parent = self.op.loc[self.op.loc[:, self.CATEGORY] == cat_name, self.CAT_PARENT].drop_duplicates().to_list()
        else: # only when creating new category
            # new category must have empty entry in self.op, so not to loose tree structure during some operation
            self.op = self.op.append({self.CATEGORY: cat_name, self.CAT_PARENT: parent[0], self.HASH: 'empty'}, ignore_index = True)

        # add category
        self.op_sub.loc[:, self.CATEGORY] = cat_name
        # add parent
        self.op_sub.loc[:, self.CAT_PARENT] = parent[0]

        return None

    def __category_commit__(self, cat_name):
        """Zmienia nazwe kategorii lub przesuwa je w drzewie\n
        __one_cat_commit__ jest wywoływany tylko, jeśli wybrane dane filtrowane były tylko kategoriami\n

        1. jeśli cat_name jest nowa nazwą (nie istnieje w drzewie), przenosi wybrane dane do nowej kategorii (rename) i podpina do GRANPDA\n
        2. jeśli cat_name istnieje, wybrane dane zmieniaja kategorie na cat_name (move)
        """        
        parent = [cfg.GRANDPA]
        op_sub_cat = self.op_sub.loc[:, self.CATEGORY].drop_duplicates()
        # stop if move parent to one of it kids
        for cat in op_sub_cat:
            kids = self.show_tree(parent=cat)
            kids = [i[1] for i in kids[1:]]
            if cat_name in kids:
                self.error = "Not possible to move parent under the kid"
                return

        # if cat_name is one already existing category:
        # take this category as parent
        op_cat = self.op.loc[:, self.CATEGORY].dropna().drop_duplicates()
        if cat_name in op_cat.to_list(): 
            parent = [cat_name]
            if len(op_sub_cat) > 1:
                # change category of all data
                self.op_sub.loc[:, self.CATEGORY] = cat_name
                # parent of category where we move
                par_parent = self.show_tree(parent=cat_name)
                parent= [par_parent[0][0]]
        else:
            #leave parent as grandpa
            #change category name to new category
            # new category must have empty entry in self.op, so not to loose tree structure during some operation
            self.op = self.op.append({self.CATEGORY: cat_name, self.CAT_PARENT: parent[0], self.HASH: 'empty'}, ignore_index = True)
            self.op_sub.loc[:, self.CATEGORY] = cat_name
        
        # change parent, kids will follow
        self.op_sub.loc[:, self.CAT_PARENT] = parent[0]
        # change parent where hash=empty
        for cat in op_sub_cat:
            self.op.loc[self.op.loc[:, self.CATEGORY] == cat, self.CAT_PARENT] = parent[0]
        
        return None

    def __validate_tree__(self):
        ##VALIDATE TREE##
        # 1. remove empty categories which are also not referenced
        empty_cat = self.op.loc[self.op.loc[:, self.HASH] == 'empty', self.CATEGORY].drop_duplicates()
        for cat in empty_cat:
            if len(self.op.loc[self.op.loc[:, self.CATEGORY] == cat]) == 1 and self.op.loc[self.op.loc[:, 'cat_parent'] == cat].empty:
                self.op.drop(self.op[self.op.loc[:, self.CATEGORY] == cat].index, inplace=True)
        # 2. make sure the names are unique
        all_cat = self.op.loc[:, self.CATEGORY].drop_duplicates()
        for cat in all_cat:
            pars = self.op.loc[self.op.loc[:, self.CATEGORY] == cat, self.CAT_PARENT].drop_duplicates()
            if len(pars) > 1:
                n = 1
                for par in pars:
                    self.op.loc[self.op.loc[:, self.CATEGORY] == cat and self.op.loc[:, self.CAT_PARENT] == par, self.CATEGORY] = cat + str(n)
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

    def show_tree(self, parent=cfg.GRANDPA):
        """ show tree structure starting from parent with no of elements:\n
        [['','GRANDPA',634],
         ['GRANDPA','cat1',0],
         ['cat1','cat5',5],
         ['cat1','cat6',10],
         ['cat6','cat8',8],
         ['cat1','cat9,9],
         ['GRANDPA','cat2',10]]
        """
        def show_kids(parent):
            kids = self.op.loc[self.op.loc[:, self.CAT_PARENT] == parent, self.CATEGORY].drop_duplicates()
            for kid in kids:
                l = len(self.op.loc[self.op.loc[:, self.CATEGORY] == kid]) - 1
                tree.append([parent, kid, l])
                show_kids(kid)
       
        tree = []
        par_parent = self.op.loc[self.op.loc[:, self.CATEGORY] == parent, self.CAT_PARENT].drop_duplicates()
        par_parent = list(par_parent)
        l = len(self.op.loc[self.op.loc[:, self.CATEGORY] == parent])
        tree.append([par_parent[0], parent, l])
        show_kids(parent)
        return tree

    def imp_data(self, file, bank):
        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        xls.bank = bank
        xls.hash = pandas.util.hash_pandas_object(xls)
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)
        xls.loc[:,self.CATEGORY] = cfg.GRANDPA
        # parse through trans db
        for row_n in range(len(self.trans)):
            row = self.trans.iloc[row_n,:]
            self.trans_col(bank=row.bank, col_name=row.col_name, op=row.op, val1=row.val1, val2=row.val2)
        # parse through cat db
        for cat_n in self.cat.filter_n.drop_duplicates():
            cat_sub = self.cat[self.cat.filter_n == cat_n]
            for row_n in range(len(cat_sub)):
                row = self.cat.iloc[row_n,:]
                self.filter_data(col=row.col_name, cat_filter=row.filter, oper=row.oper)
            self.filter_commit(row.loc[self.CATEGORY])
        self.op = self.op.append(xls, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.op, subset=self.HASH, inplace=True)

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

    def open_db(self, file):
        self.db_file = file
        engine = sqlite3.connect(file)
        try:
            for tab in cfg.DB_tabs:
                query = f'SELECT * FROM {tab}'
                exec(f'self.{tab} = pandas.read_sql(query, engine)')
            return 1
        except:
            self.error = 'Not correct DB <DB.__open_db__>'
            return None

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

    def trans_col(self, bank='', col_name='', op='', val1='', val2=''):
        """
        wywolujac funkce bez parametrow zwracamy mozliwe opareacje
        """
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
        # wywolujac funkce bez parametrow zwracamy mozliwe opareacje
        if not any([bank, col_name, op, val1, val2]):
            return ops.keys()
        if bank:  # wybrany bank
            rows = self.op[self.BANK] == bank
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
            self.trans_col(row[self.BANK], row[self.COL_NAME], row[self.OPER], row[self.VAL1], row[self.VAL2])