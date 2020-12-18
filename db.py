import pandas
import numpy as np
import sqlite3
import os
import re

import opt.parse_cfg as cfg


class DB:
    """
    manage all aspects of data. stores in SQLite db\n
    imp_data(bank): import excel with operations history from bank and apply all filtering and transformation if avilable
    imp_comit(yes|no): append importad data to main DB
    write_db(file): store data into SQL
    1.db op stores data with attached categories.
        get_op(not_categorized = bool): reqest stored data
        group_data(column=''): group data based on occurance, sorting first most common
        filter_data(col='', filter='', operation='new|add|lim|rem'): filter data and store into temporary db, NOT commiting changes
        show_tree(col=''), show kategories structure in data
    2. db cat stores filtering operation per cattegory
        filter_commit(name, nameOf='category|parent): comit filtered data and store filters for category
        get_filter_cat(cat_sel''): set temp dbs to show data and filters for category
        filter_temp_rm(oper_n): remove selected temprary filter
        filter_mv(oper_n, direction): move selected filter
        filter_rm(oper_n): remove selected filter
    3. db trans stores data basic manipulations, mainly string replace
        trans_col(bank='', col_name='', op='+|-|*|str_repl', val1='', val2=''): transform data in selected column
        trans_mv(index, direction): move selected 
        trans_rm(index): remove selected transformation
    """
    def __init__(self, file=''):
        # DB column names refered by this class
        self.DATA_OPERACJI = cfg.op_col[0]
        self.COL_NAME = cfg.cat_col[0]
        self.FILTER = cfg.cat_col[1]
        self.FILTER_N = cfg.cat_col[2]
        self.OPER = cfg.cat_col[3]
        self.OPER_N = cfg.cat_col[4]
        self.HASH = cfg.extra_col[1]
        self.CATEGORY = cfg.extra_col[2]
        self.CAT_PARENT = cfg.extra_col[3]
        self.BANK = cfg.extra_col[0]
        self.VAL1 = cfg.trans_col[3]
        self.VAL2 = cfg.trans_col[4]

        self.op = pandas.DataFrame(columns=cfg.op_col)  # table of operations
        self.op_before_imp = pandas.DataFrame(columns=cfg.op_col)  # temporary table of operations before commiting import
        self.op_sub = pandas.DataFrame(columns=cfg.op_col)  # table of operation during categorization process
        self.cat = pandas.DataFrame({self.COL_NAME: self.CATEGORY,
                                    self.FILTER: '.+',
                                    self.FILTER_N: 1,
                                    self.OPER: 'new',
                                    self.OPER_N: 1,
                                    self.CATEGORY: cfg.GRANDPA}, columns=cfg.cat_col, index=[0])  # table of categories
        self.cat_before_imp = pandas.DataFrame(columns=cfg.cat_col)
        self.cat_temp = pandas.DataFrame(columns=cfg.cat_col) # temporary DF colecting filters before commiting cattegory {col: filter}
        self.trans = pandas.DataFrame(columns=cfg.trans_col)  # table of transformations
        self.trans_before_imp = pandas.DataFrame(columns=cfg.trans_col)

        self.imp = {} # store raw data {bank: pandas.DataFrame,...}

        self.error = ''
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
    
    def getBank(self, col, txt):
        #return bank name for txt in col
        bank = self.op.loc[self.op.loc[:, col] == txt, self.BANK]
        bank = bank.to_dict()
        return list(bank.values())[0]

    def getOPsub(self):
        """return op_sub DB without category description rows
        """
        return self.op_sub.loc[self.op_sub.loc[:, self.HASH] != 'empty', :].reset_index().copy()

    def group_data(self, col):
        """Grupuje wartości w kazdej kolumnie i pokazuje licznosc dla nie pogrupownych danych:\n
        kazde grupowanie przez self.op_sub usuwa wybrane z grpowania\n
        self.sub_op_commit resetuje, i znowu gropwanie pokazuje dla wszystkich danych w self.op\n
        Przyklad:\n
        typ_transakcji              count\n
        Płatność kartą              148\n
        Przelew z rachunku           13\n
        Zlecenie stałe                6\n
        Wypłata z bankomatu           6\n
        """
        if self.op.empty:
            return ['empty']
        # do not categorize numbers
        if isinstance(self.op.loc[0,col], float):
            return ['n/a']
        # take only not categorized
        temp_op = self.op[self.op.loc[:, self.CATEGORY] == cfg.GRANDPA].copy()
        # remove what temporary selected
        # only if not first row in cat_temp with filter=="GRANDPA"
        if not self.op_sub.empty:
            if not self.cat_temp.loc[0, self.FILTER] == cfg.GRANDPA:
                temp_op.drop(temp_op.loc[temp_op.loc[:, self.HASH].isin(self.op_sub.loc[:, self.HASH]),:].index, inplace=True)

        col_grouped = temp_op.groupby(by=col).count().loc[:, self.DATA_OPERACJI]
        col_grouped = col_grouped.sort_values(ascending=False)
        if col_grouped.empty:
            return ['None']
        col_grouped = col_grouped.to_dict()
        # keys are sentences, value are no of occurance
        # aditionally replace EOL with space inside sentences
        res = []
        for i in col_grouped:
            ii = i.replace("\n"," ")
            res.append(f'x{col_grouped[i]}:  {ii}')
        return res

    def filter_data(self, col='', cat_filter='', oper='', auto=False):
        """wyswietla dane ograniczone do op_sub[col]=filtr\n
        dzialaja regex\n
        wybrany col i filtr dodaje do cat_dict: jesli zdecydujemy sie stworzyc kategorie\n
        z wybranych danych, cat_dict zostanie zapisany do cat db\n
        Mozliwe operacje:\n
        'new': filtruje z op DB i nadpisuje sub_op\n
        'add': filtruje z op DB i dopisuje do sub_op\n
        'lim': filtruje z op_sub\n
        'rem': filtruje z op_sub i usuwa z op_sub\n

        wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        """
        def new():
            # moze byc tylko jedno oper=new
            self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)
            rows = [re.findall(cat_filter.lower()[0], str(i).lower()) for i in self.op[col]]
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
                self.error = "Nothing to lim(it). First create new request with oper=new"
                return

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub[rows]
        def rem():
            if self.op_sub.empty:
                self.error = "Nothing to rem(ove). First create new request with oper=new"
                return 

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [not bool(i) for i in rows]
            return self.op_sub[rows]

        ops = {'new': new, 'add': add, 'lim': lim, 'rem': rem}

        # wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        if not any([col, cat_filter, oper]):
            return ops.keys()

        # when starting filtering from selecting category and category exists
        # parse filter for category on data
        if not auto:
            if self.cat_temp.empty and col == self.CATEGORY:
                if cat_filter in self.cat[self.CATEGORY].to_list():
                    self.get_filter_cat(cat_filter)
                    return

        # if first oper=new on GRANDPA we need to be careful so to not rename the GRANDPA
        # if comming oper=add need to remove GRANDPA from selection and conv add->new
        # rem and limiting oper on GRANDPA theoretically make sense, just need to protect
        # renaming GRANDPA (when only categories filtered) in self.filter_commit()
        if not self.cat_temp.empty:
            if self.cat_temp.loc[0, self.FILTER] == cfg.GRANDPA:
                if oper == 'add':
                    self.filter_temp_rm(0)
                    oper = 'new'

        self.op_sub = ops[oper]()

        if not self.cat_temp.empty:
            oper_n = 1 + max(self.cat_temp.oper_n)  # numer operacji, kolejnosc jest istotna
        else:
            oper_n = 1
        self.cat_temp = self.cat_temp.append({self.COL_NAME: col, self.FILTER: cat_filter, self.OPER: oper, self.OPER_N: oper_n}, ignore_index=True)
        return self.op_sub

    def get_filter_cat(self, cat_sel=''):
        """pass op DB through cat db and put to op_sub or op\n
        accept list of cats or string\n
        when cat specified, limit filters to cat only and store in op_sub\n
        when not cat specified, also do the commit after each category
        """
        commit= False
        self.__validate_cat__()
        # limit to cat if exists, otherway take all categories
        if not cat_sel:
            commit = True
            cat_sel = self.cat.loc[:,self.CATEGORY].drop_duplicates().to_list()

        if not isinstance(cat_sel, list):
            cat_sel = [cat_sel]

        for cat in cat_sel:
            fltr = self.cat[self.cat.loc[:,self.CATEGORY] == cat].copy()
            for row_i in range(len(fltr)):
                row = fltr.iloc[row_i,:].copy()
                # if oper='new' on other than first row, change to add
                if row_i > 0 and row.loc[self.OPER] == list(self.filter_data())[0]:
                    row.loc[self.OPER] = list(self.filter_data())[1]
                self.filter_data(col=row[self.COL_NAME],
                                cat_filter=row[self.FILTER],
                                oper=row[self.OPER],
                                auto=True)
            if commit:
                self.filter_commit(fltr.loc[0,self.CATEGORY])

    def filter_temp_rm(self, oper_n):
        """remove selected filter operation\n
        and recalculate op_sub
        """
        self.cat_temp.drop(self.cat_temp.loc[self.cat_temp.loc[:, self.OPER_N] == oper_n + 1].index, inplace=True)
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        for row_i in range(len(self.cat_temp)):
            row = self.cat_temp.iloc[row_i, :]
            self.filter_data(col=row.loc[self.COL_NAME],
                            cat_filter=row.loc[self.FILTER],
                            oper=row.loc[self.OPER])

    def filter_ed(self, oper_n):
        pass

    def filter_mv(self, oper_n, dir):
        pass

    def filter_commit(self, name='', nameOf = 'category'):
        """Create new category or move to existing category\n
        - if nameOf='category', move selected data to new category (existing or create new)\n
        - if nameOf='parent', move category with children to new category(existing or GRANDPA)\n
        """
        if nameOf == 'category':
            cat_name = name
            parent = ''
        else:
            cat_name = ''
            parent = name
        
        # all categories
        all_cat = self.op.loc[:, self.CATEGORY].dropna().drop_duplicates().to_list()
        if not parent:
            # parent of category where we move
            if cat_name in all_cat:
                par_parent = self.show_tree(parent=cat_name)
                parent = [par_parent[0][0]][0]
            else:
                # GRANDPAS' parent can't be GRANDPA, becouse it cause infinite loop when parsing categories
                # happens only during import new data, other way GRANDPA in all_cat list
                if cat_name != cfg.GRANDPA:
                    parent = cfg.GRANDPA
        else:
            cat_name = self.op_sub.loc[:, self.CATEGORY].drop_duplicates().to_list()
            # empty categories to be defind with nameOf='category'
            if not cat_name:
                self.reset_temp_DB()
                return
            cat_name = cat_name[0]
            if parent not in all_cat:
                # parent shall exist
                parent = cfg.GRANDPA

        # new category must have empty entry in self.op, so not to loose tree structure during some operation
        self.op_sub = self.op_sub.append({self.CATEGORY: cat_name, self.CAT_PARENT: parent, self.HASH: 'empty'}, ignore_index = True)
        # old cat definition shall be deledet by tree validator
            
        # adding empty category (no data in op_sub)
        if len(self.op_sub) == 1:
            self.op = self.op.append(self.op_sub, ignore_index=True)        
            return
        
        # add category
        self.op_sub.loc[:, self.CATEGORY] = cat_name

        # add parent
        self.op_sub.loc[:, self.CAT_PARENT] = parent

        # remove rows present in op_sub
        op_sub_hash = self.op_sub.loc[self.op_sub.loc[:,self.HASH] != 'empty', self.HASH]
        self.op.drop(self.op.loc[self.op.loc[:, self.HASH].isin(op_sub_hash),:].index, inplace=True)
        # and append op_sub
        self.op = self.op.append(self.op_sub, ignore_index=True)

        # add filters to self.cat
        self.cat_temp.loc[:, self.CATEGORY] = cat_name
        self.cat_temp.filter_n = self.__max__(self.cat) + 1
        self.cat = self.cat.append(self.cat_temp, ignore_index=True)

        #resetujemy tymczasowe DB
        self.reset_temp_DB()

        self.__validate_tree__()
        self.__validate_cat__()
        return
 
    def reset_temp_DB(self):
        """Reset temporary DBs
        """
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)

    def __validate_tree__(self):
        ##VALIDATE TREE##
        # 1. make sure the names are unique
        all_cat = self.op.loc[:, self.CATEGORY].drop_duplicates()
        for cat in all_cat:
            pars = self.op.loc[self.op.loc[:, self.CATEGORY] == cat, self.CAT_PARENT].drop_duplicates()
            if len(pars) > 1:
                n = 1
                for par in pars:
                    cats = self.op.loc[:, self.CATEGORY] == cat
                    pars = self.op.loc[:, self.CAT_PARENT] == par
                    self.op.loc[cats & pars, self.CATEGORY] = cat + str(n)
                    n += 1
        # 2. remove empty categories which are also not referenced
        empty_cat = self.op.loc[self.op.loc[:, self.HASH] == 'empty', self.CATEGORY].drop_duplicates().to_list()
        for cat in empty_cat:
            cats = self.op.loc[:, self.CATEGORY] == cat
            pars = self.op.loc[:, self.CAT_PARENT] == cat
            if len(self.op.loc[cats]) == 1 and self.op.loc[pars].empty:
                self.op.drop(self.op[cats].index, inplace=True)
                empty_cat.remove(cat)
                # if removed cat was numbered automatically
                # find other numbered cat and remove numbering
                rmCat = re.sub('\d{1,}$','', cat, re.IGNORECASE)
                if rmCat != cat:
                    for c in empty_cat:
                        if re.match(rmCat, c):
                            self.op.loc[self.op.loc[:, self.CATEGORY] == c, self.CATEGORY] = rmCat

        # 3. remove duplicated rows, may be more than one empty row for cat
        self.op.drop_duplicates(inplace=True)
        #DEBUG
        print(self.op.loc[self.op.loc[:, self.HASH] == 'empty', self.CATEGORY:self.CAT_PARENT])

    def __validate_cat__(self):
        """Do some basic cleaning of cat filters
        """
        self.cat.drop_duplicates(subset=[self.COL_NAME, self.FILTER, self.OPER, self.CATEGORY], inplace=True)
        self.cat.sort_values(by=self.CATEGORY, inplace=True, ignore_index=True)
        #DEBUG
        print(self.cat)

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
        if self.op.empty:
            return [['','empty','0']]
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

    def imp_comit(self, decision):
        if decision == 'ok':
            self.op.append(self.op_before_imp, drop_index=True)
            pandas.DataFrame.drop_duplicates(self.op, subset=self.HASH, inplace=True, ignore_index=True)
            # parse through trans db
            self.trans_all()
            # parse through cat db
            self.get_filter_cat()
            return True
        else: # not ok
            self.op = self.op_before_imp.copy()
            self.cat = self.cat_before_imp.copy()
            self.trans = self.trans_before_imp.copy()
            self.imp[-1] = ''

        self.op_before_imp = pandas.DataFrame(columns=cfg.op_col)

    def imp_data(self, file, bank):
        """import excell file, do not commit!\n
        this means creating op_before_imp for current data until commit decision
        """
        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        xls.bank = bank
        xls.hash = pandas.util.hash_pandas_object(xls)
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)

        self.op_before_imp = self.op.copy()
        self.cat_before_imp = self.cat.copy()
        self.trans_before_imp = self.trans.copy()
        self.op = xls.copy()
        self.imp[bank] = xls.copy()
        
        # parse through trans db
        self.trans_all()
        # parse through cat db
        self.get_filter_cat()
        return True

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
        engine = sqlite3.connect(file)
        try:
            for tab in cfg.DB_tabs:
                query = f'SELECT * FROM {tab}'
                exec(f'self.{tab} = pandas.read_sql(query, engine)')
            for bnk in cfg.bank.keys():
                query = f'SELECT * FROM {bnk}'
                exec(f'self.{bnk} = pandas.read_sql(query, engine)')
            return True
        except:
            self.error = 'Not correct DB <DB.__open_db__>'
            return False

    def __create_db__(self, file):
        if os.path.isfile(file):
            os.remove(file)
        db_file = sqlite3.connect(file)
        db = db_file.cursor()
        for tab in cfg.DB_tabs:
            cols = eval(f'cfg.{tab}_col_sql')
            db.execute(f'''CREATE TABLE {tab} ({cols})''')
            db_file.commit()
        for bnk in self.imp.keys():
            db.execute(f'''CREATE TABLE {bnk} ({cfg.op_col_sql})''')
            db_file.commit()

    def write_db(self, file=''):
        if file:
            self.__create_db__(file)
            self.error = f'written new DB: {file}. File overwritten if existed'
        elif not file:
            return f'no DB {file}. Nothing written.'
        else:
            self.error = f'DB {file} overwritten.'
        engine = sqlite3.connect(file)
        for tab in cfg.DB_tabs:
            exec(f'''self.{tab}.to_sql('{tab}', engine, if_exists='replace', index=False)''')
        for bnk in self.imp.keys():
            exec(f'''self.imp['{bnk}'].to_sql('{bnk}', engine, if_exists='replace', index=False)''')
        return

    def trans_col(self, bank='', col_name='', op='', val1='', val2=''):
        """
        wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        """
        def multiply(vec, x, y):
            try: x = float(x)
            except: return
            vec_new = []
            for i in vec:
                if type(i) in [float, int]:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i * x)  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        def div(vec, x, y):
            try: x = float(x)
            except: return
            return

        def add(vec, x, y):
            try: x = float(x)
            except: return
            return

        def sub(vec, x, y):
            try: x = float(x)
            except: return
            return

        def rep(vec, x, y):
            vec_new = []
            for i in vec:
                if type(i) == str:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i.replace(x, y))  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        # do not change func position in ops, changing names is ok
        ops = {'*': multiply, '/': div, '+': add, '-': sub, 'str.replace': rep}
        # wywolujac funkce bez parametrow zwracamy mozliwe opareacje
        if not any([bank, col_name, op, val1, val2]):
            return ops.keys()
        if bank in cfg.bank.keys():  # wybrany bank
            rows = self.op[self.BANK] == bank
        else:
            rows = [True] * len(self.op)  # wszystkie banki
        ser = self.op.loc[rows, col_name]
        ser = ops[op](ser, val1, val2)
        if not ser: return # propably conversion did not go well
        self.op.loc[rows, col_name] = ser
        trans = pandas.DataFrame([[bank, col_name, op, val1, val2]], columns=self.trans.columns)
        self.trans = self.trans.append(trans, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.trans, inplace=True, ignore_index=True)
        
    def trans_all(self):
        # pass op db through all transformations
        for row_n in range(len(self.trans)):
            row = self.trans.iloc[row_n,:]
            self.trans_col(bank=row[self.BANK], col_name=row[self.COL_NAME], op=row[self.OPER], val1=row[self.VAL1], val2=row[self.VAL2])

    def trans_mv(self, ind, direction):
        row = self.trans.iloc[ind, :]
        if direction == 'up':
            if ind < 1:
                return False
            new_ind = ind - 1
            aft_ind = ind + 1
        else:
            if ind >= len(self.trans) - 1:
                return False
            new_ind = ind + 2
            aft_ind = ind
        self.trans = pandas.DataFrame(np.insert(self.trans.values,new_ind,values=row.to_list(), axis=0))
        self.trans.drop([aft_ind], inplace=True)
        self.trans.reset_index(inplace=True, drop=True)
        self.trans.columns = cfg.trans_col

        self.op = pandas.DataFrame(columns=cfg.op_col)
        for i in self.imp:
            self.op = self.op.append(self.imp[i], ignore_index=True)
        self.trans_all()
        self.get_filter_cat()
        return True

    def trans_rm(self, ind):
        # remove selected row from trans db
        self.trans.drop([ind], inplace=True)
        self.trans.reset_index(inplace=True, drop=True)
        
        self.op = pandas.DataFrame(columns=cfg.op_col)
        for i in self.imp:
            self.op = self.op.append(self.imp[i], ignore_index=True)
        self.trans_all()
        self.get_filter_cat()
    