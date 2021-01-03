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
        # true when connected to parent's method
        # MUST the first thing class do
        self.msg_emit = False 
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

        # filtering operations
        self.fltrOper = {'new': 'new',
                        'add': 'add',
                        'lim': 'lim',
                        'rem': 'rem',
                        'empty': 'empty'}

        self.op = pandas.DataFrame(columns=cfg.op_col)  # table of operations
        self.op_sub = pandas.DataFrame(columns=cfg.op_col)  # table of operation during categorization process
        self.cat = pandas.DataFrame({self.COL_NAME: cfg.op_col[0],
                                    self.FILTER: '.*',
                                    self.FILTER_N: 0,
                                    self.OPER: self.fltrOper['new'],
                                    self.OPER_N: 1,
                                    self.CATEGORY: cfg.GRANDPA}, columns=cfg.cat_col, index=[0])  # table of categories
        self.cat_temp = pandas.DataFrame(columns=cfg.cat_col) # temporary DF colecting filters before commiting cattegory {col: filter}
        self.trans = pandas.DataFrame(columns=cfg.trans_col)  # table of transformations
        
        self.imp = {} # store raw data {bank: pandas.DataFrame,...}
        # during import set status and save all db until commit
        self.imp_status = False
        self.op_before_imp = pandas.DataFrame(columns=cfg.op_col)  # temporary table of operations before commiting import
        self.trans_before_imp = pandas.DataFrame(columns=cfg.trans_col)
        self.cat_before_imp = pandas.DataFrame(columns=cfg.cat_col)

        self.msg = ''
        if file:
            self.open_db(file)
    
    def connect(self, parent):
        # refrence to caller class. This way we call parent methid when needed
        self.parent=parent
        self.msg_emit = True

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == 'msg' and self.msg_emit:
            self.parent(self.msg)

    def getOP(self, not_cat = False):
        """return op DB without category description rows\n
        if not_cat = True, returns only not categorized data and different from op_sub of present
        """
        db = self.op.loc[self.op.loc[:, self.HASH] != 'empty', :].reset_index(drop=True).copy()
        if not_cat:
            db = db.loc[db.loc[:, self.CATEGORY] == cfg.GRANDPA, :].reset_index(drop=True)
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
        db = self.op_sub.loc[self.op_sub.loc[:, self.HASH] != 'empty', :].reset_index().copy()
        if db.empty:
            return self.getOP(not_cat=True)
        return db

    def group_data(self, col, db='all'):
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
        # do not categorize numbers or dates
        dtName = self.op[col].dtypes.name
        if dtName == 'datetime64[ns]' or dtName == 'float64':
            return ['n/a']
        # take data
        if db == 'all':
            temp_op = self.getOP(not_cat=True)
        else:
            temp_op = self.getOPsub()

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

        wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        """
        def new():
            # moze byc tylko jedno oper=new
            self.reset_temp_DB()
            rows = [re.findall(cat_filter, str(i), re.IGNORECASE) for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op[rows].copy()
        def add():
            # zadziala jak oper=new jesli op_sub.empty==True
            rows = [re.findall(cat_filter, str(i), re.IGNORECASE) for i in self.op[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub.append(self.op[rows])
        def lim():
            if self.op_sub.empty:
                self.msg = "Nothing to lim(it). First create new request with oper=new"
                return

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [bool(i) for i in rows]
            return self.op_sub[rows]
        def rem():
            if self.op_sub.empty:
                self.msg = "Nothing to rem(ove). First create new request with oper=new"
                return 

            rows = [re.findall(cat_filter.lower(), str(i).lower()) for i in self.op_sub[col]]
            rows = [not bool(i) for i in rows]
            return self.op_sub[rows]
        def empty():
            #self.reset_temp_DB()
            return self.op_sub.copy()
        
        ops = {self.fltrOper['new']: new,
               self.fltrOper['add']: add,
               self.fltrOper['lim']: lim,
               self.fltrOper['rem']: rem,
               self.fltrOper['empty']: empty}

        # wywolujac funkcje bez parametrow zwracamy mozliwe opareacje
        if not any([col, cat_filter, oper]):
            return list(ops.keys())

        self.op_sub = ops[oper]()

        if not self.cat_temp.empty:
            oper_n = 1 + max(self.cat_temp.oper_n)  # numer operacji, kolejnosc jest istotna
        else:
            oper_n = 1
        self.cat_temp = self.cat_temp.append({self.COL_NAME: col, self.FILTER: cat_filter, self.OPER: oper, self.OPER_N: oper_n}, ignore_index=True)
        return

    def get_filter_cat(self, cat_sel=''):
        """pass op DB through cat db and put to op_sub or op\n
        accept list of cats or string\n
        when cat specified, limit filters to cat only and store in op_sub\n
        when not cat specified, also do the commit after each category
        """
        commit = False
        # limit to cat if exists, otherway take all categories
        emptyOper = self.cat.loc[:, self.OPER] == self.fltrOper['empty']
        if not cat_sel:
            commit = True
            cat_sel = self.cat.loc[emptyOper,self.FILTER].drop_duplicates().to_list()
            # on very begining
            if not cat_sel:
                cat_sel = cfg.GRANDPA

        if not isinstance(cat_sel, list):
            cat_sel = [cat_sel]

        for cat in cat_sel:
            catRows = self.cat.loc[:, self.CATEGORY] == cat
            fltrRows = self.cat.loc[:, self.FILTER] == cat
            fltr = self.cat.loc[emptyOper & fltrRows | ~emptyOper & catRows].copy()
            # first create cat
            for row_i in range(len(fltr)):
                row = fltr.iloc[row_i,:].copy()
                # if oper='new'  change to add
                if row.loc[self.OPER] == self.fltrOper['new']:
                    row.loc[self.OPER] = self.fltrOper['add']
                self.filter_data(col=row[self.COL_NAME],
                                cat_filter=row[self.FILTER],
                                oper=row[self.OPER])
            if commit:
                self.filter_commit(cat, auto=True)
            #then move cat to parent
            # emptyOper = self.cat.loc[:, self.OPER] == self.fltrOper['empty']
            # fltrRows = self.cat.loc[:, self.FILTER] == cat
            # fltr = self.cat.loc[emptyOper & fltrRows].copy()
            # self.filter_data(col=fltr[self.COL_NAME],
            #                 cat_filter=fltr[self.FILTER],
            #                 oper=fltr[self.OPER])
            # if commit:
                self.filter_commit(fltr.loc[emptyOper, self.CATEGORY].iloc[0], nameOf= 'parent', auto=True)

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

    def filter_commit(self, name='', nameOf = 'category', auto=False):
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
        all_par = (self.op.loc[:, self.CAT_PARENT].dropna().drop_duplicates().to_list())
        [all_cat.append(i) for i in all_par]
        # if all_cat:
        #     all_cat.remove(cfg.GRANDPA)
        if not parent:
            # parent of category where we move
            if cat_name in all_cat:
                # do not create empty category which already exists
                if self.op_sub.empty:
                    self.msg = 'category already exists'
                    return
                par_parent = self.show_tree(parent=cat_name)
                parent = [par_parent[0][0]][0]
            else:
                # GRANDPAS' parent can't be GRANDPA, becouse it cause infinite loop when parsing categories
                # happens only during import new data, or creating new category
                if cat_name != cfg.GRANDPA:
                    parent = cfg.GRANDPA
                else:
                    parent = '/'
        else:
            cat_name = self.op_sub.loc[:, self.CATEGORY].drop_duplicates().to_list()
            # empty categories so cat_name is in cat_temp[filter]
            if not cat_name:
                cat_name = [self.cat_temp.loc[0, self.FILTER]]
            cat_name = cat_name[0]
            if parent not in all_cat:
                # parent shall exist
                parent = cfg.GRANDPA

        # new category must have empty entry in self.op, so not to loose tree structure during some operation
        self.op = self.op.append({self.CATEGORY: cat_name, self.CAT_PARENT: parent, self.HASH: 'empty'}, ignore_index = True)
        # old empty cat definition shall be deledet by tree validator

        # adding empty category (no data in cat_temp)
        self.filter_data(col=self.CATEGORY, cat_filter=cat_name, oper='empty')
        
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
        # do not change cat db when importing (applaing all filters on new data)
        # self cat is perfectly fine when importing...except when it dosen't exist
        # in import status=True we can categorize manually, than auto=False
        if not auto or len(self.cat) == 1:
            data_rows = self.cat_temp[self.COL_NAME] != self.CATEGORY
            self.cat_temp.loc[data_rows, self.CATEGORY] = cat_name
            self.cat_temp.loc[data_rows, self.FILTER_N] = self.__max__(self.cat) + 1
            self.cat_temp.loc[~data_rows, self.CATEGORY] = parent
            self.cat_temp.loc[~data_rows, self.FILTER_N] = self.__max__(self.cat) + 2
            self.cat = self.cat.append(self.cat_temp, ignore_index=True)

            self.__validate_cat__()

        #resetujemy tymczasowe DB
        self.reset_temp_DB()
        return
 
    def reset_temp_DB(self):
        """Reset temporary DBs
        """
        self.op_sub = pandas.DataFrame(columns = cfg.op_col)
        self.cat_temp = pandas.DataFrame(columns = cfg.cat_col)

    def __validate_tree__(self, cats):
        ##VALIDATE TREE##
        # 1. remove duplicated rows, may be more than one empty row for cat
        self.op.drop_duplicates(subset=[self.HASH, self.CATEGORY], keep='last' ,inplace=True)
        # 3. remove ctegories from self.op, which not present in cats
        hashRows = self.op.loc[:, self.HASH] == 'empty'
        op_cats = self.op.loc[hashRows, self.CATEGORY].drop_duplicates().to_list()
        for cat in op_cats:
            if cat not in cats:
                catRow = self.op.loc[:, self.CATEGORY] == cat
                ind = self.op.loc[hashRows & catRow].index
                self.op.drop(ind, inplace = True)
        #DEBUG
        print(self.op.loc[self.op.loc[:, self.HASH] == 'empty', self.CATEGORY:self.CAT_PARENT])

    def __validate_cat__(self):
        """Do some cleaning of cat filters
        iterate through filter_n; 
        1) each category must have ONE oper=empty, and other filters
        2) merge oper=add|new within category and remove duplicates
        3) put oper=empty at the end of category, with separate filter_n
        4) category GRANDPA with oper!=empty, remove
        5) check if groups (filter_n) are identical
        6) remove categories oper != empty if cat=Grandpa
        7) renumerate categories
        8) remove cats oper=empty without parent name only grandpa can be like this
        finally validate tree in self.op
        """
        #recalc = False
        hash_tab = pandas.DataFrame(columns=['hash', 'filter_n'])
        cat_new = pandas.DataFrame(columns=cfg.cat_col)
        
        filter_n = 1
        while filter_n <= self.__max__(self.cat):
            rows_data = self.cat.loc[:, self.FILTER_N] == filter_n
            rows_filters = self.cat.loc[:, self.FILTER_N] == filter_n + 1
            data = self.cat[rows_data].copy()
            empty_cat = self.cat[rows_filters].copy()

            if data.empty and empty_cat.empty:
                # missing data at index, so take next one
                filter_n += 2
                continue

            # drop duplicates for 'new' and 'add' oper
            newAddOper = data.loc[:, self.OPER].isin(['add', 'new'])
            dup = data.loc[newAddOper, :].duplicated(subset=[self.COL_NAME, self.FILTER], keep='first')
            ind = dup.loc[dup].index
            data.drop(ind, inplace=True)

            # hash data, will use later to find duplicates
            if not data.empty:
                for row in range(len(data)):
                    if data.loc[:,self.OPER].iloc[row] in ['add', 'new', 'lim']:
                        data_DF = data.loc[:,[self.COL_NAME, self.FILTER]].iloc[[row],:]
                        data_str = data_DF.to_string(header=False, index=False, index_names=False).strip()
                        hash_tab = hash_tab.append({'hash': hash(data_str), 'filter_n': filter_n}, ignore_index=True)

            data.reset_index(inplace=True, drop=True)
            data.loc[:, self.OPER_N] = [i + 1 for i in data.index]
            data.loc[:,self.FILTER_N] = filter_n
            
            empty_cat.loc[:, self.OPER_N] = 1
            empty_cat.loc[:, self.FILTER_N] = filter_n + 1
            
            filter_n += 2
            cat_new = cat_new.append(data, ignore_index = True)
            cat_new = cat_new.append(empty_cat, ignore_index = True)

        # remove filters repetition
        grp = hash_tab.groupby("hash")
        rm_fltr = [df.reset_index(drop=True).loc[:, 'filter_n'].iloc[0:-1] for index, df in grp if len(df) > 1]
        for fltr_n in rm_fltr:
            cat_ind = cat_new.loc[:, self.FILTER_N] == fltr_n[0]
            # empty category can be deleted only if no other refrence: 
            emp_ind = cat_new.loc[:, self.FILTER_N] == fltr_n[0] + 1
            cat = cat_new.loc[emp_ind, self.FILTER].to_string(index=False).strip()
            emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
            if cat in cat_new.loc[emptyOper, self.CATEGORY].to_list():
                emp_ind = [False] * len(cat_new)

            ind = cat_new.loc[cat_ind | emp_ind].index
            cat_new.drop(ind, inplace=True)
        cat_new = cat_new.reindex(copy=True)

        # remove other but last category locators if duplicated (oper=empty)
        emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
        dup = cat_new.loc[emptyOper,: ].duplicated(subset = self.FILTER, keep='last')
        ind = dup.loc[dup].index
        cat_new.drop(ind, inplace=True)

        # remove Grandpa categories, created when deleteing category
        # leave only filter_n=1
        emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
        catGrandpa = cat_new.loc[:, self.CATEGORY] == cfg.GRANDPA
        catGrandpa.iloc[0] = False
        ind = cat_new.loc[~emptyOper & catGrandpa].index
        cat_new.drop(ind, inplace=True)

        # make sure oper=empty directly follow category
        # and parents defined before children
        emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
        cats = cat_new.loc[~emptyOper, self.CATEGORY].to_list()
        for cat in cats:
            #collect data: cat filter no, parent, cat, parent filter_no
            emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
            emptyOperCat = cat_new.loc[:, self.FILTER] == cat
            catRow = cat_new.loc[:, self.CATEGORY] == cat

            filter_n = cat_new.loc[~emptyOper & catRow, self.FILTER_N].iloc[0]
            cat_parent = cat_new.loc[emptyOper & emptyOperCat, self.CATEGORY].iloc[0]

            emptyOperCatParent = cat_new.loc[:, self.FILTER] == cat_parent
            parentFltr = cat_new.loc[emptyOper & emptyOperCatParent, self.FILTER_N]
            if parentFltr.empty:
                filter_n_parent = 0
            else:
                filter_n_parent = parentFltr.iloc[0]

            if filter_n_parent > filter_n:
                #move cat to the end, to be after parent
                filter_n = self.__max__(cat_new) + 1
            
            cat_new.loc[~emptyOper & catRow, self.FILTER_N] = filter_n
            cat_new.loc[emptyOper & emptyOperCat, self.FILTER_N] = filter_n + 1

            cat_new.sort_values(by=self.FILTER_N, ignore_index=True, inplace=True)

        # remove cat without parent (reamins after deleting cat)
        emptyOper = cat_new.loc[:, self.OPER] == self.fltrOper['empty']
        noParnet = cat_new.loc[:, self.CATEGORY] == ''
        noParnet.iloc[1] = False
        ind = cat_new.loc[emptyOper & noParnet].index
        cat_new.drop(ind, inplace=True)

        cat_new.reset_index(inplace=True, drop=True)
        self.cat = cat_new

        emptyOper = self.cat.loc[:, self.OPER] == self.fltrOper['empty']
        # remove ctegories from self.op, which not present in self.cat and duplicated names
        self.__validate_tree__(self.cat.loc[emptyOper, self.FILTER].drop_duplicates().to_list())

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
            self.op = self.op.append(self.op_before_imp, ignore_index=True)
            pandas.DataFrame.drop_duplicates(self.op, subset=[self.HASH, self.CATEGORY], inplace=True, ignore_index=True)
        else: # not ok
            emptyHash = self.op.loc[:, self.HASH] == 'empty'
            bank = self.op.loc[~emptyHash, self.BANK].iloc[0]
            self.op = self.op_before_imp.copy()
            self.cat = self.cat_before_imp.copy()
            self.trans = self.trans_before_imp.copy()
            
            self.imp.pop(self.impDB_bnkName(op='rm', bank=bank), None)

        self.op_before_imp = pandas.DataFrame(columns=cfg.op_col)
        self.imp_status = False

    def impDB_bnkName(self, op, bank):
        """return bank name apropriate for imp db ( with added number)
        - op='rm': return bankn, with n=len(bank)
        - op='add': return bankn, with n=len(bank) + 1
        """
        bnks_all = list(self.imp.keys())
        bnks = [bnk for bnk in bnks_all if re.match(f'{bank}\d+', bnk)]
        n = len(bnks)
        if op == 'rm':
            return bank + str(n)
        else:
            return bank + str(n + 1)

    def imp_data(self, file, bank):
        """import excell file, does not commit!\n
        this means creating op_before_imp for current data until commit decision
        """
        self.imp_status = True

        xls = pandas.read_excel(file)
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        # set bank name
        xls.bank = bank
        # hash data, NEVER hash again, only at very begining with bank attached
        xls.hash = pandas.util.hash_pandas_object(xls, index=False)
        # set all new data to GRANDPA category
        #xls.loc[:, self.CATEGORY] = cfg.GRANDPA
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)

        self.op_before_imp = self.op.copy()
        self.cat_before_imp = self.cat.copy()
        self.trans_before_imp = self.trans.copy()
        self.op = xls.copy()
        self.imp[self.impDB_bnkName(op='add', bank=bank)] = xls.copy()
        
        # parse through trans db
        self.trans_all()
        # parse through cat db
        self.get_filter_cat()
        self.msg = f'Iported data. Review and confirm import.'
        return True

    def __correct_col_types__(self, df):
        n_col = len(df.columns)
        for i in range(n_col):
            num_type = cfg.op_col_type[i]
            if num_type in ['INT', 'REAL']:
                df.iloc[:, i] = self.__str2num__(df.iloc[:, i], num_type)
            elif num_type == 'TEXT':
                df.iloc[:,i] = df.iloc[:,i].astype('string')
            elif num_type == 'TIMESTAMP':
                df.iloc[:,i] = pandas.to_datetime(df.iloc[:,i])
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
        #not allowed when in import mode
        if self.imp_status:
            self.msg = 'finish importing before saving DB'
            return
        engine = sqlite3.connect(file)
        try:
            for tab in cfg.DB_tabs:
                query = f'SELECT * FROM {tab}'
                exec(f'self.{tab} = pandas.read_sql(query, engine)')
            self.__correct_col_types__(self.op)
            cur = engine.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            bnks = cur.fetchall()
            bnks = [i[0] for i in bnks
                            if i[0] not in cfg.DB_tabs]
            for bnk in bnks:
                query = f'SELECT * FROM {bnk}'
                exec(f'self.imp["{bnk}"] = pandas.read_sql(query, engine)')
                exec(f'self.__correct_col_types__(self.imp["{bnk}"])')
        except:
            self.msg = 'Not correct DB <DB.__open_db__>'
            return False
        self.msg = f"opened db: {file}"
        return True

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
        #not allowed when in import mode
        if self.imp_status:
            self.msg = 'finish importing before saving DB'
            return
        if file:
            self.__create_db__(file)
            self.msg = f'written new DB: {file}. File overwritten if existed'
        elif not file:
            return f'no DB {file}. Nothing written.'
        else:
            self.msg = f'DB {file} overwritten.'
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
        # now the same for cat db
        emptyOper = self.cat.loc[:, self.OPER] == self.fltrOper['empty']
        colRows = self.cat.loc[:, self.COL_NAME] == col_name
        ser = self.cat.loc[~emptyOper & colRows, self.FILTER]
        ser = ops[op](ser, val1, val2)
        if not ser: return
        self.cat.loc[~emptyOper & colRows, self.FILTER] = ser
        trans = pandas.DataFrame([[bank, col_name, op, val1, val2]], columns=self.trans.columns)
        self.trans = self.trans.append(trans, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.trans, inplace=True, ignore_index=True)
    
    def resetCatFltr(self):
        # reset data db and parse again through trans and filters
        self.op = pandas.DataFrame(columns=cfg.op_col)
        for i in self.imp:
            self.op = self.op.append(self.imp[i], ignore_index=True)
        self.trans_all()
        self.get_filter_cat()

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
        # insert row on indicated position
        self.trans = pandas.DataFrame(np.insert(self.trans.values,new_ind,values=row.to_list(), axis=0))
        # remove row rom old position
        self.trans.drop([aft_ind], inplace=True)
        self.trans.reset_index(inplace=True, drop=True)
        self.trans.columns = cfg.trans_col

        self.resetCatFltr()

        return True

    def trans_rm(self, ind):
        # remove selected row from trans db
        self.trans.drop([ind], inplace=True)
        self.trans.reset_index(inplace=True, drop=True)
        
        self.resetCatFltr()
        
    