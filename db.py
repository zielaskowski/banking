import pandas
import numpy as np
import sqlite3
import os
import re
import inspect

import opt.parse_cfg as cfg

class COMMON:
    def __init__(self):
        self.CATEGORY = cfg.extra_col[2]
        self.BANK = cfg.extra_col[0]
        self.VAL1 = cfg.trans_col[3]
        self.VAL2 = cfg.trans_col[4]
        self.TRANS_N = cfg.trans_col[5]
        self.COL_NAME = cfg.cat_col[0]
        self.FILTER = cfg.cat_col[1]
        self.FILTER_N = cfg.cat_col[2]
        self.OPER = cfg.cat_col[3]
        self.OPER_N = cfg.cat_col[4]
        self.DATA_OPERACJI = cfg.op_col[0]
        self.HASH = cfg.extra_col[1]
        self.CATEGORY = cfg.extra_col[2]
        self.CAT_PARENT = cfg.tree_col[1]

        def transMultiply(vec, x, y):
            try: x = float(x)
            except: return
            vec_new = []
            for i in vec:
                if type(i) in [float, int]:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i * x)  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new
        def transDiv(vec: list, x: str, y: str):
            try: x = float(x)
            except: return
            return
        def transAdd(vec: list, x: str, y: str):
            try: x = float(x)
            except: return
            return
        def transSub(vec: list, x: str, y: str):
            try: x = float(x)
            except: return
            return
        def transRep(vec: list, x: str, y: str):
            vec_new = []
            for i in vec:
                if type(i) == str:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i.replace(x, y))  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        def catAdd(vec_all: 'list(str)', vec: 'list(bool)', fltr: str):
            rows = [re.findall(fltr, str(i), re.IGNORECASE) for i in vec_all]
            rows = [bool(i) for i in rows]
            return list(np.array(rows) | np.array(vec))
        def catLim(vec_all: 'list(str)', vec: 'list(bool)', fltr: str):
            rows = [re.findall(fltr, str(i), re.IGNORECASE) for i in vec_all]
            rows = [bool(i) for i in rows]
            return list(np.array(vec) & np.array(rows))
        def catRem(vec_all: 'list(str)', vec: 'list(bool)', fltr: str):
            rows = [re.findall(fltr, str(i), re.IGNORECASE) for i in vec_all]
            rows = [not bool(i) for i in rows]
            return list(np.array(vec) & ~np.array(rows))
        
        # transform and category filtering operations
        self.transOps = {'*': transMultiply,
                         '/': transDiv,
                         '+': transAdd,
                         '-': transSub,
                         'str.replace': transRep}
        self.catOps = {'add': catAdd,
                       'lim': catLim,
                       'rem': catRem}

class OP(COMMON):
    """operations DB: stores all data imported from bank plus columns:\n
    hash|category\n
    1)category column shall speed up when more than one filter in category\n
    2)hash used o avoid duplicates when importing "new" data\n
    """
    def __init__(self):
        super().__init__()
        self.op = pandas.DataFrame(columns=cfg.op_col)  # table of operations
    
    def get(self, category: str) -> pandas:
        """returns pandas db for category\n
        """
        # stores list of bools for rows assigned to curCat
        catRows = self.op.loc[:, self.CATEGORY] == category
        return self.op.loc[catRows,:]
    
    def ins(self, db):
        """adds new data to self.op
        does not update categories or transformations
        """
        self.op = self.op.append(db, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.op, subset=self.HASH, inplace=True, ignore_index=True)
    
    def __updateTrans__(self, change: [{}]):
        """reset all categories and update trans\n
        change can be one or more rows from trans DB
        """
        self.__rmCat__()
        for fltr in change:
            if fltr[self.BANK] in cfg.bank.keys():  # wybrany bank
                bankRows = self.op[self.BANK] == fltr[self.BANK]
            else:
                bankRows = [True] * len(self.op)  # wszystkie banki
            ser = self.op.loc[bankRows, fltr[self.COL_NAME]]
            ser = self.transOps[fltr[self.OPER]](ser, fltr[self.VAL1], fltr[self.VAL2])
            self.op.loc[bankRows, fltr[self.COL_NAME]] = ser

    def __updateCat__(self, change: [{}]):
        """update categories
        change can be one or more rows from cat DB
        """
        cat = ''
        ser = [False] * len(self.op)
        for fltr in change:
            if cat != fltr[self.CATEGORY]:
                self.op.loc[ser,self.CATEGORY] = cat
                cat = fltr[self.CATEGORY]
                self.__rmCat__(cat)
                ser = [False] * len(self.op)
            ser_all = self.op.loc[:, fltr[self.COL_NAME]]
            ser = self.catOps[fltr[self.OPER]](ser_all, ser, fltr[self.FILTER])
        self.op.loc[ser,self.CATEGORY] = cat

    def __rmCat__(self, category=''):
        """change category=cat to grandpa\n
        if empty category, change all
        """
        # stores list of bools for rows assigned to curCat
        if not category:
            catRows = [True] * len(self.op)
        else:
            catRows = self.op.loc[:, self.CATEGORY] == category
        self.op.loc[catRows, self.CATEGORY] = cfg.GRANDPA

class CAT(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.cat = pandas.DataFrame({self.COL_NAME: cfg.op_col[0],
                                    self.FILTER: '.*',
                                    self.FILTER_N: 0,
                                    self.OPER: list(self.catOps.keys())[0],
                                    self.OPER_N: 1,
                                    self.CATEGORY: cfg.GRANDPA}, columns=cfg.cat_col, index=[0])  # table of categories
        # start with selected grandpa category,
        # there can be only one filter which we don't want to display
        self.curCat = cfg.GRANDPA
        self.catRows = False * len(self.cat)

    def setCat(self, category:str):
        """set curent category so other methods work only on sel cat\n
        if cat=grandpa return null\n
        if cat='*' return all\n
        """
        self.curCat = category
        if category == cfg.GRANDPA:
            self.catRows = [False] * len(self.cat)
        elif category == '*':
            self.catRows = [True] * len(self.cat)
        else:
            self.catRows = self.cat.loc[:, self.CATEGORY] == category
        
    def __getitem__(self,*args) -> str:
        """equivalent of pandas.iloc\n
        work on curent category only (set by setCat())
        """
        return self.cat.loc[self.catRows,:].iloc[args]

    def __to_dict__(self, category='') -> [{}]:
        """return list of rows, each row as dic\n
        if cat not provided will use self.curCat (set by setCat())\n
        category='*' for all db
        """
        if category:
            self.setCat(category)
        return self.cat.loc[self.catRows,:].to_dict('records')

    def __update__(self, change: [{}]):
        """transform all filters\n
        change is one or more rows from trans DB
        in kwarg there is caller function name and arguments
        """
        for fltr in change:
            ser = self.cat.loc[:, self.FILTER]
            ser = self.transOps[fltr[self.OPER]](ser, fltr[self.VAL1], fltr[self.VAL2])
            self.cat.loc[:, self.FILTER] = ser
        
        self.parent.__update__(change=self.__to_dict__(category="*"),
                                    fromDB='cat')

    def opers(self) -> list:
        """return avilable filtering operations
        """
        return list(self.catOps.keys())

    def add(self, fltr: "dict|list(dict)", **kwargs):
        """add new filter, can be also used for replacement when oper_n provided\n
        also possible to pass multiple dicts in list\n
        not allowed on category in ['grandpa', '*']\n
        minimum input: {col_name: str, filter: str, oper: str}\n
        optionally:\n
        - category, otherway will use self.curCat\n
        - oper_n, will replace at position if provided, other way will add after last one
        """
        if not isinstance(fltr, list):
            fltr_list = [fltr]
        else:
            fltr_list = fltr
        for fltr in fltr_list:
            #define categories
            if self.CATEGORY in fltr.keys():
                self.setCat(fltr[self.CATEGORY])
            else:
                fltr[self.CATEGORY] = self.curCat
        
            if self.curCat in [cfg.GRANDPA, '*']:
                self.parent.msg = f'category.add: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
                return
            
            if not kwargs:
                # kwargs present only if coming from other DB, so no need to call back
                kwargs = {'op': 'add', 'parent': cfg.GRANDPA, 'child': self.curCat}
            else:
                kwargs = {}

            #define filter position
            if self.OPER_N not in fltr.keys():
                fltr[self.OPER_N] = self.__max__(self.OPER_N) + 1
                old_oper_row = pandas.DataFrame().index
            else: # mark old filter_n for later removal
                old_oper_row = (self.cat.loc[self.catRows, self.OPER_N] == fltr[self.OPER_N]).index
                kwargs = {}

            # define oper_n
            fltr[self.FILTER_N] = self.__max__(self.FILTER_N) + 1

            # validate
            cat2val = self.cat.drop(old_oper_row).append(fltr, ignore_index=True)
            valid_cat = self.__validate__(cat2val)
            if not valid_cat.empty:
                self.cat = valid_cat.copy()
                self.setCat(self.curCat) # to have the same number of rows
                self.parent.__update__(change=self.__to_dict__(), fromDB='cat',
                                        **kwargs)
            else:
                return

    def rm(self, oper_n=0, category='', **kwargs):
        """remove filter or category\n
        not allowed on category in ['grandpa', '*']\n
        if category not given will use self.curCat\n
        if oper_n=0, remove whole category
        """
        #define categories
        if category:
            self.setCat(category)
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg = f'category.rm: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
            return

        if not kwargs:
            # kwargs present only if coming from other DB, so no need to call back
            kwargs = {'op': 'rm', 'child': self.curCat}
        else:
            kwargs = {}
        
        if not oper_n:
            oper_n = self.cat.loc[self.catRows, self.OPER_N].to_list()
        else:
            oper_n = [oper_n]

        # find row to be deleted
        operRows = self.cat.loc[self.catRows, self.OPER_N].isin(oper_n)

        #validate
        valid_cat = self.__validate__(self.cat.drop(operRows.index))
        if not valid_cat.empty:
            self.cat = valid_cat.copy()
            self.setCat(self.curCat) # to have the same number of rows
            if any(self.catRows):
                # we did not delete whole category, so no need to update other DB
                kwargs = {}
            self.parent.__update__(change=self.__to_dict__(category="*"),
                                    fromDB='cat',
                                    **kwargs)

    def mv(self, oper_n:int, new_oper_n:int, category=''):
        """move filter at oper_n to new position
        """
        #define categories
        if category:
            self.setCat(category)
    
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg = f'category.mv: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
            return
        
        oper_n_row = (self.cat.loc[:, self.OPER_N] == oper_n) & self.catRows
        new_oper_n_row = (self.cat.loc[:, self.OPER_N] == new_oper_n) & self.catRows
        if not(any(oper_n_row) and any(new_oper_n_row)):
            self.parent.msg = f'category.mv: Wrong operation position'
            return

        db = self.cat.copy()
        db.loc[oper_n_row, self.OPER_N] = new_oper_n
        db.loc[new_oper_n_row, self.OPER_N] = oper_n
        
        valid_cat = self.__validate__(db)
        if not valid_cat.empty:
            self.cat = valid_cat.copy()
            self.setCat(self.curCat) # to have the same number of rows
            self.parent.__update__(change=self.__to_dict__(), fromDB='cat')

    def ren(self, new_category:str, category='', **kwargs):
        """ rename category
        """
        #define categories
        if category:
            self.setCat(category)
    
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg = f'category.mv: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
            return
        
        self.cat.loc[self.catRows, self.CATEGORY] = new_category
        self.setCat(new_category)
        
        if not kwargs:
            # kwargs present only if coming from other DB, so no need to call back
            kwargs = {'op': 'ren', 'category': category, 'new_category': new_category}
        else:
            kwargs = {}

        # validate
        valid_cat = self.__validate__(self.cat)
        if not valid_cat.empty:
            self.cat = valid_cat.copy()
            self.setCat(self.curCat) # to have the same number of rows
            self.parent.__update__(change=self.__to_dict__(),
                                    fromDB='cat',
                                    **kwargs)
    
    def __max__(self, column:str):
        """max function, but also handle empty db
        - col=oper_n: limit max to selected category
        - col=filter_n: do not count current category
        """
        if column == self.FILTER_N:
                return max(self.cat.loc[~self.catRows, column], default=0)
        else:
            return max(self.cat.loc[self.catRows, column], default=0)

    def __validate__(self, db: "pandas") -> "Null or pandas":
        """ cleaning of self.cat
        1) remove duplicates within filter & col_name, only when for oper in [add, new]
        2) first oper in cat must add which is not allowed to remove
        4) renumber oper_n within categories to avoid holes
        """
        db.sort_values(by=[self.FILTER_N, self.OPER_N], ignore_index=True, inplace=True)
        # 1) check for duplicates for 'new' and 'add' oper in whole cat db
        newAddOper = db.loc[:, self.OPER].isin(['add', 'new'])
        dup = db.loc[newAddOper, :].duplicated(subset=[self.COL_NAME, self.FILTER], keep='first')
        if any(dup.to_list()):
            self.parent.msg = f'cat.__validate__: filter already exist in {self.curCat} category'            
            return pandas.DataFrame()
        
        #2) check first row in cat
        catRows = db.loc[:, self.CATEGORY] == self.curCat
        if any(catRows):
            oper = db.loc[catRows, self.OPER].iloc[0]
        else:
            # we removed whole category, but this way we pass check
            oper = 'add'

        if oper != list(self.catOps.keys())[0]:
            self.parent.msg = f'cat.__validate__: catgeory must start with "add" operation. Not allowed to remove or move.'
            return pandas.DataFrame()

        #4) renumber oper_n
        oper_max = len(db.loc[catRows, :]) + 1
        opers_l = list(range(1, oper_max))
        db.loc[catRows, self.OPER_N] = opers_l

        db.sort_values(by=[self.FILTER_N, self.OPER_N], ignore_index=True, inplace=True)
        return db

class TRANS(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.trans = pandas.DataFrame(columns=cfg.trans_col)  # table of transformations
    
    def __getitem__(self, *args) -> str:
        """equivalent of pandas.iloc\n
        """
        return self.trans.iloc[args]

    def __to_dict__(self) -> [{}]:
        """return list of rows, each row as dic\n
        """
        return self.trans.to_dict('records')

    def add(self, fltr: "dict|list(dict)"):
        """add new transformation, can be also used for replacement when trans_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {bank: str,col_name: str, oper: str, val1: str, val2: str}\n
        optionally:\n
        - trans_n, will replace at position if provided, other way will add after last one
        """
        if not isinstance(fltr, list):
            fltr_list = [fltr]
        else:
            fltr_list = fltr
        for fltr in fltr_list:
            #define filter position
            if self.TRANS_N not in fltr.keys():
                fltr[self.TRANS_N] = self.__max__(self.TRANS_N) + 1
                old_trans_row = pandas.DataFrame().index
            else: # mark old filter_n for later removal
                old_trans_row = (self.trans.loc[:, self.TRANS_N] == fltr[self.TRANS_N]).index
            
            self.trans = self.trans.drop(old_trans_row).append(fltr, ignore_index=True)
            self.trans.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)

            self.parent.__update__(change=self.__to_dict__(), fromDB='trans')
    
    def rm(self, trans_n:int):
        """remove transformation\n
        """
        transRows = self.trans.loc[:, self.TRANS_N] == trans_n
        self.trans.drop(self.trans[transRows].index, inplace=True)

        # renumber trans_n
        self.trans.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)
        trans_max = len(self.trans) + 1
        trans_l = list(range(1, trans_max))
        self.trans.loc[:, self.TRANS_N] = trans_l
                
        self.parent.__update__(change=self.__to_dict__(), fromDB='trans')

    def mv(self, trans_n:int, new_trans_n:int):
        """move transformation at trans_n to new position
        """
        trans_n_row = self.trans.loc[:, self.TRANS_N] == trans_n
        new_trans_n_row = self.trans.loc[:, self.TRANS_N] == new_trans_n

        self.trans.loc[trans_n_row, self.TRANS_N] = new_trans_n
        self.trans.loc[new_trans_n_row, self.TRANS_N] = trans_n

        self.trans.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)
        self.parent.__update__(change=self.__to_dict__(), fromDB='trans')

    def opers(self):
        """return avilable filtering operations
        """
        return list(self.transOps.keys())
    
    def __max__(self, column:str):
        """max function, but also handle empty db
        """
        return max(self.trans.loc[:, column], default=0)
        
            
class TREE(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.par = parent
        self.tree = pandas.DataFrame({self.CATEGORY: cfg.GRANDPA,
                                    self.CAT_PARENT: '/'}, columns=cfg.tree_col, index=[0])

    def child(self, parent: str) -> list:
        childRow = self.tree.loc[:, self.CAT_PARENT] == parent
        return self.tree.loc[childRow, self.CATEGORY].to_list()

    def parent(self, child: str) -> str:
        parentRow = self.tree.loc[:, self.CATEGORY] == child
        return self.tree.loc[parentRow, self.CAT_PARENT].to_string(index=False)

    def add(self, parent: str, child: str, **kwargs):
        """adds new category (only empty)
        parent must exists
        """
        if parent not in self.tree.loc[:, self.CATEGORY].to_list():
            self.par.msg = f'parent must exists.'
            return
        
        self.tree = self.tree.append({self.CATEGORY: child, self.CAT_PARENT: parent}, ignore_index=True)
        self.tree.drop_duplicates(subset=self.CATEGORY, keep='last', inplace=True)
        return

    def ren(self, category: str, new_category: str, **kwargs):
        """rename category (also the one present in parent column)
        - can't change cat name to any of cat's child
        - can't change cat to it's parent
        """
        forbiden = self.child(category)
        forbiden.append(self.parent(category))

        if category == cfg.GRANDPA:
            self.par.msg = f'Not allowed to rename "{category}".'
            return

        if new_category in forbiden:
            self.par.msg = f'Not allowed to rename into child or parent'
            return

        if not kwargs:
            # kwargs present only if coming from other DB, so no need to call back
            kwargs = {'op': 'ren', 'category': category, 'new_category': new_category}
        else:
            kwargs = {}
        
        catRows = self.tree.loc[:, self.CATEGORY] == category
        parRows = self.tree.loc[:, self.CAT_PARENT] == category
        self.tree.loc[catRows, self.CATEGORY] = new_category
        self.tree.loc[parRows, self.CAT_PARENT] = new_category
        self.par.__update__(change= [{self.CATEGORY: category, self.CAT_PARENT: ''},
                                    {self.CATEGORY: new_category, self.CAT_PARENT: ''}],
                            fromDB='tree',
                            **kwargs)

    def mov(self, new_parent: str, child: str):
        """move child to new parent
        - can't move cat to it's parent
        - can't move cat to it's child
        """
        forbiden = self.child(child)
        forbiden.append(self.parent(child))
        if new_parent in forbiden:
            self.par.msg = f"Not allowed to move category to it's children or parent"
            return
        
        childRows = self.tree.loc[:, self.CATEGORY] == child
        self.tree.loc[childRows, self.CAT_PARENT] = new_parent

    def rm(self, child: str, *args, **kwargs):
        """rem category, if data in category it will be removed (move to Grandpa) by cat DB in parent.__update__
        children of parent move to grandpa
        """
        for i in self.child(parent=child):
            self.mov(new_parent=cfg.GRANDPA, child=i)
        
        if not kwargs:
            kwargs = {'op': 'rm', 'category': child}
        else:
            kwargs = {}

        childRows = self.tree.loc[:, self.CATEGORY] == child
        self.tree.drop(self.tree.loc[childRows].index, inplace=True)

        self.par.__update__(change= [{self.CATEGORY: child, self.CAT_PARENT: ''}], 
                            fromDB='tree',
                            **kwargs)

class IMP:
    def __init__(self):
        self.imp = {} # store raw data {bank: pandas.DataFrame,...}

    def ins(self, bank: str, db: 'pandas'):
        bank = self.__bank_n__(bank=bank)
        self.imp[bank] = db

    def pop(self):
        """remove last import
        """
        bank = list(self.imp.keys())[-1]
        self.imp.pop(bank)
        return

    def __bank_n__(self, bank: str) -> str:
        """return bank name apropriate for imp db ( with added number)
        return bankn, with n=len(bank) + 1
        """
        bnks_all = list(self.imp.keys())
        bnks = [bnk for bnk in bnks_all if re.match(f'{bank}\d+', bnk)]
        n = len(bnks)
        return bank + str(n + 1)

class DB(COMMON):
    """
    manage other DBs and stores in SQLite db\n
    imp_data(bank): import excel with operations history from bank and apply all filtering and transformation if avilable\n
    imp_comit(yes|no): append importad data to main DB\n
    write_db(file): store data into SQL\n
    update(): update op DB\n
    takes othe DBs as classes:\n
    1.db op stores data with attached categories and hash col.
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
    4. db tree stores categories hierarchy
    5. db imp{} stores raw data in dictionary, before any changes. Allowes reverse changes
    """
    def __init__(self, file=''):
        # MSG system, emits signal to parrent when self.msg change
        # MUST be the first thing class do
        self.msg_emit = False # true when connected to parent's method

        super().__init__()

        self.op = OP()
        self.cat = CAT(self)
        self.trans = TRANS(self)
        self.tree = TREE(self)
        self.imp = IMP()
        
        # during import set status and save all db until commit
        self.imp_status = False
        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()

        self.msg = ''
        if file:
            self.open_db(file)
    
    def __update__(self, change: [{}], fromDB='', **kwargs):
        """synchronize data between update DB\n
        - if fromDB='trans', update self.op and self.cat\n
            with trans where change and than all cat in op
        - if fromDB='cat', update categories in op for provided cat (but not trans)
        - if fromDB='tree', remove category from cat if needed
        tree and cat needs to know caller function and it's arguments
        """
        if fromDB.lower() == 'trans':
            self.op.__updateTrans__(change)
            self.cat.__update__(change)
            # cat.update will call self.update again with fromDB='cat'
            # so op.updateCat will update with new filters
        if fromDB.lower() == 'cat':
            # when renaming or removing category, tree shall be also updated
            if kwargs:
                exec(f"self.tree.{kwargs['op']}(**{kwargs})")
            self.op.__updateCat__(change)
        if fromDB.lower() == 'tree':
            # when renaming or removing in tree, cat shall be also updated
            if kwargs:
                exec(f"self.cat.{kwargs['op']}(**{kwargs})")
       
    def connect(self, parent):
        """refrence to caller class.\n
        This way we can call parent method when needed\n
        used for msg transport\n
        """
        self.parent=parent
        self.msg_emit = True

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name == 'msg' and self.msg_emit:
            self.parent(self.msg)

    def imp_comit(self, decision):
        if decision == 'ok':
            self.op.ins(self.op_before_imp)
            self.msg = 'Data added to DB'

        else: # not ok
            self.imp.pop()
            
            self.op.op = self.op_before_imp.copy()
            self.cat.cat = self.cat_before_imp.copy()
            self.trans.trans = self.trans_before_imp.copy()
            self.tree.tree = self.tree_before_imp.copy()

            self.msg = 'Import rejected. Restored main DB'

        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()
        self.imp_status = False

    def __impDB_bnkName__(self, op, bank):
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
        
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)  # remove cells with whitespaces
        xls = self.__correct_col_types__(xls)

        self.op_before_imp = self.op.op
        self.cat_before_imp = self.cat.cat
        self.trans_before_imp = self.trans.trans
        self.tree_before_imp = self.tree.tree
        self.op.op = xls.copy()
        self.imp.ins(bank=bank, db=xls.copy())
        
        self.__update__(change=self.trans.__to_dict__(), fromDB='trans')
        self.msg = f'Iported data. Review and confirm import.'
        return True

    def __correct_col_types__(self, df):
        if df.empty:
            return
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
                exec(f'self.{tab}.{tab} = pandas.read_sql(query, engine)')
            self.__correct_col_types__(self.op.op)
            cur = engine.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            bnks = cur.fetchall()
            bnks = [i[0] for i in bnks
                            if i[0] not in cfg.DB_tabs]
            for bnk in bnks:
                query = f'SELECT * FROM {bnk}'
                exec(f'self.imp.imp["{bnk}"] = pandas.read_sql(query, engine)')
                exec(f'self.__correct_col_types__(self.imp.imp["{bnk}"])')
        except:
            engine.close()
            self.msg = 'Not correct DB <DB.__open_db__>'
            return False
        engine.close()
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
        for bnk in self.imp.imp.keys():
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
            exec(f'''self.{tab}.{tab}.to_sql('{tab}', engine, if_exists='replace', index=False)''')
        for bnk in self.imp.imp.keys():
            exec(f'''self.imp.imp['{bnk}'].to_sql('{bnk}', engine, if_exists='replace', index=False)''')
        return

   
    