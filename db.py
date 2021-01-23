import pandas
import numpy as np
import sqlite3
import os
import re

import opt.parse_cfg as cfg

class COMMON:
    def __init__(self):
        self.CATEGORY = cfg.extra_col[2]
        self.BANK = cfg.extra_col[0]
        self.VAL1 = cfg.trans_col[3]
        self.VAL2 = cfg.trans_col[4]
        self.TRANS_N = cfg.trans_col[5]
        self.COL_NAME = cfg.cat_col[0]
        self.SEL = cfg.cat_col[1]
        self.FILTER = cfg.cat_col[2]
        self.FILTER_N = cfg.cat_col[3]
        self.OPER = cfg.cat_col[4]
        self.OPER_N = cfg.cat_col[5]
        self.HASH = cfg.extra_col[1]
        self.CATEGORY = cfg.extra_col[2]
        self.CAT_PARENT = cfg.tree_col[1]
        self.START = cfg.split_col[0]
        self.END = cfg.split_col[1]
        self.DAYS = cfg.split_col[5]
        self.SPLIT_N = cfg.split_col[6]
        self.DATA_OP = cfg.op_col[0]

        def transMultiply(vec: list, x: str, y: str) -> list:
            try: x = float(x)
            except: return
            vec_new = []
            for i in vec:
                if type(i) in [float, int]:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i * x)  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new
        def transRep(vec: list, x: str, y: str) -> list:
            vec_new = []
            for i in vec:
                if type(i) == str:  # moga byc NULL lub inne kwiatki
                    vec_new.append(i.replace(x, y))  # a nie mozemy zmienic dlugosci wektora
                else:
                    vec_new.append(i)
            return vec_new

        def catAdd(rows: 'list(str)', vec: 'list(bool)') -> list:
            return list(np.array(vec) | np.array(rows))
        def catLim(rows: 'list(str)', vec: 'list(bool)') -> list:
            return list(np.array(vec) & np.array(rows))
        def catRem(rows: 'list(str)', vec: 'list(bool)') -> list:
            return list(np.array(vec) & ~np.array(rows))
        def fltrMa(vec_all: 'list(str)', fltr: str) -> list:
            rows = [re.findall(fltr, str(i), re.IGNORECASE) for i in vec_all]
            rows = [bool(i) for i in rows]
            return rows
        def fltrGt(vec_all: 'list(str)', fltr: str) -> list:
            try:
                fltr = float(fltr)
            except:
                return [False] * len(vec_all)
            return vec_all > fltr
        def fltrLt(vec_all: 'list(str)', fltr: str) -> list:
            try:
                fltr = float(fltr)
            except:
                return [False] * len(vec_all)
            return vec_all < fltr

        # transform and category filtering operations
        self.transOps = {'*': transMultiply,
                         'str.replace': transRep}
        self.catOps = {'add': catAdd,
                       'lim': catLim,
                       'rem': catRem}
        self.catFltrs = {'txt_match': fltrMa,
                        'greater >': fltrGt,
                        'smaller <': fltrLt}



class OP(COMMON):
    """operations DB: stores all data imported from bank plus columns:\n
    hash|category\n
    1)category column shall speed up when more than one filter in category\n
    2)hash used o avoid duplicates when importing "new" data\n
    """
    def __init__(self, parent):
        super().__init__()
        self.op = pandas.DataFrame(columns=cfg.op_col)  # table of operations
        self.parent = parent
    
    def get(self, category: str) -> pandas:
        """returns pandas db for category\n
        """
        # stores list of bools for rows assigned to curCat
        catRows = self.op.loc[:, self.CATEGORY] == category
        return self.op.loc[catRows,:].copy().reset_index()
    
    def ins(self, db):
        """adds new data to self.op
        does not update categories or transformations
        """
        self.op = self.op.append(db, ignore_index=True)
        pandas.DataFrame.drop_duplicates(self.op, subset=self.HASH, inplace=True, ignore_index=True)

    def group_data(self, col: str, category=cfg.GRANDPA) -> list:
        """Grupuje wartości w kazdej kolumnie i pokazuje licznosc dla nie pogrupownych danych:\n
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
        temp_op = self.get(category=category)

        col_grouped = temp_op.groupby(by=col).count().loc[:, self.DATA_OP]
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

    def sum_data(self, op='', category='') -> 'str(int)':
        """sum whole category\n
        if op == count: return number of rows
        if op == col_name: return sum of col_name for category
        if op == '' return possible op
        """
        ops = [cfg.op_col[i] for i in range(len(cfg.op_col_type)) 
                                if cfg.op_col_type[i] in ['INT', 'REAL']]
        ops.append('count')
        if not op:
            return ops
        if op in ops:
            db = self.get(category=category)
            if op == 'count':
                return str(len(db))
            else:
                summ = int(db[op].sum())
                if summ == 0: return ''
                else: return str(summ)

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
        def grandpas():
            # set rows belong to grandpa
            # store categories or rows not belonging to grandpa
            # modify only rows belong to Grandpa for add oper
                if fltr[self.OPER] == 'add':
                    grandpas = self.op.loc[:,self.CATEGORY] == cfg.GRANDPA
                else:
                    grandpas = pandas.Series([True] * len(self.op))
                otherCat.extend(list(set(self.op.loc[ser & ~grandpas,self.CATEGORY].to_list())))
                if cat in otherCat: otherCat.remove(cat)
                return grandpas
        cat = ''
        otherCat = []
        ser = [False] * len(self.op)
        for fltr in change:
            if cat != fltr[self.CATEGORY]:
                self.op.loc[ser & grandpas(), self.CATEGORY] = cat
                
                cat = fltr[self.CATEGORY]
                if cat == cfg.GRANDPA:
                    self.__rmCat__()
                else:
                    self.__rmCat__(cat)
                ser = [False] * len(self.op)

            ser_all = self.op.loc[:, fltr[self.COL_NAME]]
            rows = self.catFltrs[fltr[self.SEL]](ser_all, fltr[self.FILTER])
            ser = self.catOps[fltr[self.OPER]](rows, ser)

        # modify only rows belong to Grandpa for add oper
        self.op.loc[ser & grandpas(), self.CATEGORY] = cat

        if otherCat:
            self.parent.msg = f'Unselected rows belonging to other categories: {otherCat}'

    def __updateSplit__(self, change: [{}]) -> dict:
        """split selected rows\n
        change can be one or more rows from split db\n
        1) hash new rows\n
        2) add "split:" to category name if != grandpa and return input for cat.add
            {col_name: col_name, function: 'txt_match' , filter: filter, oper: 'add'}
        """
        catAdd = []
        colKwota = self.sum_data()[0]

        for ch in change:
            ch = pandas.Series(ch)
            if ch[self.COL_NAME] != self.HASH:
                # make sure we have proper data types
                try:
                    ch[self.START] = pandas.to_datetime(ch[self.START])
                    ch[self.END] = pandas.to_datetime(ch[self.END])
                    ch[self.VAL1] = float(ch[self.VAL1])
                    ch[self.DAYS] = float(ch[self.DAYS])
                except:
                    return catAdd

            #get op rows
            start = self.op.loc[:, self.DATA_OP] > ch[self.START]
            end = self.op.loc[:, self.DATA_OP] < ch[self.END]
            days_n = (ch[self.END] - ch[self.START]) // ch[self.DAYS]
            cat = self.op.loc[:, ch[self.COL_NAME]] == ch[self.FILTER]
            rows = self.op.loc[start & end & cat, :].copy()
            
            if abs(rows.loc[:, colKwota].sum()) < abs(days_n.days * ch[self.VAL1]):
                return catAdd

            self.op.drop(rows.index, inplace=True)

            newKwota = rows.loc[:, colKwota].apply(lambda x: x - (ch[self.VAL1] * days_n.days) / len(rows))
            rows.loc[:, colKwota] = newKwota

            # add new rows
            for i in range(days_n.days):
                row = rows.iloc[0,:].to_dict()
                row[self.DATA_OP] = ch[self.START] + pandas.Timedelta(ch[self.DAYS] * i, unit='D')
                row[self.CATEGORY] = 'split:' + ch[self.FILTER]
                row[colKwota] = ch[self.VAL1]
                hashRow = pandas.DataFrame(row, columns=list(row.keys()), index=[0])
                hashRow = hashRow.drop([self.HASH, self.CATEGORY], axis=1)
                row[self.HASH] = pandas.util.hash_pandas_object(hashRow, index=False).to_string(index=False).strip()
                rows = rows.append(row, ignore_index=True)

            self.op = self.op.append(rows, ignore_index=True)
            catAdd.append({self.COL_NAME: ch[self.COL_NAME], 
                            self.SEL: 'txt_match',
                            self.FILTER: 'split:' + ch[self.FILTER],
                            self.OPER: 'add',
                            self.CATEGORY: 'split:' + ch[self.FILTER]})
        return catAdd


    def __rmCat__(self, category='', **kwargs):
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
                                    self.SEL: 'txt_match',
                                    self.FILTER: '.*',
                                    self.FILTER_N: 0,
                                    self.OPER: 'add',
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
        
    def __getitem__(self, *args, **kwargs) -> str:
        """equivalent of pandas.loc\n
        work on curent category only (set by setCat())
        """
        it = self.cat.loc[self.catRows,:].reset_index().loc[args[0]]
        return str(it)

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

    def __len__(self):
        return len(self.cat.loc[self.catRows,:])

    def opers(self) -> list:
        """return avilable filtering operations
        """
        return list(self.catOps.keys())
    
    def fltrs(self) -> list:
        """return avilable filters
        """
        return list(self.catFltrs.keys())

    def add(self, fltr: "dict|list(dict)", **kwargs):
        """add new filter, can be also used for replacement when oper_n provided\n
        also possible to pass multiple dicts in list\n
        not allowed on category in ['grandpa', '*']\n
        minimum input: {col_name: str, function: str, filter: str, oper: str}\n
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
            c = False
            if self.CATEGORY in fltr.keys(): c = fltr[self.CATEGORY]
            if c:
                self.setCat(fltr[self.CATEGORY])
            else:
                fltr[self.CATEGORY] = self.curCat
        
            if self.curCat in [cfg.GRANDPA, '*']:
                self.parent.msg = f'category.add: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
                return
            
            if not kwargs and not any(self.catRows): 
                # if category already exists don't touch tree
                # kwargs present only if coming from other DB, so no need to call back
                kwargs = {'op': 'add', 'parent': cfg.GRANDPA, 'child': self.curCat}
            else:
                kwargs = {}

            #define filter position
            c = False
            if self.OPER_N in fltr.keys(): c =  fltr[self.OPER_N]
            if not c:
                fltr[self.OPER_N] = self.__max__(self.OPER_N) + 1
            else: # renumber oper_n above inserting position
                fltr[self.OPER_N] = float(fltr[self.OPER_N])
                newSer = self.cat.loc[self.catRows, self.OPER_N].apply(lambda x: self.__addAbove__(x, fltr[self.OPER_N]))
                self.cat.loc[self.catRows, self.OPER_N] = newSer
                kwargs = {} # not changing whole category so no need to alert tree

            # define new filter_n or take of current category
            fltr[self.FILTER_N] = self.__max__(self.FILTER_N)

            # validate
            cat2val = self.cat.append(fltr, ignore_index=True)
            valid_cat = self.__validate__(cat2val)
            if not valid_cat.empty:
                self.cat = valid_cat
                self.setCat(self.curCat) # to have the same number of rows
                self.parent.__update__(change=self.__to_dict__(), fromDB='cat',
                                        **kwargs)
                return True
            else:
                return False

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
        operRows = self.cat.loc[:, self.OPER_N].isin(oper_n)
        dropRows = self.cat.loc[operRows & self.catRows]

        #validate
        valid_cat = self.__validate__(self.cat.drop(dropRows.index))
        if not valid_cat.empty:
            self.cat = valid_cat
            self.setCat(self.curCat) # to have the same number of rows
            if any(self.catRows):
                # we did not delete whole category, so no need to update other DB
                kwargs = {}
            self.parent.__update__(change=self.__to_dict__(category="*"),
                                    fromDB='cat',
                                    **kwargs)

    def mov(self, oper_n:int, new_oper_n:int, category=''):
        """move filter at oper_n to new position
        """
        #define categories
        if category:
            self.setCat(category)
    
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg = f'category.mov: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
            return
        
        oper_n_row = (self.cat.loc[:, self.OPER_N] == oper_n) & self.catRows
        new_oper_n_row = (self.cat.loc[:, self.OPER_N] == new_oper_n) & self.catRows
        if not(any(oper_n_row) and any(new_oper_n_row)):
            self.parent.msg = f'category.mov: Wrong operation position'
            return

        db = self.cat.copy()
        db.loc[oper_n_row, self.OPER_N] = new_oper_n
        db.loc[new_oper_n_row, self.OPER_N] = oper_n
        
        valid_cat = self.__validate__(db)
        if not valid_cat.empty:
            self.cat = valid_cat
            self.setCat(self.curCat) # to have the same number of rows
            self.parent.__update__(change=self.__to_dict__(), fromDB='cat')

    def ren(self, new_category:str, category='', **kwargs):
        """ rename category
        """
        #define categories
        if category:
            self.setCat(category)
    
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg = f'category.ren: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.'
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
            self.cat = valid_cat
            self.setCat(self.curCat) # to have the same number of rows
            self.parent.__update__(change=self.__to_dict__(),
                                    fromDB='cat',
                                    **kwargs)
    
    def __addAbove__(x, filter_n):
        if x >= filter_n:
            return x + 1
        else:
            return x

    def __max__(self, column:str):
        """max function, but also handle empty db
        - col=oper_n: limit max to selected category
        - col=filter_n: take from curent category if exist or max+1
        """
        if column == self.FILTER_N:
            if any(self.catRows): # category already exists
                return self.cat.loc[self.catRows, self.FILTER_N].iloc[0]
            else:
                return max(self.cat.loc[:, column], default=0) + 1
        else:
            return max(self.cat.loc[self.catRows, column], default=0)

    def __validate__(self, db: "pandas") -> "Null or pandas":
        """ cleaning of self.cat
        1) concate FILTER_N for CATEGORY
        2) first oper in cat must be add which is not allowed to remove
        4) renumber oper_n within categories to avoid holes
        """
        db.sort_values(by=[self.FILTER_N, self.OPER_N], ignore_index=True, inplace=True)
        catRows = db.loc[:, self.CATEGORY] == self.curCat
        # 1) align filter_n among category
        # only if we have curent category (not after remove)
        if any(catRows):
            fltr_n = db.loc[catRows, self.FILTER_N].iloc[0]
            db.loc[catRows, self.FILTER_N] = fltr_n
            db.sort_values(by=[self.FILTER_N, self.OPER_N], ignore_index=True, inplace=True)
            catRows = db.loc[:, self.CATEGORY] == self.curCat
        
        #2) check first row in cat
        if any(catRows):
            oper = db.loc[catRows, self.OPER].iloc[0]
        else:
            # we removed whole category, but this way we pass check
            oper = 'add'

        if oper != 'add':
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
        return str(self.trans.loc[args[0]])

    def __to_dict__(self) -> [{}]:
        """return list of rows, each row as dic\n
        """
        return self.trans.to_dict('records')

    def __len__(self):
        return len(self.trans)

    def __validate__(self, db) -> pandas:
        """validate tranformation DB. returns DB if ok or empty (when found duplicates)
        1) remove duplicates
        2) renumber filter_n to remove holes
        3) sort by trans_n
        """
        # remove duplicates
        dup = db.duplicated(subset=[self.COL_NAME, self.OPER, self.VAL1], keep='first')
        if any(dup.to_list()):
            self.parent.msg = f'trans.__validate__: transformation already exist'            
            return pandas.DataFrame()
        # renumber trans_n
        db.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)
        trans_max = len(db) + 1
        trans_l = list(range(1, trans_max))
        db.loc[:, self.TRANS_N] = trans_l
        # sort by trans_n
        db.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)

        return db

    def add(self, fltr: "dict|list(dict)"):
        """add new transformation, can be also used for replacement when trans_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {bank: str,col_name: str, oper: str, val1: str, val2: str}\n
        optionally:\n
        - trans_n, will put at position if provided, other way will add after last one
        """
        if not isinstance(fltr, list):
            fltr_list = [fltr]
        else:
            fltr_list = fltr
        for fltr in fltr_list:
            #define filter position
            t = False
            if self.TRANS_N in fltr.keys(): t = fltr[self.TRANS_N]
            if not t:
                fltr[self.TRANS_N] = self.__max__() + 1
            else: # renumber ilter_n above inserting position
                fltr[self.TRANS_N] = float(fltr[self.TRANS_N])
                newSer = self.trans[self.TRANS_N].apply(lambda x: self.__addAbove__(x, fltr[self.TRANS_N]))
                self.trans.loc[:, self.TRANS_N] = newSer

            cat2val = self.trans.append(fltr, ignore_index=True)
            valid_cat = self.__validate__(cat2val)
            if not valid_cat.empty:
                self.trans = valid_cat
                self.parent.__update__(fromDB='restore_all')
    
    @staticmethod
    def __addAbove__(x, filter_n):
        if x >= filter_n:
            return x + 1
        else:
            return x

    def rm(self, trans_n:int):
        """remove transformation\n
        """
        trans_n = float(trans_n)
        transRows = self.trans.loc[:, self.TRANS_N] == trans_n
        self.trans.drop(self.trans[transRows].index, inplace=True)

        self.trans = self.__validate__(self.trans)
                
        self.parent.__update__(fromDB='restore_all')

    def mov(self, trans_n:int, new_trans_n:int):
        """move transformation at trans_n to new position
        """
        trans_n = float(trans_n)
        new_trans_n = float(new_trans_n)

        trans_n_row = self.trans.loc[:, self.TRANS_N] == trans_n
        new_trans_n_row = self.trans.loc[:, self.TRANS_N] == new_trans_n

        self.trans.loc[trans_n_row, self.TRANS_N] = new_trans_n
        self.trans.loc[new_trans_n_row, self.TRANS_N] = trans_n

        self.trans = self.__validate__(self.trans)
        self.parent.__update__(fromDB='restore_all')

    def opers(self):
        """return avilable filtering operations
        """
        return list(self.transOps.keys())
    
    def __max__(self):
        """max function, but also handle empty db
        """
        return max(self.trans.loc[:, self.TRANS_N], default=0)

            
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
        return self.tree.loc[parentRow, self.CAT_PARENT].to_string(index=False).strip()

    def allChild(self, catStart = cfg.GRANDPA) -> list:
        """return list of all children (categories)
        starting from category = catStart
        """
        childLev = {}
        childrenAll = self.tree.loc[:, self.CATEGORY].to_list()
        for i in list(set(childrenAll)):
            lev = 0
            parent = self.parent(i)
            while parent != '/':
                parent = self.parent(parent)
                lev += 1
            childLev[i] = lev
        
        # children only below catStart, including catStart
        def childRec(kid: str):
            for i in self.child(parent=kid):
                children.append(i)
                childRec(i)

        if catStart == cfg.GRANDPA:
            children = list(childLev.keys())
        else:
            children = [catStart]
            childRec(catStart)

        childLev = {key: value for key, value in sorted(childLev.items(), key=lambda item: item[1])
                                        if key in children}

        return childLev

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
            self.par.msg = f'tree.ren(): Not allowed to rename "{category}".'
            return

        if new_category in forbiden:
            self.par.msg = f'tree.ren(): Not allowed to rename into child or parent'
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
    def __init__(self, parent):
        self.imp = {} # store raw data {bank: pandas.DataFrame,...}
        self.iter = 100
        #we limit the banks to new only if not commit yet
        # otherway all
        self.scope = list(self.imp.keys())
        self.parent = parent

    def __iter__(self):
        self.iter = 0
        return self
    
    def __next__(self):
        """if exists banks not commited, we itareate only them
        """
        if self.iter < len(self.scope):
            bankn = self.scope[self.iter]
            bank = self.__bank__(bankn)
            db = self.imp[bankn]
            self.iter += 1
            return bank, db
        else:
            raise StopIteration

    def ins(self, bank: str, db: 'pandas'):
        """adding new data, class will work only on this new date until commit or pop
        """
        if list(self.imp.keys()) != self.scope:
            self.parent.msg = 'Commit or reject before adding another data'
        bank = self.__bank_n__(bank=bank)
        self.imp[bank] = db
        # we can add one bank per commit
        self.scope = [bank]

    def pop(self):
        """remove last import
        """
        for i in self.scope:
            self.imp.pop(i)
        self.scope = list(self.imp.keys())
        return

    def commit(self):
        self.scope = list(self.imp.keys())

    def __bank_n__(self, bank: str) -> str:
        """return bank name apropriate for imp db ( with added number)
        return bankn, with n=len(bank) + 1
        """
        bnks_all = list(self.imp.keys())
        bnks = [bnk for bnk in bnks_all if re.match(f'{bank}\d+', bnk)]
        n = len(bnks)
        return bank + str(n + 1)

    def __bank__(self, bank: str) -> str:
        """remove bank number from bank name
        """
        ret = re.findall(r'^.+(?=\d+)', bank)
        return ret[0]


class SPLIT(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.split = pandas.DataFrame(columns=cfg.split_col)
    
    def __len__(self):
        return len(self.split)

    def __getitem__(self, *args) -> str:
        """equivalent of pandas.iloc\n
        """
        return str(self.split.loc[args[0]])

    def add(self, split: "dict|list(dict)"):
        """add new split, can be also used for replacement when split_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {start_date: date, end_date: date, col_name: str, fltr: str, val: float, day: int}\n
        optionally:\n
        - split_n, will replace at position if provided, other way will add after last one
        """
        if not isinstance(split, list):
            split = [split]
        for spl in split:
            s = False
            if self.SPLIT_N in spl.keys(): s = spl[self.SPLIT_N]
            if not s:
                spl[self.SPLIT_N] = self.__max__() + 1 
            else: # renumber split_n above inserting position
                spl[self.SPLIT_N] = float(spl[self.SPLIT_N])
                newSer = self.split[self.SPLIT_N].apply(lambda x: self.__addAbove__(x, spl[self.SPLIT_N]))
                self.split.loc[:, self.SPLIT_N] = newSer

            valSplit = self.__validate__(self.split.append(spl, ignore_index=True))
            if not valSplit.empty:
                self.split = valSplit.copy()
                self.parent.__update__(change = self.__to_dict__(), fromDB='split')

    def rm(self, split_n: int):
        """remove split row
        """
        splitRow = self.split.loc[:, self.SPLIT_N] == split_n
        self.split.drop(self.split.loc[splitRow].index(), inplace = True)
        self.parent.__update__(fromDB='restore_all')

    def __validate__(self, db) -> pandas:
        """validate split DB. returns DB if ok or empty (when found duplicates)
        1) remove duplicates
        2) renumber split_n to remove holes
        3) sort by split_n
        """
        # remove duplicates
        dup = db.duplicated(subset=[self.START, self.END, self.COL_NAME, self.FILTER, self.VAL1, self.DAYS], keep='first')
        if any(dup.to_list()):
            self.parent.msg = f'split.__validate__: transformation already exist'            
            return pandas.DataFrame()
        # renumber split_n
        db.sort_values(by=self.SPLIT_N, ignore_index=True, inplace=True)
        split_max = len(db) + 1
        split_l = list(range(1, split_max))
        db.loc[:, self.SPLIT_N] = split_l
        # sort by split_n
        db.sort_values(by=self.SPLIT_N, ignore_index=True, inplace=True)

        return db

    def __to_dict__(self) -> [{}]:
        """return list of rows, each row as dic\n
        """
        return self.split.to_dict('records')

    def __max__(self):
        """max function, but also handle empty db
        """
        return max(self.split.loc[:, self.SPLIT_N], default=0)

    @staticmethod
    def __addAbove__(x, filter_n):
        if x >= filter_n:
            return x + 1
        else:
            return x


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
        filter_mov(oper_n, direction): move selected filter
        filter_rm(oper_n): remove selected filter
    3. db trans stores data basic manipulations, mainly string replace
        trans_col(bank='', col_name='', op='+|-|*|str_repl', val1='', val2=''): transform data in selected column
        trans_mov(index, direction): move selected 
        trans_rm(index): remove selected transformation
    4. db tree stores categories hierarchy
    5. db imp{} stores raw data in dictionary, before any changes. Allowes reverse changes
    6. db split stores data to split operation into two by amount
    """
    def __init__(self, file=''):
        # MSG system, emits signal to parrent when self.msg change
        # MUST be the first thing class do
        self.msg_emit = False # true when connected to parent's method

        super().__init__()

        self.op = OP(self)
        self.cat = CAT(self)
        self.trans = TRANS(self)
        self.tree = TREE(self)
        self.imp = IMP(self)
        self.split = SPLIT(self)

        self.noOPupdate = False
        
        # during import set status and save all db until commit
        self.imp_status = False
        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()
        self.split_before_imp = pandas.DataFrame()

        self.msg = ''
        if file:
            self.open_db(file)
    
    def __update__(self, fromDB: str, change='', **kwargs):
        """synchronize data between update DB\n
        change: [{}]
        - if fromDB='trans', update self.op and self.cat\n
            with trans where change and than all cat in op
        - if fromDB='cat', update categories in op for provided cat (but not trans)
        - if fromDB='tree', remove category from cat if needed
        tree and cat needs to know caller function and it's arguments
        """
        if fromDB.lower() == 'restore_all':
            change = self.trans.__to_dict__()
            #restore whole DB
            self.op = OP(self)
            for bank, db in self.imp:
                self.op.ins(db=db)

            self.op.__updateTrans__(change)
            self.cat.__update__(change)
            cat = self.op.__updateSplit__(change = self.split.__to_dict__())
            if cat: self.cat.add(cat)
            # cat.update will call self.update again with fromDB='cat'
            # so op.updateCat will update with new filters
        if fromDB.lower() == 'cat':
            # when renaming or removing category, tree shall be also updated
            if kwargs:
                exec(f"self.tree.{kwargs['op']}(**{kwargs})")
                if kwargs['op'] == 'ren':
                    # op.__updateCat__() touch only grandpa category
                    # when altering only one category, 
                    # starting from Grandpa, will claer all categories in op
                    self.op.__rmCat__(**kwargs)
            if not self.noOPupdate:
                self.op.__updateCat__(change)
            else:
                self.noOPupdate = False
        if fromDB.lower() == 'tree':
            # when renaming or removing in tree, cat shall be also updated
            # but first remove cat from op
            if kwargs:
                if kwargs['op'] == 'ren':
                    self.op.__rmCat__(**kwargs)
                exec(f"self.cat.{kwargs['op']}(**{kwargs})")
        if fromDB.lower() == 'split':
            self.noOPupdate = True
            cat = self.op.__updateSplit__(change)
            if cat: self.cat.add(cat)

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

    def imp_commit(self, decision):
        if decision == 'ok':
            self.imp.commit()
            self.__update__(change=self.trans.__to_dict__(), fromDB='trans')
            self.msg = 'Data added to DB'

        else: # not ok
            self.imp.pop()
            
            self.op.op = self.op_before_imp.copy()
            self.cat.cat = self.cat_before_imp.copy()
            self.trans.trans = self.trans_before_imp.copy()
            self.tree.tree = self.tree_before_imp.copy()
            self.split.split = self.split_before_imp.copy()

            self.msg = 'Import rejected. Restored main DB'

        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()
        self.split_before_imp = pandas.DataFrame()
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
        self.__correct_col_types__(xls)

        self.op_before_imp = self.op.op.copy()
        self.cat_before_imp = self.cat.cat.copy()
        self.trans_before_imp = self.trans.trans.copy()
        self.tree_before_imp = self.tree.tree.copy()
        self.split_before_imp = self.split.split.copy()

        self.imp.ins(bank=bank, db=xls.copy())
        
        self.__update__(fromDB='restore_all')
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
        return

    @staticmethod
    def __str2num__(col, num_type):
        col_digit = []
        for i in col:
            i = str(i)
            try:
                if num_type == "INT":
                    i = int(i)
                else:
                    i = float(i)
                col_digit.append(i)
            except:
                i = ''.join(x for x in i if x.isdigit() or x == '.' or x == ',')
                i = i.replace(",", ".")
                if i:
                    if num_type == "INT":
                        i = int(i)
                    else:
                        i = float(i)
                    col_digit.append(i)
        return pandas.Series(col_digit)

    def open_db(self, file):
        #not allowed when in import mode
        if self.imp_status:
            self.msg = 'finish importing before opening another DB'
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
            self.imp.commit()
        except:
            engine.close()
            self.msg = 'Not correct DB <DB.__open_db__>'
            self.op = OP(self)
            self.cat = CAT(self)
            self.trans = TRANS(self)
            self.tree = TREE(self)
            self.imp = IMP(self)
            self.split = SPLIT(self)
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

   
    