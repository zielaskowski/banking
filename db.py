"""
manage database logic, defines all operations
"""
#from pickle import TRUE
from typing import Dict, List, Union

# from sqlalchemy import false, null, true
import opt.parse_cfg as cfg
from modules import FileSystem
from dev.integrationTest.decorators import writeOp
#from pandas.core.dtypes.missing import isnull
import re
import os
import sqlite3
import numpy as np
import pandas
pandas.options.plotting.backend = 'plotly'


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
        self.FILTER_ORIG = cfg.cat_col[7]
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
            try:
                x = float(x)
            except:
                return
            vec_new = []
            for i in vec:
                if type(i) in [float, int]:  # moga byc NULL lub inne kwiatki
                    # a nie mozemy zmienic dlugosci wektora
                    vec_new.append(i * x)
                else:
                    vec_new.append(i)
            return vec_new

        def transRep(vec: list, x: str, y: str) -> list:
            if type(x) == str:
                x = x.lower()
            if type(y) == str:
                y = y.lower()
            vec = vec.to_list()
            new_vec = []
            for i in vec:
                if isinstance(i, str):  # work only with string (no int or NULL)
                    # removing \n: search WHOLE string
                    new_vec.append(re.sub(x, y,
                                          i.replace('\n', ''),
                                          flags=re.IGNORECASE).strip())
                else:
                    new_vec.append(i)
            return new_vec

        def catAdd(rows: 'list(bool)', vec: 'list(bool)') -> list:
            return list(np.array(vec) | np.array(rows))

        def catLim(rows: 'list(bool)', vec: 'list(bool)') -> list:
            return list(np.array(vec) & np.array(rows))

        def catRem(rows: 'list(bool)', vec: 'list(bool)') -> list:
            return list(np.array(vec) & ~np.array(rows))

        def fltrMa(self, vec_all: 'list(str)', fltr: str) -> list:
            rows = vec_all\
                .astype('string')\
                .str.contains(fltr,
                              case=False,
                              flags=re.IGNORECASE,
                              regex=True,
                              na=False)
            return rows

        def fltrGt(self, vec_all: 'list(str)', fltr: str) -> list:
            try:
                fltr = float(fltr)
                return vec_all > fltr
            except Exception as error:
                self.parent.msg(type(error).__name__ + ': ' + str(error))
                return [False] * len(vec_all)

        def fltrLt(self, vec_all: 'list(str)', fltr: str) -> list:
            try:
                fltr = float(fltr)
                return vec_all < fltr
            except Exception as error:
                self.parent.msg(type(error).__name__ + ': ' + str(error))
                return [False] * len(vec_all)

        def fltrEq(self, vec_all: 'list(str)', fltr: str) -> list:
            try:
                fltr = float(fltr)
                return vec_all == fltr
            except Exception as error:
                self.parent.msg(type(error).__name__ + ': ' + str(error))
                return [False] * len(vec_all)

        # transform and category filtering operations
        self.transOps = {'*': transMultiply,
                         'str.replace': transRep}
        self.catOps = {'add': catAdd,
                       'lim': catLim,
                       'rem': catRem}
        self.catFltrs = {'txt_match': fltrMa,
                         'greater >': fltrGt,
                         'smaller <': fltrLt,
                         'equal': fltrEq}

    def str2int(self, n: str):
        try:
            n = float(n)
        except:
            self.msg(f'unknown number : {n}')
            return False
        return n

    def validateRegEx(self, regEx: str) -> str:
        """
        validate regular expression
        return empty str if all is ok
        other way return msg
        """
        try:
            re.compile(regEx)
            return ''
        except:
            return f'Wrong filter regExp: {regEx}'


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

    def get(self, category: List[str]) -> pandas:
        """returns pandas db for category\n
        """
        # stores list of bools for rows assigned to curCat
        if isinstance(category, str):
            category = [category]
        catRows = self.op.loc[:, self.CATEGORY].isin(category)
        return self.op.loc[catRows, :].copy().reset_index()

    def ins(self, db):
        """adds new data to self.op
        does not update categories or transformations
        """
        self.op = self.op.append(db, ignore_index=True)
        pandas.DataFrame.drop_duplicates(
            self.op, subset=self.HASH, inplace=True, ignore_index=True)

    def group_data(self, col: str, category=[cfg.GRANDPA]) -> list:
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
        # categorize only strrings
        dtName = self.op[col].dtypes.name
        if dtName != 'string':
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
            ii = i.replace("\n", " ")
            res.append(f'x{col_grouped[i]}:  {ii}')
        return res

    def sum_data(self, op='', category='') -> 'str(int)':
        """sum whole category\n
        if op == count: return number of rows
        if op == cumSum: return cumulative sum of kids
        if op == col_name: return sum of col_name for category
        if op == '' return possible op
        """
        ops = [cfg.op_col[i] for i in range(len(cfg.op_col_type))
               if cfg.op_col_type[i] in ['INT', 'REAL']]
        ops.append('count')
        ops.append('cumSum')

        if not op:
            return ops
        if op in ops:
            db = self.get(category=category)
            if op == 'count':
                return str(len(db))
            else:
                if op == 'cumSum':
                    op = ops[0]
                    kids = self.parent.tree.allChild(catStart=category)
                    category = kids.keys()

                db = self.get(category=category)
                summ = int(db[op].sum())
                if summ == 0:
                    return ''
                else:
                    return str(summ)

    def __updateTrans__(self, change: List[Dict]):
        """reset all categories and re-run all trans filters on op\n
        change can be one or more rows from trans DB
        """
        if self.op.empty:
            # may happen when mporting filters on empty db
            return

        self.__rmCat__()

        for fltr in change:
            if fltr[self.BANK] in cfg.bank.keys():  # chosen bank
                bankRows = self.op[self.BANK] == fltr[self.BANK]
            else:
                bankRows = [True] * len(self.op)  # all banks
            # iterate through all columns or chosen only
            if fltr[self.COL_NAME] in cfg.cat_col_names:  # chosen column name
                cols = [fltr[self.COL_NAME]]
            else:
                cols = cfg.cat_col_names  # we search in ALL columns
            for col in cols:
                ser = self.op.loc[bankRows, col]
                ser = self.transOps[fltr[self.OPER]](
                    ser, fltr[self.VAL1], fltr[self.VAL2])
                self.op.loc[bankRows, col] = ser

    def __updateCat__(self, change: List[Dict], removeOnly=False):
        """update categories
        change can be one or more rows from cat DB
        if removeOnly=True, only remove cats in change
        """
        if self.op.empty:
            # may happen when mporting filters on empty db
            return

        def otherCats():
            otherCat = set(self.op.loc[ser & ~grandpas,
                                       self.CATEGORY])
            if cat in otherCat:
                otherCat.remove(cat)
            return otherCat

        cats = set([c[self.CATEGORY]
                    for c in change])
        self.__rmCat__(cats)
        if removeOnly:
            return
        
        # arrange by filter_n !!!!
        cats = sorted(cats, key=lambda x: [int(f[self.FILTER_N])
                                           for f in change
                                           if f[self.CATEGORY] == x])

        for cat in cats:
            ser = [False] * len(self.op)

            for fltr in [c for c in change
                         if c[self.CATEGORY] == cat]:
                # iterate through all columns or chosen only
                if fltr[self.COL_NAME] in cfg.cat_col_names:  # chosen column name
                    cols = [fltr[self.COL_NAME]]
                else:
                    cols = cfg.cat_col_names  # we search in ALL columns

                # combine all fitting rows for all filters in category
                rows = [False] * len(self.op)
                for col in cols:
                    ser_all = self.op.loc[:, col]
                    row = self.catFltrs[fltr[self.SEL]](
                        self, ser_all, fltr[self.FILTER])
                    rows = list(np.array(rows) | np.array(row))
                ser = self.catOps[fltr[self.OPER]](rows, ser)

            # we completed category, write selected rows and check for overlaps
            grandpas = self.op.loc[:, self.CATEGORY] == cfg.GRANDPA
            if otherCats():  # mark overlaping cats
                self.parent.msg(
                    f'When setting {cat}, could not get some rows belonging already to other categories: {otherCats()}')

            self.op.loc[ser & grandpas, self.CATEGORY] = cat

    def __updateSplit__(self, change: List[Dict]):
        """split selected rows\n
        change can be one or more rows from split db\n
        1) hash new rows\n
        2) {col_name: col_name, function: 'txt_match' , filter: filter, oper: 'add'}
        """
        colKwota = self.sum_data()[0]

        for ch in change:
            ch = pandas.Series(ch)
            if ch[self.COL_NAME] != self.HASH:
                if ch[self.START] in ["-", "", "*"]:
                    ch[self.START] = self.op.loc[:, self.DATA_OP].min()
                if ch[self.END] in ["-", "", "*"]:
                    ch[self.END] = self.op.loc[:, self.DATA_OP].max()

                # make sure we have proper data types
                try:
                    ch[self.START] = pandas.to_datetime(ch[self.START])
                    ch[self.END] = pandas.to_datetime(ch[self.END])
                    ch[self.VAL1] = self.str2int(ch[self.VAL1])
                    ch[self.DAYS] = self.str2int(ch[self.DAYS])
                except:
                    self.parent.msg('__updateSplit__: wrong type of input')
                    return False
                self.parent.split.validateSplit(ch[self.SPLIT_N], True)

                # get op rows
                days = (ch[self.END] - ch[self.START]) // ch[self.DAYS]
                days_n = days.days
                if days_n < 1:
                    self.parent.msg(
                        '__updateSplit__: Date range shall be at number of day')
                    ch[self.END] = ch[self.START] + days.days
                start = self.op.loc[:, self.DATA_OP] >= ch[self.START]
                end = self.op.loc[:, self.DATA_OP] <= ch[self.END]
                cat = self.op.loc[:, ch[self.COL_NAME]] == ch[self.FILTER]
                rows = self.op.loc[start & end & cat, :].copy()
                if rows.empty:
                    self.parent.msg(f'Split filter refer to empty category')
                    self.parent.split.validateSplit(ch[self.SPLIT_N], False)
                    return False

                availCash = rows.loc[:, colKwota].sum()
                if abs(availCash) < abs(days_n * ch[self.VAL1]):
                    self.parent.msg(
                        f'when setting {ch[self.CATEGORY]} not enough cash in {ch[self.FILTER]}: {round(availCash,2)} is less than {days_n * ch[self.VAL1]}.')
                    ch[self.VAL1] = abs(availCash) / days_n
                    # write info to status table
                    self.parent.split.validateSplit(ch[self.SPLIT_N], False)

                self.op.drop(rows.index, inplace=True)

                def substract(amount):
                    newAmount = rows\
                        .loc[:, colKwota]\
                        .apply(lambda x: x - amount)
                    return newAmount

                rows.loc[:, colKwota] = substract(
                    ch[self.VAL1] * days_n / len(rows))
                # some amounts may be less then substract
                smallRows = rows.loc[:, colKwota] > 0
                while any(smallRows):
                    newAmount = sum(rows.loc[smallRows, colKwota]) * -1
                    rows.drop(rows.loc[smallRows].index, inplace=True)
                    rows.loc[:, colKwota] = substract(newAmount / len(rows))
                    smallRows = rows.loc[:, colKwota] > 0

                # add new rows
                for i in range(int(days_n)):
                    row = rows.iloc[0, :].to_dict()
                    row[self.DATA_OP] = ch[self.START] + \
                        pandas.Timedelta(ch[self.DAYS] * i, unit='D')
                    row[self.CATEGORY] = ch[self.CATEGORY]
                    row[colKwota] = ch[self.VAL1]
                    hashRow = pandas.DataFrame(
                        row, columns=list(row.keys()), index=[0])
                    hashRow = hashRow.drop([self.HASH, self.CATEGORY], axis=1)
                    row[self.HASH] = pandas.util.hash_pandas_object(
                        hashRow, index=False).to_string(index=False).strip()
                    rows = rows.append(row, ignore_index=True)

                self.op = self.op.append(rows, ignore_index=True)
                self.op = self.parent.__correct_col_types__(self.op)

            else:  # find hash only and change category
                hashRow = self.op.loc[:, self.HASH] == ch[self.FILTER]
                self.op.loc[hashRow, self.CATEGORY] = ch[self.CATEGORY]

        return True

    def __rmCat__(self, category=[cfg.GRANDPA]):
        """change category=cat to grandpa\n
        if empty category, change all
        """
        for c in category:
            if c in [cfg.GRANDPA, '*']:
                self.op.loc[:, self.CATEGORY] = cfg.GRANDPA
                return
            catRows = self.op.loc[:, self.CATEGORY] == c
            self.op.loc[catRows, self.CATEGORY] = cfg.GRANDPA
        return


class CAT(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.cat = pandas.DataFrame({self.COL_NAME: cfg.cat_col_names[0],
                                     self.SEL: 'txt_match',
                                     self.FILTER: '.*',
                                     self.FILTER_N: 0,
                                     self.FILTER_ORIG: '.*',
                                     self.OPER: 'add',
                                     self.OPER_N: 1,
                                     self.CATEGORY: cfg.GRANDPA}, columns=cfg.cat_col, index=[0])  # table of categories
        # start with selected grandpa category,
        # there can be only one filter which we don't want to display
        self.curCat = [cfg.GRANDPA]
        self.catRows = pandas.DataFrame
        self.setCat(self.curCat)

    def setCat(self, category: List[str]) -> None:
        """set curent category so other methods work only on sel cat\n
        if cat=grandpa return null\n
        if cat='*' return all\n
        """
        if isinstance(category, str):
            category = [category]
        if cfg.GRANDPA in category:
            self.catRows = [False] * len(self.cat)
        elif '*' in category:
            self.catRows = [True] * len(self.cat)
        else:
            self.catRows = self.cat.loc[:, self.CATEGORY].isin(category)
        self.curCat = category

    def __getitem__(self, *args) -> str:
        """equivalent of pandas.loc\n
        work on curent category only (set by setCat())
        """
        it = self.cat.loc[self.catRows, :].reset_index().loc[args[0]]
        return it

    def __to_dict__(self, category=[]) -> List[Dict]:
        """return list of rows, each row as dic\n
        if cat not provided will use self.curCat (set by setCat())\n
        category='*' for all db
        """
        if category:
            self.setCat(category)
        return self.cat.loc[self.catRows, :].to_dict('records')

    def __restore__(self, trans: List[Dict]):
        # restore cat filters to original (before any transform)
        # necessery when playing with transform
        # filter_original not enough when cat created AFTER filter

        # warn when removing filter with val2 in cat filter and filter==filter_orig
        affCats = np.array(self.cat.loc[:, self.FILTER] == trans[0][self.VAL2])
        origCats = np.array(self.cat[self.FILTER]
                            == self.cat[self.FILTER_ORIG])

        probCats = self.cat.loc[affCats & origCats, self.CATEGORY].to_list()
        if probCats:
            self.parent.msg(
                f'This operation may have invalidated the category: {list(set(probCats))}. Please check.')

        self.cat[self.FILTER] = self.cat[self.FILTER_ORIG]

    def __update__(self, change: List[Dict]):
        """transform all filters\n
        change is one or more rows from trans DB
        """
        for fltr in change:
            # do not change if val2=='', just warn that cat is no longer valid
            affCats = self.cat.loc[:, self.FILTER] == fltr[self.VAL1]
            probCats = self.cat.loc[affCats, self.CATEGORY].to_list()

            if fltr[self.VAL2] == '' and probCats:
                self.parent.msg(
                    f'This operation may have invalidated the category: {list(set(probCats))}. Please check.')
                continue

            ser = self.cat.loc[:, self.FILTER]
            ser = self.transOps[fltr[self.OPER]](
                ser, fltr[self.VAL1], fltr[self.VAL2])
            self.cat.loc[:, self.FILTER] = ser

    def __len__(self):
        return len(self.cat.loc[self.catRows, :])

    def opers(self) -> list:
        """return avilable filtering operations
        """
        return list(self.catOps.keys())

    def fltrs(self) -> list:
        """return avilable filters
        """
        return list(self.catFltrs.keys())

    def add(self, fltr: "dict"):
        """add new filter, can be also used for replacement when oper_n provided\n
        also possible to pass multiple dicts in list\n
        not allowed on category in ['grandpa', '*']\n
        minimum input: {col_name: str, function: str, filter: str, oper: str}\n
        optionally:\n
        - category, otherway will use self.curCat\n
        - oper_n, will replace at position if provided, other way will add after last one
        - filter_orig used to restore category filter after removing transform.
            Not always possible, i.e when transform created before category....
        """
        # validate regexp in FILTER
        val = self.validateRegEx(fltr[self.FILTER])
        if val:
            self.parent.msg = val
            return False

        # if first time
        if not fltr[self.FILTER_ORIG]:
            fltr[self.FILTER_ORIG] = fltr[self.FILTER]

        # define categories
        if not any([v for k, v in fltr.items() if k == self.TRANS_N]):
            self.setCat(fltr[self.CATEGORY])
        else:
            fltr[self.CATEGORY] = self.curCat

        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg(
                f'category.add: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.')
            return False

        # define filter position
        if not any([v for k, v in fltr.items() if k == self.OPER_N]):
            fltr[self.OPER_N] = self.__max__(self.OPER_N) + 1
        else:  # renumber oper_n above inserting position
            fltr[self.OPER_N] = float(fltr[self.OPER_N])
            newSer = self.cat.loc[self.catRows, self.OPER_N].apply(
                lambda x: self.__addAbove__(x, fltr[self.OPER_N]))
            self.cat.loc[self.catRows, self.OPER_N] = newSer

        # define new filter_n or take of current category
        fltr[self.FILTER_N] = self.__max__(self.FILTER_N)

        # validate
        cat2val = self.cat.append(fltr, ignore_index=True)
        valid_cat = self.__validate__(cat2val)
        if not valid_cat.empty:
            self.cat = valid_cat
            self.setCat(self.curCat)  # to have the same number of rows
            return self.curCat
        else:
            return False

    def rm(self, category: str, oper_n=0):
        """remove filter or category\n
        not allowed on category in ['grandpa', '*']\n
        if category not given will use self.curCat\n
        if oper_n=0, remove whole category
        return true if cat removed completely, otherway return category
        """
        # define categories
        if category:
            self.setCat(category)
        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg(
                f'category.rm: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.')
            return False

        if not oper_n:
            oper_n = self.cat.loc[self.catRows, self.OPER_N].to_list()
        else:
            oper_n = [oper_n]

        # find row to be deleted
        operRows = self.cat.loc[:, self.OPER_N].isin(oper_n)
        dropRows = self.cat.loc[operRows & self.catRows]

        # validate
        valid_cat = self.__validate__(self.cat.drop(dropRows.index))
        if not valid_cat.empty:
            self.cat = valid_cat
        self.setCat(category)
        return self.curCat

    def mov(self, oper_n: int, new_oper_n: int, category='') -> list:
        """move filter at oper_n to new position
        return category or [] if mistake or movement is between add operations
        """
        # define categories
        if category:
            self.setCat(category)

        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg(
                f'category.mov: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.')
            return []

        oper_n_row = (self.cat.loc[:, self.OPER_N] == oper_n) & self.catRows
        new_oper_n_row = (
            self.cat.loc[:, self.OPER_N] == new_oper_n) & self.catRows
        if not (any(oper_n_row) and any(new_oper_n_row)):
            self.parent.msg(f'category.mov: Wrong operation position')
            return []

        db = self.cat.copy()
        db.loc[oper_n_row, self.OPER_N] = new_oper_n
        db.loc[new_oper_n_row, self.OPER_N] = oper_n

        valid_cat = self.__validate__(db)
        if not valid_cat.empty:
            self.cat = valid_cat
            self.setCat(self.curCat)  # to have the same number of rows
            # if change between add oper, no need to update
            if (self.cat.loc[oper_n_row, self.OPER] == 'add').bool() &\
                    (self.cat.loc[new_oper_n_row, self.OPER] == 'add').bool():
                return []
            return self.curCat
        else:
            return []

    def ren(self, new_category: str, category='') -> list:
        """ rename category
        """
        # define categories
        if category:
            self.setCat(category)

        if self.curCat in [cfg.GRANDPA, '*']:
            self.parent.msg(
                f'category.ren: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.')
            return []

        self.cat.loc[self.catRows, self.CATEGORY] = new_category
        self.setCat(new_category)

        # validate
        valid_cat = self.__validate__(self.cat)
        if not valid_cat.empty:
            self.cat = valid_cat
            self.setCat(self.curCat)  # to have the same number of rows
            return self.curCat
        else:
            # not validated, so recover cat
            self.cat.loc[self.catRows, self.CATEGORY] = category
            self.setCat(category)
            return []

    def arrange(self, categories: str):
        db = pandas.DataFrame()
        for cat in categories:
            cat_rows = self.cat.loc[:, self.CATEGORY] == cat
            self.cat.loc[cat_rows, self.FILTER_N] = list(categories).index(cat)
            db = db.append(self.cat.loc[cat_rows, :], ignore_index=True)
        self.cat = db.copy(deep=True)
        return

    @ staticmethod
    def __addAbove__(x, filter_n):
        if x >= filter_n:
            return x + 1
        else:
            return x

    def __max__(self, column: str):
        """max function, but also handle empty db
        - col=oper_n: limit max to selected category
        - col=filter_n: take from curent category if exist or max+1
        """
        if column == self.FILTER_N:
            if any(self.catRows):  # category already exists
                return self.cat.loc[self.catRows, self.FILTER_N].iloc[0]
            else:
                return max(self.cat.loc[:, column], default=0) + 1
        else:
            return max(self.cat.loc[self.catRows, column], default=0)

    def __validate__(self, db: pandas) -> None or pandas:
        """ cleaning of self.cat
        1) concate FILTER_N for CATEGORY
        2) first oper in cat must be add which is not allowed to remove
        4) renumber oper_n within categories to avoid holes
        5) not make sense to duplicate
        """
        db.sort_values(by=[self.FILTER_N, self.OPER_N],
                       ignore_index=True, inplace=True)
        catRows = db.loc[:, self.CATEGORY].isin(self.curCat)
        # 1) align filter_n among category
        # only if we have curent category (not after remove)
        # if any(catRows):
        #     fltr_n = db.loc[catRows, self.FILTER_N].iloc[0]
        #     db.loc[catRows, self.FILTER_N] = fltr_n
        #     db.sort_values(by=[self.FILTER_N, self.OPER_N],
        #                    ignore_index=True, inplace=True)
        #     catRows = db.loc[:, self.CATEGORY].isin(self.curCat)

        # 2) check first row in cat
        if any(catRows):
            oper = db.loc[catRows, self.OPER].iloc[0]
        else:
            # we removed whole category, but this way we pass check
            oper = 'add'

        if oper != 'add':
            self.parent.msg(
                f'cat.__validate__: catgeory must start with "add" operation. Category will be ignored.')
            # we do not have 'add' on first row
            # but for 'edit' purpose, we leave this category
            # it will be ignored by op.__validate_cat__

        # 4) renumber oper_n
        # oper_max = len(db.loc[catRows, :]) + 1
        # opers_l = list(range(1, oper_max))
        # db.loc[catRows, self.OPER_N] = opers_l

        # db.sort_values(by=[self.FILTER_N, self.OPER_N],
        #                ignore_index=True, inplace=True)

        # 5) not make sense to duplicate
        colNames = db.columns.values.tolist()
        [colNames.remove(c) for c in [self.OPER_N,
                                      self.CATEGORY,
                                      self.FILTER_N,
                                      self.FILTER_ORIG]]
        # colNames.remove(self.OPER_N)
        dup = db.loc[:, colNames].duplicated(keep=False)
        if any(dup):
            dupCat = list(db.loc[dup, self.CATEGORY])
            dupOper = list(db.loc[dup, self.OPER_N])
            self.parent.msg(
                f'Identical filter already exists in {dupCat[0]} at position {dupOper[0]}. Nothing done.')
            return pandas.DataFrame()

        return db


class TRANS(COMMON):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent
        self.trans = pandas.DataFrame(
            columns=cfg.trans_col)  # table of transformations

    def __getitem__(self, *args) -> str:
        """equivalent of pandas.iloc\n
        """
        return self.trans.loc[args[0]]

    def __to_dict__(self, trans_n='') -> List[Dict]:
        """return list of rows, each row as dic\n
        if missing trans number, return all
        """
        if not trans_n:
            fltr = self.trans
        else:
            fltr = self.trans.loc[self.trans.loc[:,
                                                 self.TRANS_N] == trans_n, :]
        return fltr.to_dict('records')

    def __len__(self):
        return len(self.trans)

    def __validate__(self, db) -> pandas:
        """validate tranformation DB. returns DB if ok or empty (when found duplicates)
        1) remove duplicates
        2) renumber filter_n to remove holes
        3) sort by trans_n
        """
        # remove duplicates
        dup = db.duplicated(
            subset=[self.COL_NAME, self.OPER, self.VAL1], keep='first')
        if any(dup.to_list()):
            self.parent.msg(
                f'trans.__validate__: transformation already exist')
            return pandas.DataFrame()
        # renumber trans_n
        db.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)
        trans_max = len(db) + 1
        trans_l = list(range(1, trans_max))
        db.loc[:, self.TRANS_N] = trans_l
        # sort by trans_n
        db.sort_values(by=self.TRANS_N, ignore_index=True, inplace=True)

        return db

    def add(self, fltr: dict):
        """add new transformation, can be also used for replacement when trans_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {bank: str,col_name: str, oper: str, val1: str, val2: str}\n
        optionally:\n
        - trans_n, will put at position if provided, other way will add after last one
        """
        # validate regexp in FILTER
        val = self.validateRegEx(fltr[self.VAL1])
        if val:
            self.parent.msg = val
            return False

        # define filter position
        if not any([v for k, v in fltr.items() if k == self.TRANS_N]):
            fltr[self.TRANS_N] = self.__max__() + 1
        else:  # renumber filter_n above inserting position
            fltr[self.TRANS_N] = float(fltr[self.TRANS_N])
            newSer = self.trans[self.TRANS_N].apply(
                lambda x: self.__addAbove__(x, fltr[self.TRANS_N]))
            self.trans.loc[:, self.TRANS_N] = newSer

        cat2val = self.trans.append(fltr, ignore_index=True)
        valid_cat = self.__validate__(cat2val)
        if not valid_cat.empty:
            self.trans = valid_cat
            return self.__to_dict__()
        else:
            return False

    @ staticmethod
    def __addAbove__(x, filter_n):
        if x >= filter_n:
            return x + 1
        else:
            return x

    def rm(self, trans_n: int):
        """remove transformation\n
        """
        trans_n = float(trans_n)
        transRows = self.trans.loc[:, self.TRANS_N] == trans_n
        self.trans.drop(self.trans[transRows].index, inplace=True)

        self.trans = self.__validate__(self.trans)

        return self.__to_dict__()

    def mov(self, trans_n: int, new_trans_n: int):
        """move transformation at trans_n to new position
        """
        trans_n = float(trans_n)
        new_trans_n = float(new_trans_n)

        trans_n_row = self.trans.loc[:, self.TRANS_N] == trans_n
        new_trans_n_row = self.trans.loc[:, self.TRANS_N] == new_trans_n

        self.trans.loc[trans_n_row, self.TRANS_N] = new_trans_n
        self.trans.loc[new_trans_n_row, self.TRANS_N] = trans_n

        self.trans = self.__validate__(self.trans)
        return True

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

    def new_cat(self) -> str:
        """
        return 'new name' with index if 'new name' already exists as cat
        """
        new_names = re.findall("new category",
                               str(self.allChild().keys()))
        new_names_no = len(new_names)
        if new_names_no == 0:
            new_names_no = ''
        return "new category" + str(new_names_no)

    def child(self, parent: str) -> list:
        childRow = self.tree.loc[:, self.CAT_PARENT] == parent
        return self.tree.loc[childRow, self.CATEGORY].to_list()

    def parent(self, child: str) -> str:
        """return parent of category.\n
        If category == Grandpa, return Grandpa
        """
        if child == cfg.GRANDPA:
            return cfg.GRANDPA
        parentRow = self.tree.loc[:, self.CATEGORY] == child
        # if child do not exist, return empty str
        if not any(parentRow):
            return ''
        # stop if more than one category: duplicates
        if len([i for i in parentRow if i]) > 1:
            self.par.msg(f'doubled category names: {child}')
            return cfg.GRANDPA  # this will stop infinite loop of allChildren()
        return self.tree.loc[parentRow, self.CAT_PARENT].to_list()[0]

    def path(self, category: str) -> list:
        """return path for category
        """
        path = [category]
        while path[-1] != cfg.GRANDPA:
            parent = self.parent(path[-1])
            path.append(parent)
        path.reverse()
        return path

    def allChild(self, catStart=cfg.GRANDPA) -> dict:
        """return list of all children (categories)
        starting from category = catStart
        for each cat return also level
        return child in correct order
        """
        childLev = {}
        lev = len(self.path(catStart))
        childLev[catStart] = lev - 1

        kids = self.child(parent=catStart)
        for i in kids:
            childLev.update(self.allChild(catStart=i))

        return childLev

    def add(self, parent: str, child: str):
        """adds new category (only empty)\n
        parent must exists, check for duplicates (drop last if found one)\n
        return True if success, otherway False\n
        """
        if parent not in self.allChild():
            self.par.msg(f'parent must exists.')
            return False

        self.tree = self.tree.append(
            {self.CATEGORY: child, self.CAT_PARENT: parent}, ignore_index=True)
        self.tree.drop_duplicates(
            subset=self.CATEGORY, keep='first', inplace=True)
        return True

    def ren(self, category: str, new_category: str):
        """rename category (also the one present in parent column)
        - can't change cat name to any of cat's child
        - can't change cat to it's parent
        Remove duplicates
        """
        forbidden = self.child(category)
        forbidden.append(self.parent(category))

        if category == cfg.GRANDPA:
            self.par.msg(f'tree.ren(): Not allowed to rename "{category}".')
            return

        if new_category in forbidden:
            self.par.msg(
                f'tree.ren(): Not allowed to rename into child or parent')
            return

        catRows = self.tree.loc[:, self.CATEGORY] == category
        parRows = self.tree.loc[:, self.CAT_PARENT] == category
        newParent = self.parent(category)
        self.tree.loc[catRows, self.CATEGORY] = new_category
        self.tree.loc[catRows, self.CAT_PARENT] = newParent
        self.tree.loc[parRows, self.CAT_PARENT] = new_category
        # remove duplicates
        self.tree.drop_duplicates(
            subset=[self.CATEGORY], keep='last', inplace=True)
        self.tree.reset_index(inplace=True, drop=True)
        return True

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

    def rm(self, child: str):
        """rem category, if data in category it will be removed (move to Grandpa) by cat DB in parent.__update__
        children of parent move to grandpa
        If not force, remove only if empty cat
        """
        for i in self.child(parent=child):
            self.mov(new_parent=cfg.GRANDPA, child=i)

        childRows = self.tree.loc[:, self.CATEGORY] == child
        self.tree.drop(self.tree.loc[childRows].index, inplace=True)

        return True

    def arrange(self, category: str, dir: str) -> list:
        """Move category in tree
        dir is direction in which to move: "up|down"
        move on the same level only
        return list of categories in order
        """
        parent = self.parent(child=category)
        siblings = self.child(parent=parent)
        cat_i = siblings.index(category)
        if dir == "up":
            cat_i -= 1
        else:
            cat_i += 1
        if cat_i < 0 or cat_i > len(siblings):
            self.par.msg(f'Can not move {dir}. Already on edge.')
            return []
        sib_row = self.tree.loc[:, self.CATEGORY] == siblings[cat_i]
        cat_row = self.tree.loc[:, self.CATEGORY] == category
        self.tree.loc[cat_row, :] = self.tree.loc[sib_row, :].to_numpy()
        self.tree.loc[sib_row, :] = [category, parent]

        return self.allChild().keys()


class IMP:
    def __init__(self, parent):
        self.imp = {}  # store raw data {bank: pandas.DataFrame,...}
        self.iter = 100
        # we limit the banks to new only if not commit yet
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

    def pop(self, bank=''):
        """remove last import
        if given bank name, remove bank
        """
        if bank != '':
            self.imp.pop(bank)
        else:
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
        bnks = [int(re.findall('\\d+$', bnk)[0])
                for bnk in bnks_all
                if re.match(f'{bank}\\d+', bnk)]
        if not bnks:
            n = 1
        else:
            n = max(bnks)
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
        self.validSplit = pandas.DataFrame(columns=[self.SPLIT_N, 'valid'])
        self.impSplit = [cfg.GRANDPA]
        self.splitRows = []
        self.setSplit(category=['*'])

    def __len__(self):
        return len(self.split.loc[self.splitRows, :])

    def __getitem__(self, *args) -> str:
        """equivalent of pandas.iloc\n
        """
        it = self.split.loc[self.splitRows, :].reset_index().loc[args[0]]
        return it

    def validateSplit(self, split_n: int, valid: bool):
        self.validSplit = self.validSplit\
            .append({self.SPLIT_N: split_n, 'valid': valid},
                    ignore_index=True)

    def setSplit(self, category=[], fltr=[], oper_n=0):
        """find rows for split: splitRows\n
        also set impSplit: impacted categories, chain down, so also splits refering to split
        can select based on category , filter or oper_n, ignore opr_n if ==0
        if cat='*' return all\n
        set catRows: List[bool] and curSplit: list
        """
        if oper_n != 0:
            self.splitRows = self.split.loc[:, self.SPLIT_N] == oper_n
            self.setImpCats()
            return

        if '*' in category:
            self.splitRows = [True] * len(self.split)
        else:
            self.splitRows = self.split.loc[:, self.CATEGORY].isin(category) |\
                self.split.loc[:, self.FILTER].isin(fltr)

        self.setImpCats()

    def setImpCats(self, category=[]):
        """
        Set all cats impacted by split in impSplit\n
        chain down, so have also split refere to categories in split etc\n
        if category missing, use self.splitRows
        """
        if not category:
            category = self.split.loc[self.splitRows, self.CATEGORY].to_list()
            category += self.split.loc[self.splitRows, self.FILTER].to_list()
        while True:
            fltr = self.split.loc[:, self.FILTER].isin(category)
            cats = self.split.loc[:, self.CATEGORY].isin(category)
            if not(any(cats) or any(fltr)):
                break
            newCat = self.split.loc[fltr | cats, self.CATEGORY].to_list()
            newCat += self.split.loc[fltr | cats, self.FILTER].to_list()
            if all([nc in category for nc in newCat]):
                break
            category = list(set(category + newCat))
        self.impSplit = category

    def add(self, split: dict) -> List[Union[Dict, None]]:
        """add new split, can be also used for replacement when split_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {start_date: date, end_date: date, col_name: str, fltr: str, val: float, day: int}\n
        optionally:\n
        - split_n, will replace at position if provided, other way will add after last one
        """
        if not any([v for k, v in split.items() if k == self.SPLIT_N]):
            split[self.SPLIT_N] = self.__max__() + 1
        else:  # renumber split_n above inserting position
            split[self.SPLIT_N] = self.str2int(split[self.SPLIT_N])
            newSer = self.split[self.SPLIT_N].apply(
                lambda x: self.__addAbove__(x, split[self.SPLIT_N]))
            self.split.loc[:, self.SPLIT_N] = newSer

        valid_split = self.__validate__(
            self.split.append(split, ignore_index=True))
        if isinstance(valid_split, pandas.DataFrame):
            self.split = valid_split
            return self.__to_dict__(category=[split[self.CATEGORY]])
        else:
            return []

    def rm(self, oper_n=0, category='', fltr='') -> List[Union[Dict, None]]:
        """remove split row
        - based on oper_n
        - based on category name if provided
        - check also filters (useful when we delete category which is referred by filter)
        return split where filter=filter or cat=cat
        """
        self.setSplit(oper_n=oper_n, category=[category], fltr=[fltr])

        dropRows = self.split.loc[self.splitRows]
        self.split.drop(dropRows.index, inplace=True)
        valid_split = self.__validate__(self.split)
        if isinstance(valid_split, pandas.DataFrame):
            self.split = valid_split
            return self.__to_dict__()
        else:
            return []  # must be list because may be send to op.__update__

    def ren(self, new_category: str, category: str) -> List[Union[Dict, None]]:
        '''
        rename split category, but also categories in filter
        return split categories, but also catrgoies in filter
        '''
        self.setSplit(category=[new_category])
        newCatRows = self.splitRows
        self.setSplit(category=[category])
        oldCatRows = self.splitRows
        if not any(oldCatRows | newCatRows):
            return []

        if category in [cfg.GRANDPA, '*']:
            self.parent.msg(
                f'split.ren: Not allowed operation on {cfg.GRANDPA}. Select one of the categories first.')
            return []

        self.split.loc[self.splitRows, self.CATEGORY] = new_category

        self.setSplit(fltr=[category])
        self.split.loc[self.splitRows, self.FILTER] = new_category

        return self.__to_dict__(category=[new_category])

    def merge(self, db: pandas):
        """
        used during import new data
        split very likely will be unintentionaly removed
        (due to insuficient funds)
        so we need to merge with orginal
        """
        self.split = self.split.append(db, ignore_index=True)
        validSplit = self.__validate__(self.split, dupComplain=False)
        if isinstance(validSplit, pandas.DataFrame):
            self.split = validSplit
            self.setSplit('*')
            return
        else:
            self.parent.msg(
                'not possible to merge split database. Restoring orginal one')
            self.split = db.copy()
            self.setSplit('*')
            return

    def __validate__(self, db, dupComplain=True) -> pandas:
        """validate split DB. returns DB if ok or empty (when found duplicates)
        1) remove duplicates
        2) renumber split_n to remove holes
        3) sort by split_n
        """
        # remove duplicates
        dup = db.duplicated(subset=[self.START, self.END, self.COL_NAME, self.FILTER, self.VAL1, self.DAYS],
                            keep='first')
        if any(dup.to_list()):
            if dupComplain:
                self.parent.msg(
                    f'split.__validate__: transformation already exist')
                return None
            else:
                db = db.loc[pandas.np.invert(dup), :]
        # renumber split_n
        db.sort_values(by=self.SPLIT_N, ignore_index=True, inplace=True)
        split_max = len(db) + 1
        split_l = list(range(1, split_max))
        db.loc[:, self.SPLIT_N] = split_l
        # sort by split_n
        db.sort_values(by=self.SPLIT_N, ignore_index=True, inplace=True)

        return db

    def __to_dict__(self, category=[]) -> List[Dict]:
        """return list of rows, each row as dic\n
        if cat not provided will use self.impSplit\n
        category='*' for all db
        """
        if category:
            self.setSplit(category=category)

        rows = pandas.Series([False] * len(self.split), dtype='boolean')
        for s in self.impSplit:
            rows = rows | self.split.loc[:, self.CATEGORY].isin([s])
        return self.split.loc[rows, :].to_dict('records')

    def __max__(self):
        """max function, but also handle empty db
        """
        return max(self.split.loc[:, self.SPLIT_N], default=0)

    @ staticmethod
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

    def __init__(self, file='', DEBUG=False):
        super().__init__()

        # write function names and arguments to csv file
        self.DEBUG = DEBUG
        self.DEBUG_F = ''  # set to CSV file with modules to be tested

        self.op = OP(self)
        self.cat = CAT(self)
        self.trans = TRANS(self)
        self.tree = TREE(self)
        self.imp = IMP(self)
        self.split = SPLIT(self)

        # during import set status and save all db until commit
        self.imp_status = False
        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()
        self.split_before_imp = pandas.DataFrame()
        self.IsData = False # change to True when data exists
        
        self.fs = FileSystem()
        self.msg = self.fs.msg
        if file:
            self.openDB(file)

    def connect(self, parent):
        """refrence to caller class.\n
        This way we can call parent method when needed\n
        used for msg transport\n
        """
        self.fs.connect(parent)

# operation methods
    @writeOp
    def splitAdd(self, split: dict) -> bool:
        """add split filter, also add category in tree if not existing\n
        if category existing in tree, will use parent of cat, otherway Grandpa\n
        split = {start_date: date, end_date: date, col_name: str, fltr: str, val: float, day: int}\n
        """
        parent = cfg.GRANDPA
        if split[self.COL_NAME] != self.CATEGORY:
            self.msg('spliting single items not implemented yet')
            return False

        if split[self.FILTER] == split[self.CATEGORY]:
            self.msg(f"can't add split to category which you split")
            return False

        # if category already exists change parent to parent of existing category
        if split[self.CATEGORY] in self.tree.allChild().keys():
            parent = self.tree.parent(split[self.CATEGORY])

        # add split category to tree
        if not self.tree.add(parent, child=split[self.CATEGORY]):
            return False  # probably missing parent

        change = self.split.add(split)
        if not change:  # probably duplicate
            self.cat.setCat([split[self.CATEGORY]])
            if self.cat[:].empty:
                self.tree.rm(child=split[self.CATEGORY])
            return False

        self.__restoreAll__(block=['op', 'trans', 'cat'],
                            change_split=change,
                            change_cat=self.split.impSplit)

        return True

    @writeOp
    def splitRm(self, oper_n=0, category='') -> bool:
        """remove split filter, also remove cat from tree
        if category have kids, kids will go to grandpa
        """
        oper_n = self.str2int(oper_n)

        changeSplit = self.split.rm(oper_n=oper_n, category=category)

        self.tree.rm(category)
        self.__restoreAll__(block=['op', 'trans'],
                            change_split=changeSplit,
                            change_cat=self.split.impSplit)
        return True

    @writeOp
    def catAdd(self, fltr: "dict", parent=cfg.GRANDPA) -> bool:
        """add new filter, also create cat in tree under parent\n
        can be also used for replacement when oper_n provided\n
        also possible to pass multiple dicts in list\n
        not allowed on category in ['grandpa', '*']\n
        minimum input: {col_name: str, function: str, filter: str, oper: str}\n
        optionally:\n
        - category, other way will use self.curCat\n
        - oper_n, will replace at position if provided, other way will add after last one
        """
        change = self.cat.add(fltr)
        if not change:
            return False

        if not self.tree.add(parent, change[0]):
            self.cat.rm(category=change[self.CATEGORY],
                        oper_n=change[self.OPER_N])
            return False

        self.__restoreAll__(block=['op', 'trans', 'split'],
                            change_cat=change)
        self.msg(f'Category {fltr[self.CATEGORY]} added')
        return True

    @writeOp
    def catRm(self, category: str, oper_n=0) -> List[Union[Dict, None]]:
        """remove filter or category and update tree\n
        not allowed on category in ['grandpa', '*']\n
        if category not given will use self.curCat\n
        if oper_n=0, remove whole category
        if split also in this cat, keep cat in tree
        if split ref to cat, return removed split so possible to restore when cat edited only
        """
        oper_n = self.str2int(oper_n)
        if not oper_n:
            return []

        rmCat = self.cat.rm(category, oper_n)

        # need to update ref split
        rmSplit = self.split.rm(fltr=category,
                                category=category)
        rmSplitCat = self.split.impSplit
        # categories with split only must be removed from op now
        # before cat update
        # also completely removed categories shall be removed now
        # becouse updete cat ....wil not update
        self.op.__rmCat__(rmSplitCat + rmCat)

        self.__restoreAll__(block=['op', 'trans'],
                            change_split=rmSplit,
                            change_cat=rmCat + rmSplitCat)
        return rmSplit

    @writeOp
    def catMov(self, oper_n: int, new_oper_n: int, category: str) -> None:
        """move filter at oper_n to new position
        """
        change = self.cat.mov(oper_n, new_oper_n, category)
        if not change:
            return

        rmSplit = self.split.rm(filter=category,
                                category=category,
                                confirm=False)
        rmSplitCat = [s[self.CATEGORY] for s in rmSplit]

        self.__restoreAll__(block=['op', 'trans'],
                            change_split=rmSplit,
                            change_cat=change + rmSplitCat)
        return

    @writeOp
    def transAdd(self, fltr: dict):
        """add new transformation, can be also used for replacement when trans_n provided\n
        also possible to pass multiple dicts in list\n
        minimum input: {bank: str,col_name: str, oper: str, val1: str, val2: str}\n
        optionally:\n
        - trans_n, will put at position if provided, other way will add after last one
        """
        change_trans = self.trans.add(fltr)
        if not change_trans:
            return False

        self.__restoreAll__()
        return True

    @writeOp
    def transRm(self, trans_n: int) -> bool:
        # to restore cat, need to replace FILTER with FILTER_ORIG
        # we can reverse only cats existed before filtering
        # warn when removing filter created before category (see CAT.__update__())
        trans_n = self.str2int(trans_n)
        if not trans_n:
            return

        self.cat.__restore__(self.trans.__to_dict__(trans_n))

        self.trans.rm(trans_n)

        self.__restoreAll__()
        return True

    @writeOp
    def transMov(self, trans_n: int, new_trans_n: int) -> bool:
        # to restore cat, need to replace FILTER with FILTER_ORIG
        # we can reverse only cats existed before filtering
        # warn when removing filter created before category
        self.cat.__restore__(self.trans.__to_dict__(trans_n))

        self.trans.mov(trans_n, new_trans_n)
        self.__restoreAll__()
        return True

    @writeOp
    def treeRen(self, new_category: str, category: str):
        if not self.tree.ren(category, new_category):
            return

        # need also change split filter, may refer to changed cat
        change_split = self.split.ren(new_category, category)
        change_cat = self.cat.ren(new_category, category)
        self.op.__rmCat__([category])  # remove old category

        self.__restoreAll__(block=['op', 'trans'],
                            change_split=change_split,
                            change_cat=change_cat + self.split.impSplit)
        return True

    @writeOp
    def treeAdd(self, parent: str, child: str):
        self.tree.add(parent, child)
        return True

    @writeOp
    def treeArrange(self, category: str, dir: str):
        """Move category in tree
        dir is direction in which to move
        move on the same level only
        then rearange the categories in cat
        """
        categories = self.tree.arrange(category=category, dir=dir)
        if categories:
            self.cat.arrange(categories=categories)
        self.__restoreAll__(block=['op', 'trans'])
        return

    @writeOp
    def treeMov(self, new_parent: str, child: str):
        self.tree.mov(new_parent, child)
        return True

    @writeOp
    def treeRm(self, child: str):
        '''
        remove cat from tree, together with cat and split,
        possibly, will also remove cat with split refering to child
        '''
        splitChange = self.split.rm(
            category=child, fltr=child)

        self.cat.rm(category=child)
        self.tree.rm(child)
        for s in self.split.impSplit:
            # remove tree only if no category
            self.cat.setCat([s])
            if self.cat[:].empty:
                self.tree.rm(s)

        self.__restoreAll__(block=['op', 'trans'],
                            change_split=splitChange,
                            change_cat=[child] + self.split.impSplit)
        return True

    @writeOp
    def impRm(self, bank: str):
        """remove data from imp db
        """
        self.imp.pop(bank)
        self.__restoreAll__()

    def __restoreAll__(self, block=[], change_split=['*'], change_cat=['*']):
        """restore all DB, if nothing indicated restore all filters
        to block filters in some DB, put into list block. Add 'op' to not reset op DB
        When only cat changed, consider updating of impacted split, both filter and category
        """
        # restore whole op DB
        # restore also if  changed split db
        if 'op' not in block:  # or change_split != []:
            self.op = OP(self)
            for bank, db in self.imp:
                self.op.ins(db=db)
            block = []
            change_cat = ['*']
            change_split = ['*']

        if change_split == ['*']:
            change_split = self.split.__to_dict__(change_split)

        # order is important
        if 'trans' not in block:
            change_trans = self.trans.__to_dict__()
            self.cat.__update__(change_trans)
            self.op.__updateTrans__(change_trans)
        if 'cat' not in block:
            change_cat_fltr = self.cat.__to_dict__(change_cat)
            if not change_cat_fltr:
                # removed category but we need to update op anyway
                change_cat_fltr = [{self.CATEGORY: c} for c in change_cat]
                self.op.__updateCat__(change_cat_fltr, removeOnly=True)
            else:
                self.op.__updateCat__(change_cat_fltr)
        if 'split' not in block:
            self.op.__updateSplit__(change_split)
        
        if len(self.op.op.index) == 0:
            self.IsData = False
        else:
            self.IsData = True

# file operation methods
    def impCommit(self, decision: str):
        """
        Commit imported data if decision=='ok'\n
        otherway will drop and restore orginal dbs\n
        do NOT save dbs
        """
        if not self.imp_status:
            return

        if decision == 'ok':
            self.imp.commit()
            # split very likely will be unintentionaly removed
            # (due to insuficient funds)
            # so we need to merge with orginal
            self.split.merge(self.split_before_imp)
            self.__restoreAll__()
            self.msg('Data added to DB')

        else:  # not ok
            self.imp.pop()

            self.op.op = self.op_before_imp.copy()
            self.cat.cat = self.cat_before_imp.copy()
            self.trans.trans = self.trans_before_imp.copy()
            self.tree.tree = self.tree_before_imp.copy()
            self.split.split = self.split_before_imp.copy()

            self.msg('Import rejected. Restored main DB')

        self.op_before_imp = pandas.DataFrame()
        self.trans_before_imp = pandas.DataFrame()
        self.cat_before_imp = pandas.DataFrame()
        self.tree_before_imp = pandas.DataFrame()
        self.split_before_imp = pandas.DataFrame()
        self.imp_status = False

    def impData(self, file='') -> bool:
        """import excell file, does not commit!\n
        this means creating op_before_imp for current data until commit decision
        """
        file = self.fs.setIMP(file)
        if not file:
            self.msg(f'Missing file {file}. Nothing imported')
            return False

        try:
            xls = pandas.read_excel(file)
        except:
            self.msg(f'wrong import file {file}')
            self.imp_status = False
            return False

        bank = self.__checkBank__(list(xls.columns))
        if not bank:
            self.imp_status = False
            return False
        xls = xls.reindex(columns=cfg.bank[bank])
        xls.columns = cfg.op_col
        # set bank name
        xls.bank = bank
        # hash data, NEVER hash again, only at very begining with bank attached
        xls.hash = pandas.util.hash_pandas_object(xls, index=False)

        # remove cells with whitespaces
        xls.replace(r'^\s+$', np.nan, regex=True, inplace=True)
        xls = self.__correct_col_types__(xls)

        self.op_before_imp = self.op.op.copy()
        self.cat_before_imp = self.cat.cat.copy()
        self.trans_before_imp = self.trans.trans.copy()
        self.tree_before_imp = self.tree.tree.copy()
        self.split_before_imp = self.split.split.copy()

        self.imp.ins(bank=bank, db=xls.copy())

        self.__restoreAll__()
        self.msg(f'Iported data from {bank} bank. Review and confirm import.')
        self.imp_status = True
        return True

    def openDB(self, file='', onlyTrans=False) -> bool:
        """
        opens sql DB
        when file is missing get current file or lastOpened
        when onlyTrans, import only transformations, without op DB
        """
        if self.imp_status:
            # not allowed when in import mode
            self.msg('finish importing before opening another DB')
            return False

        if not onlyTrans:
            file = self.fs.setDB(file)
        else:
            file = self.fs.setIMPDB(file)

        if not file:
            return False

        engine = sqlite3.connect(file)
        tabs = cfg.DB_tabs.copy()
        if onlyTrans:
            tabs.remove('op')
        try:
            for tab in tabs:
                query = f'SELECT * FROM {tab}'
                exec(f'self.{tab}.{tab} = pandas.read_sql(query, engine)')
            if not onlyTrans:
                self.op.op = self.__correct_col_types__(self.op.op)
                cur = engine.cursor()
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
                bnks = cur.fetchall()
                bnks = [i[0] for i in bnks
                        if i[0] not in cfg.DB_tabs]
                for bnk in bnks:
                    query = f'SELECT * FROM {bnk}'
                    exec(
                        f'self.imp.imp["{bnk}"] = pandas.read_sql(query, engine)')
                    exec(
                        f'self.imp.imp["{bnk}"] = self.__correct_col_types__(self.imp.imp["{bnk}"])')
            if onlyTrans:
                self.impCommit(decision='ok')
            else:
                self.imp.commit()
        except:
            engine.close()
            self.msg(f'DB.__open_db__: Not correct DB: {file}')
            self.op = OP(self)
            self.cat = CAT(self)
            self.trans = TRANS(self)
            self.tree = TREE(self)
            self.imp = IMP(self)
            self.split = SPLIT(self)
            return False
        engine.close()
        self.msg(f"opened db: {file}")
        return True

    def writeDB(self, file='') -> bool:
        """
        save all dbs as sqlite file
        used also to create new DB
        """
        # not allowed when in import mode
        if self.imp_status:
            self.msg('finish importing before saving DB')
            return False

        file = self.fs.setDB(file)

        if file:
            self.__create_db__(file)
        else:
            self.msg(f'no DB {file}. Nothing written.')
            return False

        engine = sqlite3.connect(file)
        try:
            for tab in cfg.DB_tabs:
                exec(
                    f'''self.{tab}.{tab}.to_sql('{tab}', engine, if_exists='replace', index=False)''')
            for bnk in self.imp.imp.keys():
                exec(
                    f'''self.imp.imp['{bnk}'].to_sql('{bnk}', engine, if_exists='replace', index=False)''')
        except:
            self.msg('write failed')
            engine.close()
            return False
        engine.close()
        self.msg(f'written new DB: {file}. File overwritten if existed')
        return True

    def exportCSV(self, file='') -> bool:
        """
        Export db with operations and categories
        """
        file = self.fs.setCSV(file)
        if not file:
            return False

        colNames = self.op.op.columns.values.tolist()
        colNames.remove(self.HASH)
        db = self.op.op.copy(deep=True)
        path = ['/'.join(self.tree.path(cat))
                for cat in db[self.CATEGORY]]
        db[self.CATEGORY] = path
        db.to_csv(path_or_buf=file,
                  columns=colNames,
                  index=False)
        self.msg(f'data exported to {file}')
        return True

    def __checkBank__(self, cols: list) -> str:
        """check if cols exist in cfg file
        return bank name for cols
        """
        for bank in cfg.bank.keys():
            # check if ALL cols element are in bank cols
            raw_bank = [i for i in cfg.raw_bank[bank] if i]
            matchCols = [i for i in raw_bank if i in cols]
            if len(matchCols) == len(raw_bank):
                return bank

        self.msg('file from unknown bank')
        return None

    def __correct_col_types__(self, df) -> pandas:
        df_new = pandas.DataFrame()
        n_col = len(cfg.op_col_type)
        for i in range(n_col):
            num_type = cfg.op_col_type[i]
            col_name = cfg.op_col[i]
            if col_name in df.columns:
                df_new.loc[:, col_name] = df.loc[:, col_name]
            else:
                df_new.loc[:, col_name] = [None] * len(df)

            if num_type in ['INT', 'REAL']:
                df_new.iloc[:, i] = self.__str2num__(
                    df_new.iloc[:, i], num_type)
            elif num_type == 'TEXT':
                df_new.iloc[:, i] = df_new.iloc[:, i].astype('string')
            elif num_type == 'TIMESTAMP':
                df_new.iloc[:, i] = pandas.to_datetime(df_new.iloc[:, i])
        return df_new

    @ staticmethod
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
                i = ''.join(x for x in i
                            if x.isdigit() or x == '.' or x == ',')
                i = i.replace(",", ".")
                if i:
                    if num_type == "INT":
                        i = int(i)
                    else:
                        i = float(i)
                    col_digit.append(i)
        return pandas.Series(col_digit, dtype='float')

    def __create_db__(self, file: str) -> bool:
        dirName = os.path.dirname(file)
        if not os.path.exists(dirName):
            self.msg(f'{dirName} is not correct path')
            return False
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
        db_file.close()
        return True

# info methods
    def dataRange(self, bank='') -> list:
        """return data range for selected bank
        if bank not selected check min max among all banks
        return list [start_dat, end_date]
        """
        if not bank:
            bank = self.dataBanks()
        else:
            bank = [bank]
        datMin = self.imp.imp[bank[0]][self.DATA_OP].min()
        datMax = self.imp.imp[bank[0]][self.DATA_OP].max()
        for b in bank[1:]:
            datMin = min(datMin, self.imp.imp[b][self.DATA_OP].min())
            datMax = max(datMax, self.imp.imp[b][self.DATA_OP].max())
        return [datMin, datMax]

    def dataBanks(self) -> list:
        """return available banks imported"""
        return self.imp.scope

    def dataRows(self) -> List:
        """return #rows in db and percent of categorized rows
        """
        if isinstance(self.op.op, pandas.DataFrame):
            catRows = self.op.op.loc[:, self.CATEGORY] != cfg.GRANDPA
            catRowsL = len(self.op.op.loc[catRows, :])
            rowsL = len(self.op.op)
            per = 0
            if rowsL != 0:
                per = catRowsL / rowsL
            return [rowsL, per]
        else:
            return [0, 0]

    def dataHist(self, bank: str) -> pandas:
        """return data histogram (date vs #operations)
        from imp db
        """
        return self.imp.imp[bank][self.DATA_OP].hist()
