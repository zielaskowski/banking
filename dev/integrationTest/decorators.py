import json
import pandas as pd
import re

"""
Decorators to allow easy creation of fixtures for test cases
when self.read==False & self.write==True, will write all db to csv files
wehen self.read==True & self.write==False, read csv files for comparision with actual dbs
set fixtures location in self.path
"""


def writeRes(fn):
    def deco(self, *args, **kwargs):
        fn(self, *args, **kwargs)
        if self.write:
            try:
                DEBUG_Fn = self.DEBUG_Fn
                fNameId = self.fNameId
                fName = self.fName
            except:
                DEBUG_Fn = self.__module__.split('.')[-1]
                fNameId = ''
                fName = self._testMethodName
            self.db.op.op.to_csv(
                self.path +
                f'{DEBUG_Fn}_'
                f'{fNameId}_'
                f'{fName}_op',
                index=False)
            self.db.cat.cat.to_csv(
                self.path +
                f'{DEBUG_Fn}_'
                f'{fNameId}_'
                f'{fName}_cat',
                index=False)
            self.db.split.split.to_csv(
                self.path +
                f'{DEBUG_Fn}_'
                f'{fNameId}_'
                f'{fName}_split',
                index=False)
            self.db.trans.trans.to_csv(
                self.path +
                f'{DEBUG_Fn}_'
                f'{fNameId}_'
                f'{fName}_trans',
                index=False)
            self.db.tree.tree.to_csv(
                self.path +
                f'{DEBUG_Fn}_'
                f'{fNameId}_'
                f'{fName}_tree',
                index=False)
        return
    return deco


def readRes(fn):
    def deco(self, *args, **kwargs):
        if self.read:
            try:
                DEBUG_Fn = self.DEBUG_Fn
                fNameId = self.fNameId
                fName = self.fName
            except:
                DEBUG_Fn = self.__module__.split('.')[-1]
                fNameId = ''
                fName = self._testMethodName
            self.fixtures = {'op': pd.read_csv(f'{self.path}'
                                               f'{DEBUG_Fn}_'
                                               f'{fNameId}_'
                                               f'{fName}_op'),
                             'cat': pd.read_csv(f'{self.path}'
                                                f'{DEBUG_Fn}_'
                                                f'{fNameId}_'
                                                f'{fName}_cat',
                                                na_filter=False,
                                                dtype='str'),
                             'split': pd.read_csv(f'{self.path}'
                                                  f'{DEBUG_Fn}_'
                                                  f'{fNameId}_'
                                                  f'{fName}_split',
                                                  na_filter=False,
                                                  dtype='str'),
                             'trans': pd.read_csv(f'{self.path}'
                                                  f'{DEBUG_Fn}_'
                                                  f'{fNameId}_'
                                                  f'{fName}_trans',
                                                  na_filter=False,
                                                  dtype='str'),
                             'tree': pd.read_csv(f'{self.path}'
                                                 f'{DEBUG_Fn}_'
                                                 f'{fNameId}_'
                                                 f'{fName}_tree',
                                                 na_filter=False,
                                                 dtype='str')}
            self.fixtures['op'] = self.db.__correct_col_types__(
                self.fixtures['op'])
        return fn(self, *args, **kwargs)
    return deco


def compRes(fn):
    def deco(self, *args, **kwargs):
        fn(self, *args, **kwargs)
        if self.read:
            pd.testing.assert_frame_equal(
                self.db.op.op,
                self.fixtures['op'],
                check_dtype=False,
                check_index_type=False,
                obj='op_db')
            pd.testing.assert_frame_equal(
                self.db.cat.cat.astype('str'),
                self.fixtures['cat'],
                check_dtype=False,
                check_index_type=False,
                obj='cat_db')
            pd.testing.assert_frame_equal(
                self.db.split.split.astype('str'),
                self.fixtures['split'],
                check_dtype=False,
                check_index_type=False,
                obj='split_db')
            pd.testing.assert_frame_equal(
                self.db.trans.trans.astype('str'),
                self.fixtures['trans'],
                check_dtype=False,
                check_index_type=False,
                obj='trans_db')
            pd.testing.assert_frame_equal(
                self.db.tree.tree, self.fixtures['tree'],
                check_dtype=False,
                check_index_type=False,
                obj='tree_db')
        return
    return deco


def blockWrite(fn):
    def deco(self, *args, **kwargs):
        if not self.write:
            fn(self, *args, **kwargs)
    return deco


def writeJSON(fn):
    def deco(self, *args, **kwargs):
        dat = fn(self, *args, **kwargs)
        if self.write:
            with open(self.fs.getDB(path=True)+self._testMethodName, 'w') as file:
                json.dump(dat, file)
        return
    return deco


def readJSON(fn):
    def deco(self, *args, **kwargs):
        if self.read:
            with open(self.fs.getDB(path=True) + self._testMethodName, 'r') as file:
                self.fixtures = json.load(file)
        return fn(self, *args, **kwargs)
    return deco


def compJSON(fn):
    def deco(self, *args, **kwargs):
        dat = fn(self, *args, **kwargs)
        if self.read:
            self.assertEqual(dat, self.fixtures)
        return dat
    return deco


def writeOp(fn):
    # write fn arguments values and fn name to csv (DEBUG_F)
    def deco(self, *args, **kwargs):
        if self.DEBUG:
            fnName = fn.__name__
            with open(self.DEBUG_F, 'a') as f:
                f.write(f'{fnName};{json.dumps(args)};{json.dumps(kwargs)}\n')
        return fn(self, *args, **kwargs)
    return deco
