import unittest as ut
from db import DB
from modules import FileSystem
from dev.integrationTest.decorators import *


class testDBfilter(ut.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.db = DB()
        self.fs = FileSystem()
        ####################################
        # use write==True when setting test environment (fixtures)
        # after, use write==False for real testing
        self.write = False
        ####################################
        self.read = not self.write
        self.fixtures = {}

        self.fs.setDB(
            path='./dev/integrationTest/fixtures/testDB.s3db')
        # if sqlite file not present, run test_DB_fileOper
        self.db.openDB(self.fs.getDB())
        self.path = self.fs.getDB(path=True)

    @writeRes
    @readRes
    @compRes
    def test01transAdd(self):
        """transAdd"""
        fltr = {'bank': 'all', 'col_name': 'ALL', 'oper': 'str.replace',
                'val1': 'Tytuł: ', 'val2': '', 'trans_n': ''}
        self.assertTrue(self.db.transAdd(fltr=fltr))

        fltr = {'bank': 'ipko', 'col_name': 'lokalizacja', 'oper': 'str.replace',
                'val1': 'Lokalizacja: ', 'val2': '', 'trans_n': ''}
        self.assertTrue(self.db.transAdd(fltr=fltr))

    @writeRes
    @readRes
    @compRes
    def test02catAdd(self):
        """catAdd"""
        fltr = {'col_name': 'ALL', 'function': 'txt_match', 'filter': 'Adres: pyszne.pl',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'rest', 'filter_orig': ''}
        self.assertTrue(self.db.catAdd(fltr))

    @writeRes
    @readRes
    @compRes
    def test03transAdd(self):
        """transAdd"""
        fltr = {'bank': 'all', 'col_name': 'ALL', 'oper': 'str.replace',
                'val1': 'Adres: ', 'val2': '', 'trans_n': ''}
        self.assertTrue(self.db.transAdd(fltr=fltr))

    @writeRes
    @readRes
    @compRes
    def test04transMov(self):
        """transMov"""
        self.assertTrue(self.db.transMov(2, 1))

    @writeRes
    @readRes
    @compRes
    def test05transMov(self):
        """transRm"""
        self.assertTrue(self.db.transRm(2))

    @writeRes
    @readRes
    @compRes
    def test06catRm(self):
        """catRm"""
        self.db.catRm(category='rest')

    @writeRes
    @readRes
    @compRes
    def test07catAdd(self):
        """catAdd"""
        fltr = {'col_name': 'ALL', 'function': 'txt_match', 'filter': 'pyszne.pl',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'rest', 'filter_orig': ''}
        self.assertTrue(self.db.catAdd(fltr))

        fltr = {'col_name': 'ALL', 'function': 'txt_match', 'filter': 'BAR OK ORIENTALNY',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'rest', 'filter_orig': ''}
        self.assertTrue(self.db.catAdd(fltr))

        fltr = {'col_name': 'ALL', 'function': 'txt_match', 'filter': 'PANEK CAR SHARING',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'carshare', 'filter_orig': ''}
        self.assertTrue(self.db.catAdd(fltr))

        fltr = {'col_name': 'ALL', 'function': 'txt_match', 'filter': 'TRAFICAR',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'carshare', 'filter_orig': ''}
        self.assertTrue(self.db.catAdd(fltr))

    @writeRes
    @readRes
    @compRes
    def test08catMov(self):
        """catMov"""
        self.db.catMov(2, 1, 'carshare')

    @writeRes
    @readRes
    @compRes
    def test09treeRen(self):
        """treeRen"""
        self.db.treeRen('carS', 'careshare')

    @writeRes
    @readRes
    @compRes
    def test10treeAdd(self):
        """treeAdd"""
        self.db.treeAdd('carshare', 'someCat')

    @writeRes
    @readRes
    @compRes
    def test11treeMov(self):
        """treeMov"""
        self.db.treeMov('someCat', 'rest')

    @writeRes
    @readRes
    @compRes
    def test12treeRm(self):
        """treeRm"""
        self.db.treeRm('someCat')

    @writeRes
    @readRes
    @compRes
    def test13ImpRm(self):
        """impRm"""
        banks = self.db.dataBanks()
        self.db.impRm(banks[-1])

    @writeRes
    @readRes
    @compRes
    def test14splitAdd(self):
        """splitAdd"""
        fltr = {'col_name': 'typ_transakcji', 'function': 'txt_match', 'filter': 'Wypłata z bankomatu',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'bank', 'filter_orig': ''}
        self.db.catAdd(fltr)

        split = {'start_date': '*', 'end_date': '*', 'col_name': 'kategoria',
                 'filter': 'bank', 'val1': '-5', 'days': '10', 'split_n': '', 'kategoria': 'faje'}
        self.db.splitAdd(split)

        split = {'start_date': '*', 'end_date': '2022-01-17', 'col_name': 'kategoria',
                 'filter': 'carshare', 'val1': '-2', 'days': '10', 'split_n': '', 'kategoria': 'faje'}
        self.db.splitAdd(split)

    @writeRes
    @readRes
    @compRes
    def test15splitRm(self):
        """splitRm"""
        split = {'start_date': '*', 'end_date': '*', 'col_name': 'kategoria',
                 'filter': 'carshare', 'val1': '', 'days': '', 'split_n': 3, 'kategoria': 'faje'}
        self.db.splitAdd(split)
        self.db.splitRm(3)

    @writeRes
    @readRes
    @compRes
    def test16splitAdd(self):
        """splitAdd"""
        split = {'start_date': '2021-10-21', 'end_date': '2021-12-30', 'col_name': 'kategoria',
                 'filter': 'faje', 'val1': '-2', 'days': '40', 'split_n': '', 'kategoria': 'alko'}
        self.db.splitAdd(split)

    @writeRes
    @readRes
    @compRes
    def test17catAdd(self):
        """catAdd"""
        fltr = {'col_name': 'lokalizacja', 'function': 'txt_match', 'filter': 'BOLT.EU',
                'filter_n': '', 'oper': 'add', 'oper_n': '', 'kategoria': 'alko', 'filter_orig': ''}
        self.db.catAdd(fltr)

    @blockWrite
    def test99writeDB(self):
        self.db.writeDB(self.path+'final_filters.s3db')


if __name__ == '__main__':
    ut.main()
