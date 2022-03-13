from operator import index
import unittest as ut
import os
import pandas as pd
from db import DB
from modules import FileSystem
from dev.integrationTest.decorators import *


class testDBfileOper(ut.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.db = DB()
        self.fs = FileSystem()
        self.fs.setIMP(
            path='./dev/integrationTest/fixtures/history_20220118_002911.xls')
        self.path = self.fs.getIMP(path=True)
        self.fs.setDB(self.path+'testDB')

        ####################################
        # use write==True when setting test environment (fixtures)
        # after, use write==False for real testing
        self.write = False
        ####################################
        self.read = not self.write
        self.fixtures = {}


    @writeRes
    @readRes
    @compRes
    def test1ImportData(self):
        """impData"""
        self.assertFalse(self.db.impData(file=""))
        self.assertFalse(self.db.impData(file="bla"))
        
        self.assertTrue(self.db.impData(file=self.fs.getIMP()))
        self.db.impCommit('ok')

        self.fs.setIMP(self.path+'Zestawienie operacji.xlsx')
        self.assertTrue(self.db.impData(file=self.fs.getIMP()))
        self.db.impCommit('ok')

    @blockWrite
    def test2WriteDB(self):
        """writeDB"""
        self.assertFalse(self.db.writeDB(''))

        self.assertTrue(self.db.writeDB(self.fs.getDB()))
        self.assertTrue(os.path.isfile(self.fs.getDB()))

        self.db.impData(file=self.fs.getIMP())
        self.assertFalse(self.db.writeDB(self.fs.getDB()))
        self.db.impCommit('not ok')

    @writeRes
    @readRes
    @compRes
    def test3OpenDB(self):
        """openDB"""
        self.db = DB()
        self.assertFalse(self.db.openDB(''))
        self.assertFalse(self.db.openDB('bla'))

        self.assertTrue(self.db.openDB(self.fs.getDB()))

        self.db.impData(file=self.fs.getIMP())
        self.assertFalse(self.db.openDB(self.fs.getDB()))
        self.db.impCommit('not ok')

    @blockWrite
    def test4OpenDBfilters(self):
        self.fs.setDB(self.path + 'filters.s3db')
        self.assertTrue(self.db.openDB(file=self.fs.getDB(), onlyTrans=True))

    @blockWrite
    def test5ExportCSV(self):
        """exportCSV"""
        self.assertFalse(self.db.exportCSV(''))

        self.assertTrue(self.db.exportCSV(self.path + 'export.csv'))
        self.fs.setCSV(self.path + 'export.csv')
        self.assertTrue(os.path.isfile(self.fs.getCSV()))


if __name__ == '__main__':
    ut.main()
