import unittest as ut
import os

from db import DB
from dev.integrationTest.decorators import *


class testDBfileOper(ut.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.db = DB()

        # set path for fixtures
        self.path = './dev/integrationTest/fixtures/'

        ####################################
        # use write==True when setting test environment (fixtures)
        # after, use write==False for real testing
        self.write = False
        ####################################
        self.read = not self.write
        self.fixtures = {}

    @blockWrite
    def test1WriteDB(self):
        """writeDB"""
        dbFile = 'testDB.s3db'
        dbPath = self.path+'db/'

        self.assertFalse(self.db.writeDB('foo'))
        self.assertTrue(self.db.writeDB(file=dbPath+dbFile))

        self.assertTrue(dbFile == self.db.fs.getDB(file=True))

        self.assertTrue(self.db.writeDB())
        self.assertTrue(os.path.isfile(self.db.fs.getDB()))

        self.db.impData(file='./dev/op_history/Zestawienie operacji.xlsx')
        self.assertFalse(self.db.writeDB())
        self.db.impCommit('not ok')

    @writeRes
    @readRes
    @compRes
    def test2ImportData(self):
        """impData"""
        impFile = './dev/op_history/history_20220118_002911.xls'

        self.assertTrue(self.db.writeDB(self.path+'db/testDB.s3db'))
        self.assertFalse(self.db.impData(file="foo"))

        self.assertTrue(self.db.impData(impFile))
        self.assertTrue(self.db.imp_status)
        self.db.impCommit(decision='ok')
        self.assertFalse(self.db.imp_status)

        self.assertTrue(self.db.impData())
        self.db.impCommit('not ok')

        self.assertTrue('history_20220118_002911.xls' ==
                        self.db.fs.getIMP(file=True))
        self.assertTrue(self.db.writeDB())

    @blockWrite
    def test3OpenDB(self):
        """openDB"""
        self.db = DB()
        dbFile = 'testDB.s3db'
        dbPath = self.path+'db/'
        self.assertTrue(self.db.writeDB(file=dbPath+dbFile))
        
        self.assertFalse(self.db.openDB('foo'))

        self.assertTrue(self.db.openDB())

        self.db.impData(file='./dev/op_history/Zestawienie operacji.xlsx')
        self.assertFalse(self.db.openDB())
        self.db.impCommit('not ok')

    @writeRes
    @readRes
    @compRes
    def test4OpenDBfilters(self):
        """open DB"""
        self.assertTrue(self.db.openDB(
            file=self.path + 'db/filters.s3db', onlyTrans=True))

        self.assertTrue(self.db.fs.getIMPDB(file=True) == 'filters.s3db')

    @blockWrite
    def test5ExportCSV(self):
        """exportCSV"""
        exFile = self.path + 'export.csv'
        self.assertFalse(self.db.exportCSV('foo'))

        self.assertTrue(self.db.exportCSV(exFile))
        self.assertTrue(os.path.isfile(self.db.fs.getCSV()))
        self.assertTrue('export.csv' == self.db.fs.getCSV(file=True))


if __name__ == '__main__':
    ut.main()
