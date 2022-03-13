import unittest as ut
from db import DB
from dev.integrationTest.decorators import blockWrite, compJSON, readJSON, writeJSON
from modules import FileSystem


class testDBinfo(ut.TestCase):
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

    @blockWrite
    def testDataRange(self):
        """dataRange"""
        banks = self.db.dataBanks()
        self.assertEqual(str(self.db.dataRange(
            banks[0])), "[Timestamp('2021-10-21 00:00:00'), Timestamp('2022-01-17 00:00:00')]")
        self.assertEqual(str(self.db.dataRange(
        )), "[Timestamp('2021-09-01 00:00:00'), Timestamp('2022-01-17 00:00:00')]")

    @blockWrite
    def testDataRows(self):
        """dataRows"""
        self.assertEqual(str(self.db.dataRows()), '[876, 0.0]')

    @writeJSON
    @readJSON
    @compJSON
    def testDataHist(self):
        """dataHist"""
        banks = self.db.dataBanks()
        return self.db.dataHist(banks[0]).to_json()


if __name__ == '__main__':
    ut.main()
