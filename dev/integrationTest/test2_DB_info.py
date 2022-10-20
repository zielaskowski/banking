import unittest as ut
from db import DB
from dev.integrationTest.decorators import blockWrite, compJSON, readJSON, writeJSON


class testDBinfo(ut.TestCase):
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

        # setUp sql db
        impFile1 = './dev/op_history/history_20220118_002911.xls'
        impFile2 = './dev/op_history/Zestawienie operacji.xlsx'
        dbFile = 'testDB.s3db'
        dbPath = self.path+'db/'
        self.db.impData(impFile1)
        self.db.impCommit(decision='ok')
        self.db.impData(impFile2)
        self.db.impCommit(decision='ok')
        self.db.writeDB(file=dbPath+dbFile)

    @blockWrite
    def testDataRange(self):
        """dataRange"""
        banks = self.db.dataBanks()
        self.assertEqual(str(self.db.dataRange(banks[0])),
                         "[Timestamp('2021-10-21 00:00:00'), Timestamp('2022-01-17 00:00:00')]")
        self.assertEqual(str(self.db.dataRange(banks[1])),
                         "[Timestamp('2021-09-01 00:00:00'), Timestamp('2022-01-10 00:00:00')]")
        self.assertEqual(str(self.db.dataRange()),
                         "[Timestamp('2021-09-01 00:00:00'), Timestamp('2022-01-17 00:00:00')]")

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
