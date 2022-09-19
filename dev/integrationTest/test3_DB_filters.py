import unittest as ut
import re

from db import DB
from dev.integrationTest.decorators import *


class testDBfromFile(ut.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        self.db = DB(DEBUG=False)
        self.DEBUG_F = './dev/integrationTest/setTest_filters.csv'
        self.DEBUG_Fn = re.sub("^.*(?=\/)\/", "", self.DEBUG_F) #remove path
        self.DEBUG_Fn = re.sub("\.[^\.]*$", "", self.DEBUG_Fn) #remove file ext
        ####################################
        # use write==True when setting test environment (fixtures)
        # after, use write==False for real testing
        self.write = True
        ####################################
        self.read = not self.write
        self.fixtures = {}
        self.fName = "" #store name of called function
        self.fNameId = 0
        
        # create empty DB
        self.db.writeDB(file='./dev/integrationTest/fixtures/db/testDB.s3db')
        self.db.impData('./dev/op_history/Zestawienie operacji.xlsx')
        self.db.impCommit(decision='ok')
        self.path = self.db.fs.getDB(path=True)

    def testIterateFunc(self):
        """
        Iterate through file
        and call each line function with args and kwargs
        """
        with open(self.DEBUG_F, 'r') as f:
            file = f.readlines()
        for l in file:
            self.fName, a, k = l.split(';')
            args = json.loads(a)
            kwargs = json.loads(k)
            self.fNameId += 1
            
            @writeRes
            @readRes
            @compRes
            def test(self):
                """call the function"""
                eval(f'self.db.{self.fName}(*{args}, **{kwargs})')
            test(self)

if __name__ == '__main__':
    ut.main()
