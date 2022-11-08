from fnmatch import fnmatch
import unittest as ut
import re

from db import DB
from dev.integrationTest.decorators import *


class testDBfromFile(ut.TestCase):
    @classmethod
    def setUpClass(self) -> None:
        # set path for fixtures
        self.path = './dev/integrationTest/fixtures/'
        
        self.db = DB(DEBUG=False)
        ####
        # select test suite
        #self.DEBUG_F = './dev/integrationTest/setTest_cat.csv'
        #self.DEBUG_F = './dev/integrationTest/setTest_split.csv'
        self.DEBUG_F = './dev/integrationTest/setTest_allFilters.csv'
        ####
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
        self.db.writeDB(file=self.path+'db/testDB.s3db')
        self.db.impData('./dev/op_history/Zestawienie operacji.xlsx')
        self.db.impCommit(decision='ok')

    def testIterateFunc(self):
        """
        Iterate through file
        and call each line function with args and kwargs
        """
        with open(self.DEBUG_F, 'r') as f:
            file = f.readlines()
        for l in file:
            if l[0]=='#': # comment
                continue
            
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
        self.db.writeDB()

if __name__ == '__main__':
    ut.main()
