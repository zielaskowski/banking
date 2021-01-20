import os, sys, json
import pandas as pd

class FileSystem:
    """Handles all file system stuff

    Define file path and names. For each type stores as list of path, name and extensions:\n
    -imported XLS file:          self._fileXLS, access through self.getIMP and self.setIMP\n
    -DB file:                    self._fileDB, access through self.getDB and self.setDB\n
    -aplication location:        self._fileAPP, access through self.getAPP\n
    -configuration file:         self._fileCONF, access to options through self.getOpt and self.writeOpt\n

    Handles expected type of file: SQlite3, TXT used as parameter for QtFileDialog
    """

    _PS = os.path.sep  # / for linux; \\ for win
    _PATH = 0  # location of path in self._fileXXX list
    _NAME = 1  # location of name in self._fileXXX list
    _EXT = 2  # location of extension in self._fileXXX list

    def __init__(self):
        self._fileIMP = ['', '', '']
        self._fileDB = ['', '', '']
        self._fileAPP = ['', '', '']
        self._fileCONF = ['opt', 'conf', '.txt']  # Configuration file. name is constant
        self.option = {"LastDB": '',
                       "welcome": "Write welcome message into ./opt/conf.txt...",
                       "visColumns": [],
                       "winSize": ''}
        self.typeIMP = ['excel', '*.xls *.xlsx']
        self.typeDB = ['SQlite3', '.s3db']

        self.setAPP()

        self._fileCONF[self._PATH] = self._fileAPP[self._PATH] + self._fileCONF[self._PATH] + self._PS
        self._checkCONF()
        self.setDB(self.getOpt('LastDB'), check=True)

    def setIMP(self, path: str):
        self._fileIMP = self._split_path(path)

    def getIMP(self, path=False, file=False, ext=False):
        if not ext and not self._fileIMP[0]:
            return ''
        fp = ''
        if path:
            fp += self._fileIMP[self._PATH]
        if file:
            fp += self._fileIMP[self._NAME] + self._fileIMP[self._EXT]
        if ext:  # 'text (*.txt)'
            fp += self.typeIMP[0] + ' (' + self.typeIMP[1] + ')'
        if not path and not file and not ext:  # all: path+name+ext
            fp = self._fileIMP[self._PATH] + self._fileIMP[self._NAME] + self._fileIMP[self._EXT]
        return fp

    def setDB(self, path: str, check=False):
        """set path and filename for DB. \n
        checks if file exist if requested, useful for reading config (lastDB)
        """
        if check and not os.path.isfile(path):
            # file is missing
            self.writeOpt('LastDB', '')
            self._fileDB = ['', '', '']
            return
        # override file extension. No other than s3db can be opened
        self._fileDB = self._split_path(path)
        self._fileDB[self._EXT] = self.typeDB[1]

    def getDB(self, path=False, file=False, ext=False):
        """ext=True: input typical for QtFileDialog: ('SQlite3 (*.s3db)') \n
        all False: path+file+ext
        """
        if not ext and not self._fileDB[0]:
            return ''
        fp = ''
        if path:
            fp += self._fileDB[self._PATH]
        if file:
            fp += self._fileDB[self._NAME] + self._fileDB[self._EXT]
        if ext:  # 'SQlite (*.s3db)'
            fp += self.typeDB[0] + ' (*' + self.typeDB[1] + ')'
        if not path and not file and not ext:  # all: path+name+ext
            fp = self._fileDB[self._PATH] + self._fileDB[self._NAME] + self._fileDB[self._EXT]
        return fp

    def setAPP(self):
        if getattr(sys, 'frozen', False):  # exe file
            file = os.path.realpath(sys.executable)
        else:
            try:
                file = os.path.realpath(__file__)  # debug in IDE
            except NameError:
                file = os.getcwd()  # command line >python3 app.py
        self._fileAPP = self._split_path(file)

    def getCONF(self, path=False, file=False):
        """Returns file path (inculding filename) to config file.

        config file is used to store app configuration: /n
        - name of DB file when exited last time /n
        - welcome text/n
        If appropraite option is given,
        can return only path or only filename
        """
        fp = ''
        if path:
            fp += self._fileCONF[self._PATH]
        if file:
            fp += self._fileCONF[self._NAME] + self._fileCONF[self._EXT]
        if not path and not file:  # all: path+name+ext
            fp = self._fileCONF[self._PATH] + self._fileCONF[self._NAME] + self._fileCONF[self._EXT]
        return fp

    def writeOpt(self, op, val):
        """write new value for option
        allowed options: \n
        LastDB - last DB when app was closed \n
        welcome - welcome text, showed when no DB opened \n
        visColumns - list of names for visible columns\n
        """
        if op not in self.option:
            return 'nie ma takiej opcji'
        with open(self.getCONF(), 'r') as file:
            conf = json.load(file)
        conf[op] = val
        with open(self.getCONF(), 'w') as file:
            json.dump(conf, file)

    def getOpt(self, op):
        """Get value or option
        allowed options: \n
        LastDB - last DB when app was closed \n
        welcome - welcome text, showed when no DB opened \n
        visColumns - list of names for visible columns\n
        """
        if op in self.option:
            with open(self.getCONF(), 'r') as file:
                conf = json.load(file)
            return conf[op]
        else:
            return ""

    def _checkCONF(self):
        """Make sure the config file exists and has proper content.
        Removes wrong entries, add entries if missing
        """
        ref_conf = {}
        with open(self.getCONF(), 'a+') as file:  # will create file if not exist
            try:
                # on WIN 'r+' is not creating new file! why??
                # a+ is working fine, but set the cursor to eof
                # so need to move back to the begining
                file.seek(0)
                conf = json.load(file)
            except:
                conf = {'new conf tbc': ''}  # empty file
        for op in self.option:  # check here for known options
            if op not in conf:
                ref_conf[op] = self.option[op]
            else:
                ref_conf[op] = conf[op]
        if ref_conf != conf:
            self._repairCONF(ref_conf)

    def _repairCONF(self, conf: dict):
        """create new fresh conf file
        """
        with open(self.getCONF(), 'w') as file:
            json.dump(conf, file)

    def _split_path(self, file):
        """split the string by path separator. Last list item is name.
        What is left is the path. Than name is split by dot, giving extension
        """

        path = ''
        file = file.split(self._PS)  # path separator is system specific (/ for linux, \ for win)
        name = file.pop()
        path = self.list2str(file, self._PS)
        name = name.split('.')
        if len(name) > 1:
            ext = name.pop()
        else:
            ext = ''
        name = self.list2str(name, '.')
        # dot between name and extension move to extension
        name = name[0:-1]
        ext = '.' + ext
        return [path, name, ext]

    def list2str(self, li, sep=''):
        str = ''
        for i in li:
            str += i + sep
        return str


class Panply():
    def __init__(self, *args, **kwargs):
        self.db = pd.DataFrame(*args, **kwargs)
        self.cols = []
        self.val = 0
        self.oper = ''
        self.row = -1

    def __ne__(self, *args, **kwargs):
        self.oper = 'ne'
        self.filter()

    def __eq__(self, other):
        self.oper = 'eq'
        self.filter()

    def __iter__(self):
        self.row = -1
        return self

    def __next__(self):
        self.row += 1
        if self.row >= len(self.db) or self.db.empty:
            raise StopIteration
        return self.db.iloc[self.row,:]
    
    def __len__(self):
        return len(self.db)

    def __getitem__(self, *args):
        if len(args) == 1:
            return self.db.iloc[args[0]]
        return self.db.iloc[args[0], args[1]]

    def c(self,*args):
        self.cols = args[0]
        return self
    
    def v(self, *args):
        self.val = args[0]
        return self

    def filter(self, *args, **kwargs):
        if not self.cols or not self.val:
            return
        def eq():
            rows = self.db.loc[:, self.cols] == self.val
            self.db = self.db.loc[rows,:]
            return
        def ne():
            rows = self.db.loc[:, self.cols] != self.val
            self.db = self.db.loc[rows,:]
            return
        exec(f'{self.oper}()')
    




