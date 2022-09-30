"""
various modules
class FileSystem manage writing, savin, import and export of data
"""
import os
import sys
import json
import datetime as dt


class FileSystem:
    """Handles all file system stuff

    Define file path and names. For each type stores as list of path, name and extensions:\n
    -imported XLS file:          self._fileIMP, access through self.getIMP and self.setIMP\n
    -DB file:                    self._fileDB, access through self.getDB and self.setDB\n
    -aplication location:        self._fileAPP, access through self.getAPP\n
    -configuration file:         self._fileCONF, access to options through self.getOpt and self.writeOpt\n
    -csv export file:            self._fileCSV, access through self.getCSV and self.setCSV\n
    -log file:                   self._fileLOG, access through self.getLog(last|all) and self.writeLog\n

    Handles expected type of file: SQlite3, TXT, XLS used as parameter for QtFileDialog\n

    attach msg output with self.connect() to print new message when arrive

    """

    _PS = os.path.sep  # / for linux; \\ for win
    _PATH = 0  # location of path in self._fileXXX list
    _NAME = 1  # location of name in self._fileXXX list
    _EXT = 2  # location of extension in self._fileXXX list

    def __init__(self):
        self.printMsg = print
        self._fileIMP = ['', '', '']
        self._fileCSV = ['', '', '']
        self._fileDB = ['', '', '']
        self._fileIMPDB = ['', '', '']
        self._fileAPP = ['', '', '']
        # Configuration file. name is constant
        self._fileCONF = ['opt', 'conf', '.txt']
        # Log file. name is constant. csv format
        self._fileLOG = ['opt', 'log', '.log']
        self.option = {"LastDB": '',
                       "LastIMP": '',
                       "LastIMPDB": '',
                       "LastCSV": '',
                       "welcome": "Write welcome message into ./opt/conf.txt...",
                       "visColumns": [],
                       "winSize": ''}
        self.typeIMP = ['excel', '*.xls *.xlsx']
        self.typeDB = ['SQlite3', '.s3db']
        self.typeCSV = ['CSV file', '.csv']

        self.setAPP()

        self._fileCONF[self._PATH] = self._fileAPP[self._PATH] + \
            self._fileCONF[self._PATH] + self._PS
        self._fileLOG[self._PATH] = self._fileAPP[self._PATH] + \
            self._fileLOG[self._PATH] + self._PS
        self.__checkCONF__()
        self.__checkLOG__()

    def connect(self, parent: object):
        self.printMsg = parent

    def setIMP(self, path: str) -> str:
        """set path and filename for importing excel with operations. \n
        """
        return self.__set__(path,
                            optName="LastIMP",
                            _Ffile=self._fileIMP,
                            _Ftype=None,
                            getFn=self.getIMP)

    def getIMP(self, path=False, file=False, ext=False):
        """ext=True: input typical for QtFileDialog: ('excel (*.xls)') \n
        all False: path+file+ext
        """
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
            fp = self._fileIMP[self._PATH] + \
                self._fileIMP[self._NAME] + self._fileIMP[self._EXT]
        return fp

    def setIMPDB(self, path: str) -> str:
        """set path and filename for DB for importing cats and trans. \n
        """
        return self.__set__(path,
                            optName="LastIMPDB",
                            _Ffile=self._fileIMPDB,
                            _Ftype=self.typeDB,
                            getFn=self.getIMPDB)

    def getIMPDB(self, path=False, file=False, ext=False):
        """ext=True: input typical for QtFileDialog: ('SQlite3 (*.s3db)') \n
        all False: path+file+ext
        """
        if not ext and not self._fileIMPDB[0]:
            return ''
        fp = ''
        if path:
            fp += self._fileIMPDB[self._PATH]
        if file:
            fp += self._fileIMPDB[self._NAME] + self._fileIMPDB[self._EXT]
        if ext:  # 'SQlite (*.s3db)'
            fp += self.typeDB[0] + ' (*' + self.typeDB[1] + ')'
        if not path and not file and not ext:  # all: path+name+ext
            fp = self._fileIMPDB[self._PATH] + \
                self._fileIMPDB[self._NAME] + self._fileIMPDB[self._EXT]
        return fp

    def setDB(self, path='') -> str:
        """set path and filename for DB. \n
        takes lastDB from options if path is missing
        return file path with name (or '' if file is missing)
        """
        return self.__set__(path,
                            optName="LastDB",
                            _Ffile=self._fileDB,
                            _Ftype=self.typeDB,
                            getFn=self.getDB,
                            dirOnly=True)

    def getDB(self, path=False, file=False, ext=False) -> str:
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
            fp = self._fileDB[self._PATH] + \
                self._fileDB[self._NAME] + self._fileDB[self._EXT]
        return fp

    def setCSV(self, path: str) -> str:
        """set path and filename for export to csv file. \n
        """
        return self.__set__(path,
                            optName="LastCSV",
                            _Ffile=self._fileCSV,
                            _Ftype=self.typeCSV,
                            getFn=self.getCSV,
                            dirOnly=True)

    def __set__(self, path, optName: str, _Ffile: list, _Ftype: list, getFn: object, dirOnly=False):
        """set path and filename for 
        """
        if not path:
            path = self.getOpt(optName)
            # file may disapear in meantime
            if not self.__checkFile__(path):
                self.writeOpt(op=optName, val='')
                _Ffile[0:2] = ['', '', '']
                return ''
                
        if not self.__checkFile__(path, dirOnly=dirOnly):
            _Ffile[0:2] = ['', '', '']
            return ''

        _Ffile[0:2] = self.__splitPath__(path)
        # override file extension
        if _Ftype:
            _Ffile[self._EXT] = _Ftype[1]

        self.writeOpt(optName, getFn())
        return getFn()

    def getCSV(self, path=False, file=False, ext=False):
        """ext=True: input typical for QtFileDialog: ('CSV file (*.csv)') \n
        all False: path+file+ext
        """
        if not ext and not self._fileCSV[0]:
            return ''
        fp = ''
        if path:
            fp += self._fileCSV[self._PATH]
        if file:
            fp += self._fileCSV[self._NAME] + self._fileCSV[self._EXT]
        if ext:  # 'CSV file (*.csv)'
            fp += self.typeCSV[0] + ' (*' + self.typeCSV[1] + ')'
        if not path and not file and not ext:  # all: path+name+ext
            fp = self._fileCSV[self._PATH] + \
                self._fileCSV[self._NAME] + self._fileCSV[self._EXT]
        return fp

    def setAPP(self) -> str:
        if getattr(sys, 'frozen', False):  # exe file
            file = os.path.realpath(sys.executable)
        else:
            try:
                file = os.path.realpath(__file__)  # debug in IDE
            except NameError:
                file = os.getcwd()  # command line >python3 app.py
        self._fileAPP = self.__splitPath__(file)

    def getCONF(self, path=False, file=False) -> str:
        """Returns file path (inculding filename) to config file.

        config file is used to store app configuration: /n
        - name of DB file when exited last time /n
        - welcome text/n
        If appropraite option is given,
        can return only path or only filename, or both if none given
        """
        fp = ''
        if path:
            fp += self._fileCONF[self._PATH]
        if file:
            fp += self._fileCONF[self._NAME] + self._fileCONF[self._EXT]
        if not path and not file:  # all: path+name+ext
            fp = self._fileCONF[self._PATH] + \
                self._fileCONF[self._NAME] + self._fileCONF[self._EXT]
        return fp

    def getLOG(self, path=False, file=False) -> str:
        """Return file path (including filename) to log file.
        If appropraite option is given,
        can return only path or only filename, or both if none given
        """
        fp = ''
        if path:
            fp += self._fileLOG[self._PATH]
        if file:
            fp += self._fileLOG[self._NAME] + self._fileLOG[self._EXT]
        if not path and not file:  # all: path+name+ext
            fp = self._fileLOG[self._PATH] + \
                self._fileLOG[self._NAME] + self._fileLOG[self._EXT]
        return fp

    def msg(self, msg: str):
        """
        add message to end of log file, also add date
        If connected to output object, call it
        """
        now = dt.datetime.now()
        with open(self.getLOG(), 'a+') as file:
            file.write(msg + ',' + now.strftime('%d %b %Y %H:%M') + '\n')
        self.printMsg(msg)

    def getMsg(self, all=False) -> str:
        """
        return log 
        return all with all=True
        or only last line with all=False
        """
        with open(self.getLOG(), 'r') as file:
            msgs = file.readlines()
        if all:
            return '\n'.join(msgs[:])
        else:
            return msgs[-1]

    def writeOpt(self, op, val):
        """write new value for option
        allowed options: \n
        allowed options: \n
        LastDB - last DB when app was closed \n
        LastIMP - last import file \n
        LastIMPDB - last imported DB with filters \n
        LastCSV - last CSV file exported \n
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

    def getOpt(self, op) -> str:
        """Get value or option
        allowed options: \n
        LastDB - last DB when app was closed \n
        LastIMP - last import file \n
        LastIMPDB - last imported DB with filters \n
        LastCSV - last CSV file exported \n
        welcome - welcome text, showed when no DB opened \n
        visColumns - list of names for visible columns\n
        """
        if op in self.option:
            with open(self.getCONF(), 'r') as file:
                conf = json.load(file)
            return conf[op]
        else:
            return ""

    def __checkLOG__(self):
        """Will create a file if not exists
        Delete content
        """
        with open(self.getLOG(), 'w+') as file:
            file.write('')

    def __checkCONF__(self):
        """Make sure the config file exists and has proper content.
        Removes wrong entries, add entries if missing
        """
        ref_conf = {}
        conf = self.__readFile__(self.getCONF())
        for op in self.option:  # check here for known options
            if op not in conf:
                ref_conf[op] = self.option[op]
            else:
                ref_conf[op] = conf[op]
        if ref_conf != conf:
            self.__repairCONF__(ref_conf)

    def __readFile__(self, fileName) -> dict:
        with open(fileName, 'a+') as file:  # will create file if not exist
            try:
                # on WIN 'r+' is not creating new file! why??
                # a+ is working fine, but set the cursor to eof
                # so need to move back to the begining
                file.seek(0)
                conf = json.load(file)
            except:
                conf = {}  # empty file
        return conf

    def __repairCONF__(self, conf: dict):
        """create new fresh conf file
        """
        with open(self.getCONF(), 'w') as file:
            json.dump(conf, file)

    def __checkFile__(self, file: str, dirOnly=False) -> bool:
        """Check if file exists\n
        write message if not
        """
        if dirOnly:
            if not os.path.exists(self.__splitPath__(file)[self._PATH]):
                self.msg(f"path '{file}' dosent't exists")
                return False
        else:
            if not os.path.isfile(file):
                # file is missing
                if file:
                    self.msg(f"file '{file}' dosen't exists")
                return False
        return True

    def __splitPath__(self, file):
        """split the string by path separator. Last list item is name.\n
        What is left is the path. Than name is split by dot, giving extension\n
        If path start with './': add py module path
        """
        path = ''
        # when file starts with dot, add module path
        if file[0:2] == './':
            file = self._fileAPP[self._PATH] + file[2:]
        # path separator is system specific (/ for linux, \ for win)
        file = file.split(self._PS)
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

    def list2str(self, li: list, sep='') -> str:
        str = ''
        for i in li:
            str += i + sep
        return str
