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
        self._fileLOG = ['opt', 'log', '.txt']
        self.option = {"LastDB": '',
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
        self.setDB(self.getOpt('LastDB'))

    def connect(self, parent: object):
        self.printMsg = parent

    def setIMP(self, path: str):
        """set path and filename for importing excel with operations. \n
        """
        self._fileIMP = self.__splitPath__(path)
        if not self.__checkFile__(self.getIMP()):
            self._fileIMP = ['', '', '']

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

    def setIMPDB(self, path: str):
        """set path and filename for DB for importing cats and trans. \n
        """
        self._fileIMPDB = self.__splitPath__(path)
        # override file extension. No other than s3db can be opened
        self._fileIMPDB[self._EXT] = self.typeDB[1]
        if not self.__checkFile__(self.getIMPDB()):  # missing file
            self._fileIMPDB = ['', '', '']
            return

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

    def setDB(self, path: str):
        """set path and filename for DB. \n
        checks if file exist if requested, useful for reading config (lastDB)
        """
        self._fileDB = self.__splitPath__(path)
        # override file extension. No other than s3db can be opened
        self._fileDB[self._EXT] = self.typeDB[1]
        if not self.__checkFile__(self.getDB()):  # missing file?
            # try to restore from options
            if not self.__checkFile__(self.getOpt("LastDB")):
                self._fileDB = ['', '', '']
            else:
                self._fileDB = self.getOpt("LastDB")
        self.writeOpt("LastDB", self.getDB())

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

    def setCSV(self, path: str):
        """set path and filename for export to csv file. \n
        """
        # override file extension. No other than s3db can be opened
        self._fileCSV = self.__splitPath__(path)
        self._fileCSV[self._EXT] = self.typeCSV[1]
        if not self.__checkFile__(self.getCSV()):
            self._fileCSV = ['', '', '']

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

    def setAPP(self):
        if getattr(sys, 'frozen', False):  # exe file
            file = os.path.realpath(sys.executable)
        else:
            try:
                file = os.path.realpath(__file__)  # debug in IDE
            except NameError:
                file = os.getcwd()  # command line >python3 app.py
        self._fileAPP = self.__splitPath__(file)

    def getCONF(self, path=False, file=False):
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

    def getLOG(self, path=False, file=False):
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

    def writeMsg(self, msg: str):
        """
        add message to end of log file, also add date
        If connected to output object, call it
        """
        now = dt.datetime.now()
        with open(self.getLOG(), 'a+') as file:
            file.write(msg + ',' + now.strftime('%d %b %Y %H:%M') + '\n')
        self.printMsg(msg)

    def getMsg(self, all=False):
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

    def __checkFile__(self, file: str) -> bool:
        """Check if file exists\n
        write message if not
        """
        if not os.path.isfile(file):
            # file is missing
            if file:
                self.printMsg(f"file '{file}' dosen't exists")
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

    def list2str(self, li, sep=''):
        str = ''
        for i in li:
            str += i + sep
        return str
