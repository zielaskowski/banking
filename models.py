"""
data models extending PyQT models
"""
from PyQt5 import QtCore
from PyQt5 import QtGui
import re

class DBmodel(QtCore.QAbstractTableModel):
    def __init__(self, db):
        super().__init__()
        self.db = db
        self.columns = self.db.columns.to_list()
        self.columns.remove('index')
        self.backgroundColor = [False] * self.rowCount(None)
        self.fltr = ''
        self.fltrCol = ''
        self.markRows = []

    def markBlueAddRows(self, db):
        # append db, mark blue background color what already in model
        # id db=None, remove coloring and added data
        self.layoutAboutToBeChanged.emit()
        if db is not None:
            self.backgroundColor = [True] * self.rowCount(None)
            self.db = self.db.append(db, ignore_index=True)
            self.backgroundColor.extend([False] * len(db))
        else:
            if any(self.backgroundColor):
                self.db = self.db.loc[self.backgroundColor, :].copy()
                self.backgroundColor = [False] * self.rowCount(None)
        self.layoutChanged.emit()

    def markFltr(self, fltr='', fltrCol=''):
        if fltr != self.fltr:
            self.layoutAboutToBeChanged.emit()
            self.fltr = fltr
            self.fltrCol = fltrCol
            self.layoutChanged.emit()

    def findRows(self, search):
        self.layoutAboutToBeChanged.emit()
        self.markRows = []
        if search:
            flags = QtCore.Qt.MatchContains | QtCore.Qt.MatchWrap
            for i in range(len(self.columns)):
                ind = self.match(self.createIndex(0, i),
                                 QtCore.Qt.DisplayRole,
                                 search,
                                 hits=-1,
                                 flags=flags)
                [self.markRows.append(i.row()) for i in ind]
        self.layoutChanged.emit()
        return len(self.markRows)

    def rowCount(self, parent):
        # required by QAbstractTableModel
        return len(self.db)

    def columnCount(self, parent):
        # required by QAbstractTableModel
        return len(self.columns)

    def data(self, index, role):
        # required by QAbstractTableModel
        nas = ['None', '<NA>', 'NaT', 'nan']
        # can display only strings??, so convert numbers to string
        txt = self.db.loc[index.row(), self.columns[index.column()]]
        if type(txt).__name__ == 'Timestamp':
            txt = txt.date()
        txt = str(txt)

        # set text
        if role == QtCore.Qt.DisplayRole:
            if txt in nas:
                txt = ''
            return txt

        # set background collors
        # green for search
        # blue when not categorized data requested
        if role == QtCore.Qt.BackgroundRole:
            color = None
            # blue
            if self.backgroundColor[index.row()]:
                color = QtGui.QBrush(QtGui.QColor(0, 170, 255))
            # green search
            if index.row() in self.markRows:
                color = QtGui.QBrush(QtGui.QColor(26, 255, 14))
            # orange filter
            if self.fltr and self.columns[index.column()] == self.fltrCol:
                if re.findall(self.fltr, txt, re.IGNORECASE):
                    color = QtGui.QBrush(QtGui.QColor(255, 226, 79))
            return color

        # set tool tip
        if role == QtCore.Qt.ToolTipRole:
            if not (txt in nas):
                return txt

    def headerData(self, section, orientation, role):
        # required by QAbstractTableModel
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return self.columns[section]


class DBmodelProxy(QtCore.QSortFilterProxyModel):
    # extends model by sorting and filtering
    def __init__(self, db):
        super().__init__()
        self.setSourceModel(DBmodel(db))

    def lessThan(self, left, right):
        # sorting for float
        # make sure empty rows stay at end when sorted (in any order)
        # make sure collored cells will stay on topwhen sorted (in any order)

        lDat = self.sourceModel().data(left, QtCore.Qt.DisplayRole)
        rDat = self.sourceModel().data(right, QtCore.Qt.DisplayRole)
        lColor = self.sourceModel().data(left, QtCore.Qt.BackgroundRole)
        rColor = self.sourceModel().data(right, QtCore.Qt.BackgroundRole)
        # trying to convert
        try:
            lDat = float(lDat)
            rDat = float(rDat)
        except:
            pass
        # both  must be the same type to compare
        if type(lDat) != type(rDat):
            lDat = str(lDat)
            rDat = str(rDat)
        # keep colored cells at begining
        if self.sortOrder() == QtCore.Qt.AscendingOrder:
            if lColor:
                return True
            if rColor:
                return False
        if self.sortOrder() == QtCore.Qt.DescendingOrder:
            if lColor:
                return False
            if rColor:
                return True
        # keep empty cells at end whatever sort order is
        if self.sortOrder() == QtCore.Qt.AscendingOrder:
            if lDat == '' or rDat == '':
                return not lDat < rDat
        return lDat < rDat
