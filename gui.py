from qt_gui.main_window import Ui_banking, QtCore, QtGui, QtWidgets
from db import DB
from modules import FileSystem
import opt.parse_cfg as cfg



class GUIMainWin(QtWidgets.QMainWindow, Ui_banking):
    def __init__(self):
        super().__init__()
        self.setupUi(self)



class GUIMainWin_ctrl(QtCore.QObject):
    def __init__(self, view):
        super().__init__(view)
        self.view = view
        self.db = DB()
        self.fs = FileSystem()
        #connect signals
        self.connect_signals()
        #hide import widgets
        self.view.import_info.hide()
        self.view.import_status_btn.hide()
        # enable sorting
        self.view.DB_cat_view.setSortingEnabled(True)
        self.view.DB_trans_view.setSortingEnabled(True)

    def connect_signals(self):
        # file management
        self.view.openDB_btn.clicked.connect(self.openDB)
        self.view.newDB_btn.clicked.connect(self.newDB)
        self.view.save_asDB_btn.clicked.connect(self.save_asDB)
        self.view.imp_btn.clicked.connect(self.imp)
        self.view.export_btn.clicked.connect(self.exp)
        
        self.view.addFltr_btn.clicked.connect(self.addFltr)
        self.view.rmFltr_btn.clicked.connect(self.rmFltr)
        self.view.also_not_cat.toggled.connect(lambda: self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view))
        self.view.new_cat_name.editingFinished.connect(self.new_cat)
        # QTreeWidget
        self.view.tree_db.clicked.connect(self.con_tree)
        # DB_cat_view context menu
        self.view.DB_cat_view.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.addFltrQA = QtWidgets.QAction("add filter", self)
        self.view.DB_cat_view.addAction(self.addFltrQA)
        self.addFltrQA.triggered.connect(self.addFltr_fromDB)
        # QTreeWidget context menu
        self.view.tree_db.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)

        self.renCatQA = QtWidgets.QAction("rename category", self)
        self.movCatQA = QtWidgets.QAction("move category", self)
        self.remCatQA = QtWidgets.QAction("remove category", self)
        
        self.view.tree_db.addAction(self.renCatQA)
        self.view.tree_db.addAction(self.movCatQA)
        self.view.tree_db.addAction(self.remCatQA)
        
        self.renCatQA.triggered.connect(self.renCat)
        self.movCatQA.triggered.connect(self.movCat)
        self.remCatQA.triggered.connect(self.remCat)

    def renCat(self):
        pass

    def movCat(self):
        pass

    def remCat(self):
        pass

    # fill tables
    def __tabViewItems__(self, tabName, col_n):
        """gets table name and col id, call appropriate function and return QtWidget\n
        table names: ['op', 'cat', 'trans']
        """
        def trans0():
            # QtWidget:combo, col_name:BANK
            colWidget = QtWidgets.QComboBox()
            it = list(cfg.bank)
            it.append('all')
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget
        
        def trans1():
            # QtWidget:combo, col_name:COL_NAME
            colWidget = QtWidgets.QComboBox()
            it = cfg.cat_col_names
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def trans2():
            # QtWidget:combo, col_name:OPER
            colWidget = QtWidgets.QComboBox()
            it = self.db.trans_col()
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def trans3():
            # QtWidget:lineEdit, col_name:val1
            colWidget = QtWidgets.QLineEdit()
            return colWidget
        
        def trans4():
            # QtWidget:lineEdit, col_name:val2
            return trans3()
        
        def cat0():
            # QtWidget:combo, col_name:col_name
            colWidget = QtWidgets.QComboBox()
            it = cfg.cat_col_names
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget
        def cat1():
            # QtWidget:lineEdit, col_name:filter
            colWidget = QtWidgets.QLineEdit()
            return colWidget
        def cat2():
            # filter_n
            pass
        def cat3():
            # QtWidget:combo, col_name:oper
            colWidget = QtWidgets.QComboBox()
            it = self.db.filter_data()
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget
        def cat4():
            # oper_n
            pass
        def cat5():
            # QtWidget:lineEdit, col_name:category
            colWidget = QtWidgets.QLineEdit()
            return colWidget
        def cat6():
            pass

        # common stuff
        colWidget = eval(tabName + str(col_n) + '()')
        if colWidget:
            colWidget.setStyleSheet("background-color: rgb(26, 255, 14);")

        return colWidget

    def fill_trans(self):
        cols = self.db.trans.columns
        rows_n = len(self.db.trans)
        self.view.trans_view.setColumnCount(len(cols))
        self.view.trans_view.setRowCount(0) # reset table
        self.view.trans_view.setRowCount(rows_n + 1)
        # set column labels
        self.view.trans_view.setHorizontalHeaderLabels(cols)
        col = self.view.trans_view.horizontalHeader()
        col.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        # populate table
        for x in range(len(cols)):
            # first row
            self.view.trans_view.setCellWidget(0, x, self.__tabViewItems__('trans', x))
            # and the other
            for y in range(rows_n):
                cell = QtWidgets.QTableWidgetItem(str(self.db.trans.iloc[y, x]))
                self.view.trans_view.setItem(y + 1, x, cell)
    
    def fill_DB(self, db, widget):
        """ fills tableView without Widgets in first row\n
        applicable to op or op_sub DB\n
        check status of also_not_cat
        """
        widget.setSortingEnabled(False) # otherway we end up with mess
        # append not categorized data if required
        # but only for DB_cat_view widget
        if self.view.also_not_cat.isChecked() and widget.objectName() == 'DB_cat_view':
            not_cat = True
        else:
            not_cat = False

        cols = cfg.cat_col_names
        row_n = len(db)
        widget.setColumnCount(len(cols))
        widget.setRowCount(0) # reset table
        widget.setRowCount(row_n)
        # set column labels
        widget.setHorizontalHeaderLabels(cols)
        col = widget.horizontalHeader()
        col.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        # populate table
        for x in cols:
            for y in range(row_n):
                cell = QtWidgets.QTableWidgetItem(str(db.loc[y,x]))
                if not_cat:
                    cell.setBackground(QtGui.QBrush(QtGui.QColor(0, 170, 255)))
                widget.setItem(y, cols.index(x), cell)
        # add not categorized if requested
        if not_cat:
            db = self.db.getOP(not_cat = True)
            row_n2 = len(db)
            widget.setRowCount(row_n + row_n2)
            # populate table
            for x in cols:
                for y in range(row_n, row_n2 + row_n):
                    cell = QtWidgets.QTableWidgetItem(str(db.loc[y - row_n,x]))
                    widget.setItem(y, cols.index(x), cell)
        widget.setSortingEnabled(True)

    def fill_cat(self):
        cols_all = list(self.db.cat.columns)
        cols = [cols_all[i] for i in [0,1,3]] # limit cols to interesting only
        rows_n = len(self.db.cat_temp)
        self.view.cat_view.setColumnCount(len(cols))
        self.view.cat_view.setRowCount(0) # reset table
        self.view.cat_view.setRowCount(rows_n + 1)
        # set column labels
        self.view.cat_view.setHorizontalHeaderLabels(cols)
        col = self.view.cat_view.horizontalHeader()
        col.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        # populate table
        for x in cols:
            # first row
            x_n = cols.index(x)
            self.view.cat_view.setCellWidget(0, x_n, self.__tabViewItems__('cat', cols_all.index(x)))
            # and the other
            for y in range(rows_n):
                cell = QtWidgets.QTableWidgetItem(str(self.db.cat_temp.loc[y, x]))
                self.view.cat_view.setItem(rows_n - y, x_n, cell)

    def con_tree(self):
        #reset temp DB
        self.db.reset_temp_DB()

        # limit views to selected category
        it = self.view.tree_db.selectedItems()
        cat = it[0].text(0)
        self.db.filter_data(self.db.CATEGORY, cat, 'new')
        self.fill_cat()
        self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)

    def fill_tree(self):
        # reset the tree
        self.view.tree_db.clear()
        # name columns
        self.view.tree_db.setHeaderLabels([cfg.extra_col[2],'n'])
        # list of categories in db
        cats = self.db.show_tree()
        # first row is Grandpa
        grandpa = QtWidgets.QTreeWidgetItem(self.view.tree_db, [cats[0][1], str(cats[0][2])])
        self.view.tree_db.addTopLevelItem(grandpa)
        # than other
        match = QtCore.Qt.MatchExactly | QtCore.Qt.MatchRecursive
        for cat in cats[1:]:
            parent = self.view.tree_db.findItems(cat[0], match)
            QtWidgets.QTreeWidgetItem(parent[0], [cat[1], str(cat[2])])
        # expand and resize
        self.view.tree_db.expandAll()
        self.view.tree_db.resizeColumnToContents(0)
        # select GRANDPA
        self.view.tree_db.setCurrentItem(grandpa)
        self.con_tree() # and adjust tables

    #categorizing
    def setCatInput(self):
        # fill new category combo box
        cats = self.db.show_tree()
        cats = [i[1] for i in cats[1:]]
        completer = QtWidgets.QCompleter(cats)
        completer.setCaseSensitivity(False)
        self.view.new_cat_name.setCompleter(completer)

    def new_cat(self):
        # called when signal emitted editingFinished
        #DEBUG
        print('editingFinished')
        cat = self.view.new_cat_name.text()
        if not cat:
            #DEBUG
            print('editingFinished break')
            return
        self.view.new_cat_name.setText('')
        self.db.filter_commit(cat)

        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.fill_tree() # will set top item and filter tables accordingly
        self.setCatInput()

    def addFltr_fromDB(self):
        fltr = {}
        it = self.view.DB_cat_view.currentItem()
        col_i = self.view.DB_cat_view.column(it)
        fltr[self.db.COL_NAME] = self.view.DB_cat_view.horizontalHeaderItem(col_i).text()
        fltr[self.db.FILTER] = it.text()
        for col_i in range(self.view.cat_view.columnCount()):
            widget_name = self.view.cat_view.horizontalHeaderItem(col_i).text()
            if widget_name in fltr.keys():
                widget = self.view.cat_view.cellWidget(0,col_i)
                if isinstance(widget, QtWidgets.QComboBox):
                    widget.setCurrentText(fltr[widget_name])
                else:
                    widget.setText(fltr[widget_name])

    def addFltr(self):
        fltr = self.__collectWidgets__()
        if all(fltr.values()):
            self.db.filter_data(col=fltr[self.db.COL_NAME],
                                cat_filter=fltr[self.db.FILTER],
                                oper=fltr[self.db.OPER])
            self.fill_cat()
            self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)

    def rmFltr(self):
        row_count = self.view.cat_view.rowCount() - 1
        it = self.view.cat_view.currentItem()
        row_i = row_count - self.view.cat_view.row(it)
        self.db.cat_temp_rm(oper_n = row_i)
        self.fill_cat()
        self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)

        pass

    def __collectWidgets__(self):
        """collects text from first row widgets in table
        """
        fltr = {}
        for col_i in range(self.view.cat_view.columnCount()):
            widget = self.view.cat_view.cellWidget(0,col_i)
            if isinstance(widget, QtWidgets.QComboBox):
                widget_txt = widget.currentText()
            else:
                widget_txt = widget.text()
            widget_name = self.view.cat_view.horizontalHeaderItem(col_i).text()
            fltr[widget_name] = widget_txt
        return fltr

    # file operations
    def openDB(self):
        file = QtWidgets.QFileDialog.getOpenFileName(self.view, caption='Choose SQlite3 file',
                                                                directory='',
                                                                filter=self.fs.getDB(ext=True))
        # Qt lib returning always / as path separator
        # we need system specific, couse we are checking for file existence
        path = QtCore.QDir.toNativeSeparators(file[0])
        if path:
            self.fs.setDB(path)
        else:  # operation canceled
            return
        if self.db.open_db(self.fs.getDB()) is not None:  # return None if fail
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
            #self.disp_statusbar('openDB')
            self.fs.writeOpt("LastDB", self.fs.getDB())
            self.fill_trans()
            self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
            self.fill_tree() # will set top item and filter tables accordingly
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
    
    def newDB(self):
        pass

    def save_asDB(self):
        pass

    def imp(self):
        pass

    def exp(self):
        pass