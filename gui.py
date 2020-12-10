import re
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
        # controling context menu
        self.before_edit = ''
        self.contextFunc = ''

    def connect_signals(self):
        # file management
        self.view.openDB_btn.clicked.connect(self.openDB)
        self.view.newDB_btn.clicked.connect(self.newDB)
        self.view.save_asDB_btn.clicked.connect(self.save_asDB)
        self.view.impPKO_btn.clicked.connect(lambda: self.imp(bank='ipko'))
        self.view.impBNP_btn.clicked.connect(lambda: self.imp(bank='bnp'))
        self.view.impBNPkredyt_btn.clicked.connect(lambda: self.imp(bank='bnp_kredyt'))
        self.view.export_btn.clicked.connect(self.exp)
        
        self.view.addFltr_btn.clicked.connect(self.addFltr)
        self.view.rmFltr_btn.clicked.connect(self.rmFltr)
        self.view.also_not_cat.toggled.connect(lambda: self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view))
        self.view.new_cat_name.editingFinished.connect(self.new_cat)
        # QTreeWidget
        self.view.tree_db.clicked.connect(self.con_tree)
        self.view.tree_db.itemChanged.connect(self.tree_edited)
        self.view.tree_db.itemDoubleClicked.connect(self.treeItemActivated)
        # group_view widgets triggers are defined when created, in self.__tabViewItems__()
        # DB_cat_view context menu
        self.view.DB_cat_view.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.addFltrQA = QtWidgets.QAction("add filter", self)
        self.view.DB_cat_view.addAction(self.addFltrQA)
        self.addFltrQA.triggered.connect(self.addFltr_fromDB)
        # DB_cat_view & DB_trans_view header context menu
        head = self.view.DB_cat_view.horizontalHeader()
        head.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        head.customContextMenuRequested.connect(self.DB_headerContextMenu_cat)
        head = self.view.DB_trans_view.horizontalHeader()
        head.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        head.customContextMenuRequested.connect(self.DB_headerContextMenu_trans)
        # QTreeWidget context menu
        self.view.tree_db.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.tree_db.customContextMenuRequested.connect(self.TreeContextMenu)

    def DB_headerContextMenu_cat(self, position):
        self.DB_headerContextMenu(position=position, source=self.view.DB_cat_view,)

    def DB_headerContextMenu_trans(self, position):
        self.DB_headerContextMenu(position=position, source=self.view.DB_trans_view,)

    def DB_headerContextMenu(self, source, position):
        """create context menu on DB_cat_view\n
        used to select visible columns
        """
        menu = QtWidgets.QMenu()
        for i in cfg.op_col:
            widget = QtWidgets.QRadioButton()
            widget.setText(i)
            widget.toggled.connect(lambda: self.col_vis(i))
            widgetA = QtWidgets.QWidgetAction(widget)
            if i in cfg.cat_col_names:
                widget.setDown(True)
            else:
                widget.setDown(False)
            menu.addAction(widgetA)
        act = menu.exec_(source.mapToGlobal(position))
        print(act)

    def col_vis(self, col):
        print(col)

    def TreeContextMenu(self, position):
        """create submenu on QTreeWidget\n
        add - will add taking selected as parent\n
        ren - will rename selected
        merge - active only when two or more selected, submenu to select new name
        move - submenu to select where to move
        remove - remove selected
        """
        it = self.view.tree_db.selectedItems()
        it_names = [i.text(0) for i in it]

        cat = self.db.show_tree()
        cat = [i[1] for i in cat]

        menu = QtWidgets.QMenu()
        add = menu.addAction("add actegory")
        ren = menu.addAction("rename category")
        mer = menu.addMenu("merge category")
        for i in it_names:
            mer.addAction(i)
        mov = menu.addMenu("move category")
        for i in cat:
            mov.addAction(i)
        rem = menu.addAction("remove category")
        
 
        if len(it) != 1:
            add.setEnabled(False)
            ren.setEnabled(False)
        if len(it) < 2:
            mer.setEnabled(False)
        if cfg.GRANDPA in it_names:
            ren.setEnabled(False)
            mov.setEnabled(False)
            rem.setEnabled(False)

        act = menu.exec_(self.view.tree_db.mapToGlobal(position))
        # adding empty category
        if act == add:
            self.contextFunc = 'add'
            self.view.tree_db.blockSignals(True)
            #create kid under selected item
            it_kid = QtWidgets.QTreeWidgetItem(it[0], ['', ''])
            flag = QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled
            it_kid.setFlags(flag)
            #clear all selection
            self.view.tree_db.selectionModel().clear()
            # select only kid
            it_kid.setSelected(True)
            self.view.tree_db.blockSignals(False)
            #edit name
            self.view.tree_db.editItem(it_kid)
        # rename existing category
        elif act == ren:
            self.before_edit = it_names[0]
            self.contextFunc = 'ren'
            self.view.tree_db.editItem(it[0])
        # remove category
        elif act == rem:
            for i in it_names:
                self.db.filter_data(col=self.db.CATEGORY,
                                        cat_filter=i,
                                        oper='add')
            self.db.filter_commit(cfg.GRANDPA)
            self.fill_tree()
        # merge selected categories
        elif act:
            if act.text() in it_names: # merge
                for i in it_names:
                    self.db.filter_data(col=self.db.CATEGORY,
                                        cat_filter=i,
                                        oper='add')
                self.db.filter_commit(act.text())
                self.fill_tree()
            # move selected categories
            if act.text() in cat: # move
                for i in it_names:
                    self.db.filter_data(col=self.db.CATEGORY,
                                        cat_filter=i,
                                        oper='add')
                    self.db.filter_commit(act.text())
                self.fill_tree()

    def treeItemActivated(self, it):
        """called by double clicked, when editing tree item\n
        equivalent to context menu: rename (will be catched after editing)\n
        """
        self.before_edit = it.text(0)
        self.contextFunc = 'ren'

    def tree_edited(self):
        """called when tree edited
        - can be from rename context menu
        - can be from add context menu
        - can be edited "by hand", equal to ren
        when redrawing the tree also can be triggered, so importent to block signals
        """
        if self.contextFunc == 'ren':
            it = self.view.tree_db.selectedItems()
            new_cat = it[0].text(0)
            parent = it[0].parent().text(0)
            cat = self.before_edit
            # move category with new name to grandpa
            self.db.filter_data(col=self.db.CATEGORY,
                            cat_filter=cat,
                            oper='new')
            self.db.filter_commit(new_cat)
            # move to parent if necessery
            if parent != cfg.GRANDPA:
                self.db.filter_data(col=self.db.CATEGORY,
                                    cat_filter=new_cat,
                                    oper='new')
                self.db.filter_commit(parent)

        elif self.contextFunc == 'add':
            it = self.view.tree_db.selectedItems()
            new_cat = it[0].text(0)
            parent = it[0].parent().text(0)
            #create empty category under parent
            self.db.reset_temp_DB()
            self.db.filter_commit(new_cat, parent)
        
        self.before_edit = ''
        self.contextFunc = ''
        self.fill_tree()

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
        def DB0():
            data = self.db.group_data(cfg.cat_col_names[col_n])
            colWidget = QtWidgets.QComboBox()
            colWidget.insertItems(-1,data)
            colWidget.setEditable(False)
            colWidget.textActivated.connect(self.addFltr_fromGR)
            return colWidget
        def DB1():
            return DB0()
        def DB2():
            return DB0()
        def DB3():
            return DB0()
        def DB4():
            return DB0()
        def DB5():
            return DB0()
        def DB6():
            return DB0()
        def DB7():
            return DB0()
        def DB8():
            return DB0()
        def DB9():
            return DB0()
        def DB10():
            return DB0()
        # common stuff
        colWidget = eval(tabName + str(col_n) + '()')
        if colWidget:
            colWidget.setStyleSheet("background-color: rgb(26, 255, 14); "
                                    "color: rgb(0,0,0); "
                                    "selection-background-color: rgb(255, 255, 255); "
                                    "selection-color: rgb(0, 0, 0);")

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
    
    def fill_group(self):
        """fills group_view
        """
        self.view.group_view.setSortingEnabled(False) # otherway we end up with mess
        cols = cfg.cat_col_names
        row_n = 1
        self.view.group_view.setColumnCount(len(cols))
        self.view.group_view.setRowCount(0) # reset table
        self.view.group_view.setRowCount(row_n)
        # hide column and row header
        col = self.view.group_view.horizontalHeader()
        col.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        col.hide()
        row = self.view.group_view.verticalHeader()
        row.hide()
        # populate table
        for x in cols:
            x_n = cols.index(x)
            self.view.group_view.setCellWidget(0, x_n, self.__tabViewItems__('DB', x_n))
        self.view.group_view.setSortingEnabled(True)

    def fill_DB(self, db, widget):
        """ fills tableView \n
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
            x_n = cols.index(x)
            for y in range(row_n):
                cell = QtWidgets.QTableWidgetItem(str(db.loc[y,x]))
                if not_cat:
                    cell.setBackground(QtGui.QBrush(QtGui.QColor(0, 170, 255)))
                widget.setItem(y, x_n, cell)
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
        """called when QTreeWidget selected item
        """
        #reset temp DB
        self.db.reset_temp_DB()

        # limit views to selected category
        it = self.view.tree_db.selectedItems()
        cat = it[0].text(0)
        self.db.filter_data(self.db.CATEGORY, cat, 'new')

        self.fill_cat()
        self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
        self.fill_group()

    def fill_tree(self):
        self.view.tree_db.blockSignals(True)
        # reset the tree
        self.view.tree_db.clear()
        # allow muliselection
        self.view.tree_db.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
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
            it = QtWidgets.QTreeWidgetItem(parent[0], [cat[1], str(cat[2])])
            flag = QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled
            it.setFlags(flag)
        # expand and resize
        self.view.tree_db.expandAll()
        self.view.tree_db.resizeColumnToContents(0)
        # select GRANDPA
        self.view.tree_db.setCurrentItem(grandpa)
        self.con_tree() # and adjust tables
        self.view.tree_db.blockSignals(False)

    #categorizing
    def setCatInput(self):
        # fill new category QlineEdit
        cats = self.db.show_tree()
        cats = [i[1] for i in cats]
        completer = QtWidgets.QCompleter(cats)
        completer.setCaseSensitivity(False)
        self.view.new_cat_name.setCompleter(completer)

    def new_cat(self):
        # called when signal emitted editingFinished on QLineEdit
        cat = self.view.new_cat_name.text()
        cat.strip()
        if not cat:
            return
        self.view.new_cat_name.setText('')
        self.db.filter_commit(cat)

        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.fill_tree() # will set top item and filter tables accordingly
        self.setCatInput()
    
    def addFltr_fromGR(self, txt):
        fltr = {}
        col_i = self.view.group_view.currentColumn()
        txt = re.split(r'x\d+:\s+', txt)[-1] # remove no of occurence, leave only sentence
        fltr[self.db.COL_NAME] = cfg.cat_col_names[col_i]
        fltr[self.db.FILTER] = txt
        self.__setFltrWidgets__(fltr)

    def addFltr_fromDB(self):
        """trigered by context menu in DB_view
        adds new filter based on selection
        """
        fltr = {}
        it = self.view.DB_cat_view.currentItem()
        col_i = self.view.DB_cat_view.column(it)
        fltr[self.db.COL_NAME] = self.view.DB_cat_view.horizontalHeaderItem(col_i).text()
        fltr[self.db.FILTER] = it.text()
        self.__setFltrWidgets__(fltr)
 
    def addFltr(self):
        """ads filter to db.cat_temp based on widgets selection
        """
        fltr = self.__getFltrWidgets__()
        if all(fltr.values()):
            self.db.filter_data(col=fltr[self.db.COL_NAME],
                                cat_filter=fltr[self.db.FILTER],
                                oper=fltr[self.db.OPER])
            self.fill_cat()
            self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
            self.fill_group()

    def rmFltr(self):
        """remove filter from db.cat_temp based on selected row
        """
        row_count = self.view.cat_view.rowCount() - 1
        it = self.view.cat_view.currentItem()
        row_i = row_count - self.view.cat_view.row(it)
        if row_i < row_count:
            self.db.cat_temp_rm(oper_n = row_i)
            if self.db.op_sub.empty: # removed completely filters
                self.fill_tree()
                return
            self.fill_cat()
            self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
            self.fill_group()

    def __getFltrWidgets__(self):
        """collects text from first row widgets in table cat table
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

    def __setFltrWidgets__(self, fltr):
        """set cat widgets according to requested
        """
        for col_i in range(self.view.cat_view.columnCount()):
            widget_name = self.view.cat_view.horizontalHeaderItem(col_i).text()
            if widget_name in fltr.keys():
                widget = self.view.cat_view.cellWidget(0,col_i)
                if isinstance(widget, QtWidgets.QComboBox):
                    widget.setCurrentText(fltr[widget_name])
                else:
                    widget.setText(fltr[widget_name])

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
            self.fill_group()
            self.fill_tree() # will set top item and filter tables accordingly
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
    
    def newDB(self):
        pass

    def save_asDB(self):
        pass

    def imp(self, bank):
        file = QtWidgets.QFileDialog.getOpenFileName(self.view, caption='Choose Excell file',
                                                                directory='',
                                                                filter=self.fs.getIMP(ext=True))
        # Qt lib returning always / as path separator
        # we need system specific, couse we are checking for file existence
        path = QtCore.QDir.toNativeSeparators(file[0])
        if path:
            self.fs.setIMP(path)
        else:  # operation canceled
            return
        if self.db.imp_data(self.fs.getIMP(), bank) is not None:
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
            #self.disp_statusbar('openDB')
            self.fill_trans()
            self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
            self.fill_group()
            self.fill_tree() # will set top item and filter tables accordingly
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def exp(self):
        pass