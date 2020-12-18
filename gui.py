import re
from qt_gui.main_window import Ui_banking, QtCore, QtGui, QtWidgets
from db import DB
from modules import FileSystem
import opt.parse_cfg as cfg



class GUIMainWin(QtWidgets.QMainWindow, Ui_banking):
    def __init__(self):
        super().__init__()
        self.setupUi(self)


class DBmodel(QtCore.QAbstractTableModel):
    def __init__(self, db): #, parent=None, *args):
        super().__init__() #self, parent, *args)
        self.db = db
        self.columns = self.db.columns
        self.backgroundColor = [False] * self.rowCount(None)
        self.fltr = ''
        self.markRows = []
    
    def markBlueAddRows(self,db):
        #append db, mark blue background color what already in model
        self.layoutAboutToBeChanged.emit()
        self.backgroundColor = [True] * self.rowCount(None)
        self.db = self.db.append(db, ignore_index=True)
        self.backgroundColor.extend([False] * len(db))
        self.layoutChanged.emit()

    def markFltr(self, fltr):
        if fltr:
            self.layoutAboutToBeChanged.emit()
            self.fltr = fltr
            self.layoutChanged.emit()
    
    def findRows(self, search):
        self.layoutAboutToBeChanged.emit()
        self.markRows = []
        if search:
            flags = QtCore.Qt.MatchContains | QtCore.Qt.MatchWrap
            for i in range(len(self.columns)):
                ind = self.match(self.createIndex(0,i),
                                QtCore.Qt.DisplayRole,
                                search,
                                hits=-1,
                                flags=flags)
                [self.markRows.append(i.row()) for i in ind]
        self.layoutChanged.emit()

    def DispColumns(self, col_names: [str]):
        # set displayed columns to provided names
        self.layoutAboutToBeChanged.emit()
        self.columns = col_names
        self.layoutChanged.emit()
        
    def rowCount(self, parent):
        # required by QAbstractTableModel
        return len(self.db)
    
    def columnCount(self, parent):
        # required by QAbstractTableModel
        return len(self.columns)
    
    def data(self, index, role):
        # required by QAbstractTableModel
        #can display only strings??, so convert numbers to string
        txt = str(self.db.loc[index.row(), self.columns[index.column()]])
        if role == QtCore.Qt.DisplayRole:
            if txt == 'None' or txt == '<NA>':
                txt = ''
            return txt
        # set background collors
        # green for fltr
        # blue when not categorized data requested
        if role == QtCore.Qt.BackgroundRole:
            # blue
            if self.backgroundColor[index.row()]:
                return QtGui.QBrush(QtGui.QColor(0, 170, 255))
            # green serach
            if index.row() in self.markRows:
                return QtGui.QBrush(QtGui.QColor(26, 255, 14))
            if txt == self.fltr:
                return QtGui.QBrush(QtGui.QColor(26, 255, 14))
        # set tool tip
        if role == QtCore.Qt.ToolTipRole:
            if txt != 'None':
                return txt
    
    def headerData(self, section, orientation, role):
        # required by QAbstractTableModel
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return self.columns[section]
   

class DBmodelProxy(QtCore.QSortFilterProxyModel):
    #extends model by sorting and filtering
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
        #trying to convert
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
        # timer to delay execution, i.e. textChnaged in QlineEdit
        self.timer = QtCore.QTimer()

    def connect_signals(self):
        self.view.installEventFilter(self)
        # file management
        self.view.openDB_btn.clicked.connect(self.openDB)
        self.view.newDB_btn.clicked.connect(self.newDB)
        self.view.save_asDB_btn.clicked.connect(self.save_asDB)
        self.view.impPKO_btn.clicked.connect(lambda: self.imp(bank='ipko'))
        self.view.impBNP_btn.clicked.connect(lambda: self.imp(bank='bnp'))
        self.view.impBNPkredyt_btn.clicked.connect(lambda: self.imp(bank='bnp_kredyt'))
        self.view.export_btn.clicked.connect(self.exp)
        
        #buttons
        self.view.addFltr_btn.clicked.connect(self.addFltr)
        self.view.edFltr_btn.clicked.connect(self.edFltr)
        self.view.upFltr_btn.clicked.connect(lambda: self.mvFltr(dir='up'))
        self.view.downFltr_btn.clicked.connect(lambda: self.mvFltr(dir='down'))
        self.view.rmFltr_btn.clicked.connect(self.rmFltr)
        self.view.addTrans_btn.clicked.connect(self.addTrans)
        self.view.upTrans_btn.clicked.connect(lambda: self.mvTrans(direction='up'))
        self.view.downTrans_btn.clicked.connect(lambda: self.mvTrans(direction='down'))
        self.view.rmTrans_btn.clicked.connect(lambda: self.modTrans('remove'))
        self.view.edTrans_btn.clicked.connect(lambda: self.modTrans('edit'))
        self.view.import_status_btn.accepted.connect(lambda: self.imp_commit('ok'))
        self.view.import_status_btn.rejected.connect(lambda: self.imp_commit('no'))
        # radio buttons
        self.view.also_not_cat.toggled.connect(lambda: self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view))
        self.view.markGrp.toggled.connect(self.markFltrColors)
        #text widgets
        self.view.new_cat_name.editingFinished.connect(self.new_cat)
        self.view.search.textChanged.connect(lambda: self.callDelay(self.search(), 800))
        
        # QTreeWidget
        self.view.tree_db.clicked.connect(self.con_tree)
        self.view.tree_db.itemChanged.connect(self.tree_edited)
        self.view.tree_db.itemDoubleClicked.connect(self.treeItemActivated)
        # group_view widgets triggers are defined when created, in self.__tabViewItems__()
        # DB_[cat|trans]_view context menu
        self.view.DB_cat_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.DB_cat_view.customContextMenuRequested.connect(self.DB_ContextMenu_cat)
        self.view.DB_trans_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.DB_trans_view.customContextMenuRequested.connect(self.DB_ContextMenu_trans)
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

        self.view.tabMenu.currentChanged.connect(lambda: self.headerSize(self.view.DB_cat_view))
        self.view.tabMenu.currentChanged.connect(lambda: self.headerSize(self.view.DB_trans_view))

    def eventFilter(self, source, event):
        """Catch signal:\n
        - if user closed the window\n
        - if user resize the window\n
        """
        #exit
        if event.type() == QtCore.QEvent.Close:
            self.exit()
        #win resize
        elif event.type() == QtCore.QEvent.Resize:
            if source.__class__ is self.view.__class__:
                self.callDelay(lambda: self.headerSize(self.view.DB_cat_view), 400)
                self.callDelay(lambda: self.headerSize(self.view.DB_trans_view), 400)
        return False

    def callDelay(self, method, delay):
        #called when textChanged on self.view.search signal eimitted
        self.timer.stop()
        ind = self.timer.metaObject().indexOfMethod('timeout()')
        if self.isSignalConnected(self.timer.metaObject().method(ind)):
            self.timer.timeout.disconnect(method)
        
        self.timer.setSingleShot(True)
        self.timer.timeout.connect(method)
        self.timer.start(delay)

    def DB_ContextMenu_cat(self, position):
        # need to know on which table clicked so to position context menu correctly
        self.DB_ContextMenu(position=position, source=self.view.DB_cat_view)

    def DB_ContextMenu_trans(self, position):
        # need to know on which table clicked so to position context menu correctly
        self.DB_ContextMenu(position=position, source=self.view.DB_trans_view,)

    def DB_ContextMenu(self, position, source):
        """trigered by context menu in DB_view
        adds new filter based on selection
        """
        fltr = {}
        widgets = {}

        if source.objectName() == 'DB_cat_view':
            oper = self.db.filter_data()
            txt = 'add filter: '
        else:
            oper = self.db.trans_col()
            txt = 'add transformation: '
        
        menu = QtWidgets.QMenu()
        
        for op in oper:
            widgets[op] = menu.addAction(f'{txt} {op}')
            
        act = menu.exec(source.mapToGlobal(position))

        index = source.selectionModel().currentIndex()
        model = source.model()
        txt = model.data(index, QtCore.Qt.DisplayRole)
        col = model.headerData(index.column(),QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
        fltr[self.db.COL_NAME] = col
        fltr[self.db.FILTER] = txt
        fltr[self.db.BANK] = self.db.getBank(col, txt)
        for op in oper:
            if act == widgets[op]:
                fltr[self.db.OPER] = op
                if op == list(self.db.trans_col())[4]:
                    fltr[self.db.VAL1] = txt
        if source.objectName() == 'DB_cat_view':
            self.__setFltrWidgets__(fltr, self.view.cat_view)
        else:
            self.__setFltrWidgets__(fltr, self.view.trans_view)

    def DB_headerContextMenu_cat(self, position):
        # need to know on which table clicked so to position context menu correctly
        self.DB_headerContextMenu(position=position, source=self.view.DB_cat_view,)

    def DB_headerContextMenu_trans(self, position):
        # need to know on which table clicked so to position context menu correctly
        self.DB_headerContextMenu(position=position, source=self.view.DB_trans_view,)

    def DB_headerContextMenu(self, source, position):
        """create context menu on DB_cat_view\n
        used to select visible columns
        """
        while True:
            all_widgets = {}
            menu = QtWidgets.QMenu()
            #widgetA = QtWidgets.QWidgetAction(menu)
            for i in cfg.op_col:
                #widget = QtWidgets.QRadioButton()
                widget = QtWidgets.QAction(i,menu)
                #widget.setText(i)
                widget.setCheckable(True)
                #widget.triggered.connect(lambda: self.col_vis(i))
                if i in cfg.cat_col_names:
                    widget.setChecked(True)
                else:
                    widget.setChecked(False)
                #widgetA.createWidget(widget)
                all_widgets[i] = widget
                menu.addAction(widget)
            menu.addSeparator()
            apply = menu.addAction('apply')
            act = menu.exec_(source.mapToGlobal(position))
            cfg.cat_col_names = []
            for col in all_widgets:
                if all_widgets[col].isChecked():
                    cfg.cat_col_names.append(col)
            if act == apply:
                self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
                self.fill_trans()
                self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
                self.fill_group()
                self.fill_cat()
                self.fill_cat()
                self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
                self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
                break

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
        [cat.remove(i) for i in it_names]

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
            # itemChanged signal will be cathced by self.treeEdited
        # rename existing category
        elif act == ren:
            self.before_edit = it_names[0]
            self.contextFunc = 'ren'
            self.view.tree_db.editItem(it[0])
            # itemChanged signal will be cathced by self.treeEdited
        # remove category
        elif act == rem:
            for i in it_names:
                self.db.get_filter_cat(i)
            self.db.filter_commit(name=cfg.GRANDPA)
            self.fill_tree()
        # merge selected categories
        elif act:
            if act.text() in it_names: # merge
                self.db.reset_temp_DB()
                for i in it_names:
                    self.db.get_filter_cat(i)
                self.db.filter_commit(name=act.text())
                self.fill_tree()
            # move selected categories
            elif act.text() in cat: # move
                self.db.reset_temp_DB()
                for i in it_names:
                    self.db.get_filter_cat(i)
                    self.db.filter_commit(name=act.text(), nameOf='parent')
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
            self.db.get_filter_cat(self.before_edit)
            self.db.filter_commit(name=it[0].text(0))

        elif self.contextFunc == 'add':
            it = self.view.tree_db.selectedItems()
            #create empty category under parent
            self.db.reset_temp_DB()
            self.db.filter_commit(name=it[0].text(0))
        
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
            it = cfg.cat_col_names.copy()
            # don't mess with categories
            it.remove(self.db.CATEGORY)
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
        def DB():
            data = self.db.group_data(cfg.cat_col_names[col_n])
            colWidget = QtWidgets.QComboBox()
            colWidget.insertItems(-1,data)
            colWidget.setEditable(False)
            colWidget.textActivated.connect(self.addFltr_fromGR)
            return colWidget
    
        # common stuff
        if tabName == 'DB':
            # for DB all widgets are the same
            colWidget = eval(tabName + '()')
        else:
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
        widget.setModel(DBmodelProxy(db))
        widget.setSortingEnabled(True)
        
        mod = widget.model().sourceModel()

        mod.DispColumns(cfg.cat_col_names)

        self.headerSize(widget)

        #mark green cells matching filter
        self.markFltrColors()

        # append not categorized data if required
        # but only for DB_cat_view widget
        if self.view.also_not_cat.isChecked() and widget.objectName() == 'DB_cat_view':
            mod.markBlueAddRows(self.db.getOP(not_cat = True))

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
        """called when QTreeWidget selected item\n
        set sub date from selected category and refresh tables
        """
        it = self.view.tree_db.selectedItems()
        #reset temp DB if new selection (one item selected only)
        self.db.reset_temp_DB()

        # limit views to selected category
        cat = [i.text(0) for i in it]
        self.db.get_filter_cat(cat)

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
        # fill category completer for search QlineEdit
        cats = self.db.show_tree()
        cats = [i[1] for i in cats]
        completer = QtWidgets.QCompleter(cats)
        completer.setCaseSensitivity(False)
        self.view.new_cat_name.setCompleter(completer)

    def new_cat(self):
        # called when signal emitted editingFinished on QLineEdit new category 
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
        self.__setFltrWidgets__(fltr, self.view.cat_view)
 
    def addTrans(self):
        """triggered when trans add btn clicked\n
        """
        fltr = self.__getFltrWidgets__(self.view.trans_view)
        self.db.trans_col(bank=fltr[self.db.BANK],
                        col_name=fltr[self.db.COL_NAME],
                        op=fltr[self.db.OPER],
                        val1=fltr[self.db.VAL1],
                        val2=fltr[self.db.VAL2])
        self.fill_trans()
        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.con_tree()
        #DEBUG
        print(self.db.trans)

    def modTrans(self, oper):
        # modify row in trans db: edit or remove
        if not self.view.trans_view.currentItem(): return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.trans_view.currentItem().row()
        
        if oper == 'edit':
            fltr = {}
            for col in range(self.view.trans_view.columnCount()):
                fltr[cfg.trans_col[col]] = self.view.trans_view.item(row_n, col).text()
        
        self.db.trans_rm(row_n - 1)
        #refresh views
        self.fill_trans()
        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.fill_group()
        self.fill_tree() # will set top item and filter tables accordingly
        self.setCatInput()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
        
        if oper == 'edit':
            self.__setFltrWidgets__(fltr, self.view.trans_view)
        #DEBUG
        print(self.db.trans)

    def mvTrans(self, direction):
        # move row in trans db
        if not self.view.trans_view.currentItem(): return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.trans_view.currentItem().row()

        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        if not self.db.trans_mv(row_n - 1, direction):
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
            return

        #refresh views
        self.fill_trans()
        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.fill_group()
        self.fill_tree() # will set top item and filter tables accordingly
        self.setCatInput()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def addFltr(self):
        """triggered when cat add btn clicked\n
        ads filter to db.cat_temp based on widgets selection
        """
        fltr = self.__getFltrWidgets__(self.view.cat_view)
        if all(fltr.values()):
            self.db.filter_data(col=fltr[self.db.COL_NAME],
                                cat_filter=fltr[self.db.FILTER],
                                oper=fltr[self.db.OPER])
            self.fill_cat()
            self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
            self.fill_group()

    def edFltr(self):
        pass

    def mvFltr(self, dir):
        pass

    def rmFltr(self):
        """remove filter from db.cat_temp based on selected row
        """
        row_count = self.view.cat_view.rowCount() - 1
        it = self.view.cat_view.currentItem()
        row_i = row_count - self.view.cat_view.row(it) #becouse view is reversed
        if row_i < row_count:
            self.db.filter_temp_rm(oper_n = row_i)
            if self.db.op_sub.empty: # removed completely filters
                self.fill_tree()
                return
            self.fill_cat()
            self.fill_DB(self.db.getOPsub(), self.view.DB_cat_view)
            self.fill_group()

    def __getFltrWidgets__(self, source):
        """collects text from first row widgets in table cat table
        """
        fltr = {}
        for col_i in range(source.columnCount()):
            widget = source.cellWidget(0,col_i)
            if isinstance(widget, QtWidgets.QComboBox):
                widget_txt = widget.currentText()
            else:
                widget_txt = widget.text()
            widget_name = source.horizontalHeaderItem(col_i).text()
            fltr[widget_name] = widget_txt
        return fltr

    def __setFltrWidgets__(self, fltr, source):
        """set cat or trans widgets according to requested
        """
        for col_i in range(source.columnCount()):
            widget_name = source.horizontalHeaderItem(col_i).text()
            if widget_name in fltr.keys():
                widget = source.cellWidget(0,col_i)
                if isinstance(widget, QtWidgets.QComboBox):
                    widget.setCurrentText(fltr[widget_name])
                else:
                    widget.setText(fltr[widget_name])
        
        #mark green cells matching filter
        self.markFltrColors()

    def markFltrColors(self):
        # mark filter by color
        # make sure we already have model in view
        mod = self.view.DB_cat_view.model() or None
        if mod:
            if self.view.markGrp.isChecked():
                # clean search
                self.view.search.setText('')
                fltr = self.__getFltrWidgets__(self.view.cat_view)
                mod.sourceModel().markFltr(fltr[self.db.FILTER])
            else:
                mod.sourceModel().markFltr('')
        
    def search(self):
        # mark search result with color
        # make sure we already have model in view
        mod = self.view.DB_cat_view.model() or None
        if mod:
            txt = self.view.search.text()
            mod.sourceModel().findRows(txt)
            mod.sort(0,QtCore.Qt.AscendingOrder)

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
        file = QtWidgets.QFileDialog.getOpenFileName(self.view, caption='Choose name for new SQlite3 file',
                                                                directory='',
                                                                filter=self.fs.getDB(ext=True))
        # Qt lib returning always / as path separator
        # we need system specific, couse we are checking for file existence
        path = QtCore.QDir.toNativeSeparators(file[0])
        if path:
            self.fs.setDB(path)
        else:  # operation canceled
            return
        self.fs.writeOpt("LastDB", self.fs.getDB())
        self.db = DB()
        self.fill_trans()
        self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
        self.fill_group()
        self.fill_tree() # will set top item and filter tables accordingly
        self.setCatInput()

    def save_asDB(self):
        self.db.write_db(self.fs.getDB())

    def exit(self):
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
            self.view.import_info.show()
            self.view.import_status_btn.show()

    def imp_commit(self, sig):
        #hide import widgets
        self.view.import_info.hide()
        self.view.import_status_btn.hide()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        self.db.imp_comit(sig)
        if sig == 'ok':
            #refresh views
            self.fill_trans()
            self.fill_DB(self.db.getOP(), self.view.DB_trans_view)
            self.fill_group()
            self.fill_tree() # will set top item and filter tables accordingly
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def exp(self):
        # export data to csv
        pass

    # GUI
    def headerSize(self, view):
        # spread the columns, get the size, change to interactive mode and set size manualy
        header = view.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        for i in range(header.count()):
            size = header.sectionSize(i)
            header.setDefaultSectionSize(size)
        header.setSectionResizeMode(QtWidgets.QHeaderView.Interactive)

