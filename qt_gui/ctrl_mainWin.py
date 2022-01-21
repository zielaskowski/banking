"""
controls GUI windows
"""
import re
from wsgiref.validate import validator
from PyQt5 import QtCore, QtGui, QtWidgets
from db import DB
from modules import FileSystem
from models import DBmodelProxy
import opt.parse_cfg as cfg
from qt_gui.gui_classes import GUIMainWin
from qt_gui.ctrl_plot import GUIPlot_ctrl
from qt_gui.gui_classes import GUICalendar


class GUIMainWin_ctrl(QtCore.QObject):
    def __init__(self):
        self.view = GUIMainWin()
        super().__init__(self.view)
        self.view.show()
        self.db = DB()
        self.tmpSplit = []
        self.db.connect(parent=self.showMsg)
        self.fs = FileSystem()
        # connect signals
        self.connect_signals()
        # hide import widgets
        self.view.import_info.hide()
        self.view.import_status_btn.hide()
        # controling context menu
        self.before_edit = ''
        self.contextFunc = ''
        # timer to delay execution, i.e. textChnaged in QlineEdit
        self.timer = QtCore.QTimer()
        # set status bar
        self.setStatusBar()
        # stores curent category selected
        self.curCat = cfg.GRANDPA
        # store tree summary column
        self.summCol = ''
        # read options
        self.setStat()
        self.view.db_stat_txt.setText(self.fs.getOpt('welcome'))
        file = self.fs.getOpt('LastDB')
        if file:
            self.openDB(file=file)

    # signals and connections
    def connect_signals(self):
        self.view.installEventFilter(self)
        # file management
        self.view.openDB_btn.clicked.connect(self.openDB)
        self.view.newDB_btn.clicked.connect(self.newDB)
        self.view.save_asDB_btn.clicked.connect(self.save_asDB)
        self.view.imp_btn.clicked.connect(self.imp)
        self.view.export_btn.clicked.connect(self.exp)

        # buttons
        self.view.addFltr_btn.clicked.connect(self.addFltr)
        self.view.edFltr_btn.clicked.connect(lambda: self.modFltr('edit'))
        self.view.upFltr_btn.clicked.connect(
            lambda: self.mvFltr(direction='up'))
        self.view.downFltr_btn.clicked.connect(
            lambda: self.mvFltr(direction='down'))
        self.view.rmFltr_btn.clicked.connect(lambda: self.modFltr('remove'))
        self.view.addSplit_btn.clicked.connect(self.addSplit)
        self.view.edSplit_btn.clicked.connect(lambda: self.modSplit('edit'))
        self.view.rmSplit_btn.clicked.connect(lambda: self.modSplit('remove'))
        self.view.addTrans_btn.clicked.connect(self.addTrans)
        self.view.upTrans_btn.clicked.connect(
            lambda: self.mvTrans(direction='up'))
        self.view.downTrans_btn.clicked.connect(
            lambda: self.mvTrans(direction='down'))
        self.view.rmTrans_btn.clicked.connect(lambda: self.modTrans('remove'))
        self.view.edTrans_btn.clicked.connect(lambda: self.modTrans('edit'))
        self.view.import_status_btn.accepted.connect(
            lambda: self.imp_commit('ok'))
        self.view.import_status_btn.rejected.connect(
            lambda: self.imp_commit('no'))
        self.view.plot_btn.clicked.connect(self.plot)
        # radio buttons
        self.view.also_not_cat.toggled.connect(self.alsoNoCatData)
        self.view.markGrp.toggled.connect(self.markFltrColors)
        # text widgets
        self.view.search.textChanged.connect(
            lambda: self.callDelay(self.search, 800))
        self.view.new_cat_name.returnPressed.connect(self.catORsplit)
        # cat_view
        self.view.cat_view.itemSelectionChanged.connect(self.markFltrColors)
        # split - calendar widget
        self.view.split_view.itemSelectionChanged.connect(self.showCalendar)
        # win layout change
        self.view.splitter_2.splitterMoved.connect(
            lambda: self.callDelay(self.headerSize, 200))
        # QTreeWidget
        self.view.tree_db.clicked.connect(self.con_tree)
        self.view.tree_db.itemChanged.connect(self.tree_edited)
        self.view.tree_db.itemEntered.connect(self.tree_edited)
        self.view.tree_db.itemDoubleClicked.connect(self.treeItemActivated)
        # group_view widgets triggers are defined when created, in self.__viewWidget__()
        # DB_view context menu
        self.view.DB_view.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.DB_view.customContextMenuRequested.connect(
            self.DB_ContextMenu)
        # DB_view header context menu
        head = self.view.DB_view.horizontalHeader()
        head.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        head.customContextMenuRequested.connect(self.DB_headerContextMenu)
        # QTreeWidget context menu
        self.view.tree_db.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.view.tree_db.customContextMenuRequested.connect(
            self.TreeContextMenu)
        # tab changed
        self.view.tabMenu.currentChanged.connect(self.DBforTab)

    def connectTreeWidget(self, txt):
        self.summCol = txt
        self.fill_tree()

    def eventFilter(self, source, event):
        """Catch signal:\n
        - if user closed the window\n
        - if user resize the window\n
        """
        # exit
        if event.type() == QtCore.QEvent.Close:
            self.save_asDB()
            self.fs.writeOpt(op='visColumns', val=self.visCol)
        # win resize
        elif event.type() == QtCore.QEvent.Resize:
            if source.__class__ is self.view.__class__:
                self.callDelay(self.headerSize, 200)
        return False

    def callDelay(self, method, delay):
        # called when you want delay signal
        # can be one timer only :(

        try:
            self.timer.timeout.disconnect(method)
        except TypeError:
            pass

        self.timer.setSingleShot(True)
        self.timer.timeout.connect(method)
        self.timer.start(delay)

    # context menus and tree selection

    def DB_ContextMenu(self, position):
        """trigered by context menu in DB_view
        adds new filter based on selection
        """
        fltr = {}
        widgets = {}
        source = self.view.DB_view

        if self.view.tabMenu.currentIndex() == 2:  # 'categorize'
            oper = self.db.cat.opers()
            txt = 'add filter: '
        elif self.view.tabMenu.currentIndex() == 1:  # 'wrangling'
            oper = self.db.trans.opers()
            txt = 'add transformation: '
        elif self.view.tabMenu.currentIndex() == 3:  # split
            oper = ['sel row']
            txt = 'add split: '
        else:
            return

        menu = QtWidgets.QMenu()

        for op in oper:
            widgets[op] = menu.addAction(f'{txt} {op}')

        act = menu.exec(source.mapToGlobal(position))

        indexProxy = source.selectionModel().currentIndex()
        indexModel = source.model().mapToSource(indexProxy)
        model = source.model().sourceModel()

        txt = model.data(indexModel, QtCore.Qt.DisplayRole)
        col = model.headerData(indexModel.column(),
                               QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
        bankIndex = model.createIndex(
            indexModel.row(), cfg.op_col.index(self.db.BANK))
        hashIndex = model.createIndex(
            indexModel.row(), cfg.op_col.index(self.db.HASH))
        bank = model.data(bankIndex, QtCore.Qt.DisplayRole)
        hashRow = model.data(hashIndex, QtCore.Qt.DisplayRole)

        for op in oper:
            if act == widgets[op]:
                fltr[self.db.OPER] = op
                if op == 'str.replace':
                    fltr[self.db.VAL1] = txt
                if op == 'sel row':
                    fltr[self.db.COL_NAME] = self.db.HASH
                    fltr[self.db.FILTER] = hashRow
                else:
                    fltr[self.db.COL_NAME] = col
                    fltr[self.db.FILTER] = txt
                    fltr[self.db.BANK] = bank

        if self.view.tabMenu.currentIndex() == 2:  # 'categorize'
            self.__setFltrWidgets__(fltr, self.view.cat_view)
        elif self.view.tabMenu.currentIndex() == 1:  # wrangling
            self.__setFltrWidgets__(fltr, self.view.trans_view)
        elif self.view.tabMenu.currentIndex() == 3:  # split
            self.__setFltrWidgets__(fltr, self.view.split_view)

    def DB_headerContextMenu(self, position):
        """create context menu on DB_cat_view\n
        used to select visible columns
        """
        while True:
            all_widgets = {}
            menu = QtWidgets.QMenu()
            for i in cfg.op_col:
                widget = QtWidgets.QAction(i, menu)
                widget.setCheckable(True)
                if i in self.visCol:
                    widget.setChecked(True)
                else:
                    widget.setChecked(False)
                all_widgets[i] = widget
                menu.addAction(widget)
            menu.addSeparator()
            apply = menu.addAction('apply')
            act = menu.exec_(self.view.DB_view.mapToGlobal(position))

            prevCols = self.visCol
            self.visCol = []
            for col in all_widgets:
                if all_widgets[col].isChecked():
                    self.visCol.append(col)
            if act == apply:
                self.fs.writeOpt(op='visColumns', val=self.visCol)
                self.fill_DB('')  # fill_DB() will not affect the model
                break
            else:
                if prevCols == self.visCol:
                    # nothing changed so exit
                    return

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

        cat = self.db.tree.allChild()
        cat = list(cat.keys())
        kids = {}
        [kids.update(self.db.tree.allChild(catStart=i)) for i in it_names]
        kids = list(kids.keys())
        [cat.remove(i) for i in kids]

        menu = QtWidgets.QMenu()
        add = menu.addAction("add category")
        ren = menu.addAction("rename category")
        mer = menu.addMenu("merge category")
        spl = menu.addAction("split category")
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
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        # adding empty category
        if act == add:
            self.contextFunc = 'add'
            self.view.tree_db.blockSignals(True)
            # create kid under selected item
            new_name = self.db.tree.new_cat()
            it_kid = QtWidgets.QTreeWidgetItem(it[0], [new_name, ''])
            flag = QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled
            it_kid.setFlags(flag)
            # clear all selection
            self.view.tree_db.selectionModel().clear()
            # select only kid
            it_kid.setSelected(True)
            self.view.tree_db.blockSignals(False)
            # create new item
            self.tree_edited()
            # edit name
            # tree already redraw so all references lost
            it_new = self.view.tree_db.selectedItems()
            self.contextFunc = 'ren'
            self.before_edit = it_new[0].text(0)
            self.view.tree_db.editItem(it_new[0])
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
                # tree will handle children and data inside cat
                # move children to Grandpa
                # move data to grandpa
                self.db.treeRm(child=i)
            self.fill_tree()
        elif act == spl:
            # set split tab
            self.view.tabMenu.setCurrentIndex(3)
            # set current cat (right click on tree doesnot do that)
            self.con_tree()
            catData = self.db.op.get(self.curCat)
            # min date
            minDate = catData.loc[:, self.db.DATA_OP].min()
            # max date
            maxDate = catData.loc[:, self.db.DATA_OP].max()
            fltr = {self.db.START: str(minDate.date()),
                    self.db.END: str(maxDate.date()),
                    self.db.COL_NAME: self.db.CATEGORY,
                    self.db.FILTER: self.curCat}
            self.__setFltrWidgets__(fltr, self.view.split_view)
        # merge selected categories
        elif act:
            if act.text() in it_names:  # merge
                it_names.remove(act.text())
                for i in it_names:
                    self.db.treeRen(new_category=act.text(), category=i)
                self.fill_tree()
            # move selected categories
            elif act.text() in cat:  # move
                for i in it_names:
                    self.db.treeMov(new_parent=act.text(), child=i)
                self.fill_tree()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

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
        when redrawing the tree also can be triggered, so important to block signals
        """
        if self.contextFunc == '':
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        it = self.view.tree_db.selectedItems()
        if self.contextFunc == 'ren':
            self.db.treeRen(category=self.before_edit,
                            new_category=it[0].text(0))
            self.setCatInput()

        elif self.contextFunc == 'add':
            it_parent = it[0].parent()
            self.db.treeAdd(parent=it_parent.text(0), child=it[0].text(0))
            self.setCatInput()

        self.before_edit = ''
        self.contextFunc = ''
        self.curCat = it[0].text(0)
        self.fill_tree()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    # fill tables
    def __viewWidget__(self, tabName, col_n):
        """gets table name and col id, call appropriate function and return QtWidget\n
        table names: ['op', 'cat', 'trans', 'tree', 'split']
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
            if self.db.CATEGORY in it:
                it.remove(self.db.CATEGORY)
            # add ALL column names
            it.append('ANY')
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def trans2():
            # QtWidget:combo, col_name:OPER
            colWidget = QtWidgets.QComboBox()
            it = self.db.trans.opers()
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

        def trans5():
            # QtWidget:lineEdit, col_name:trans_n
            return trans3()

        def cat0():
            # QtWidget:combo, col_name:col_name
            colWidget = QtWidgets.QComboBox()
            it = cfg.cat_col_names.copy()
            # add ALL column names
            it.append('ANY')
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def cat1():
            # QtWidget:lineEdit, function:selector
            colWidget = QtWidgets.QComboBox()
            it = self.db.cat.fltrs()
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def cat2():
            # QtWidget:combo, col_name:filter
            colWidget = QtWidgets.QLineEdit()
            return colWidget

        def cat3():
            # filter_n
            return cat2()

        def cat4():
            # QtWidget:combo, col_name:oper
            colWidget = QtWidgets.QComboBox()
            it = self.db.cat.opers()
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def cat5():
            # oper_n
            return cat2()

        def cat6():
            # QtWidget:lineEdit, col_name:category
            return cat2()

        def cat7():
            # QtWidget:lineEdit, col_name:category
            return cat2()

        def DB():
            if self.view.also_not_cat.isChecked() or not self.view.also_not_cat.isEnabled():
                cat = cfg.GRANDPA
            else:
                cat = self.curCat
            data = self.db.op.group_data(col=self.visCol[col_n], category=cat)
            colWidget = QtWidgets.QComboBox()
            colWidget.insertItems(-1, data)
            colWidget.setEditable(False)
            colWidget.textActivated.connect(self.addFltr_fromGR)
            return colWidget

        def tree1():
            # widget for column header in tree
            colWidget = QtWidgets.QComboBox()
            colWidget.insertItems(-1, self.db.op.sum_data())
            colWidget.setEditable(False)
            return colWidget

        def split0():
            # QLineEdit start date
            colWidget = QtWidgets.QLineEdit()
            colWidget.setText('-')
            colWidget.setReadOnly(True)
            return colWidget

        def split1():
            # QLineEdit end date
            return split0()

        def split2():
            # QComboWidget col_name
            colWidget = QtWidgets.QComboBox()
            it = [self.db.CATEGORY, self.db.HASH]
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def split3():
            # QLineEdit filter
            colWidget = QtWidgets.QComboBox()
            it = self.db.tree.allChild().keys()
            colWidget.insertItems(-1, it)
            colWidget.setEditable(False)
            return colWidget

        def split4():
            # QLineEdit value
            colWidget = QtWidgets.QLineEdit()
            colWidget.setValidator(QtGui.QDoubleValidator(top=0, decimals=2))
            return colWidget

        def split5():
            # QLineEdit days
            colWidget = QtWidgets.QLineEdit()
            if self.db.op.op.empty:
                totDays = 0
            else:
                totDays = self.db.op.op.loc[:, self.db.DATA_OP].max() \
                    - self.db.op.op.loc[:, self.db.DATA_OP].min()
                totDays = totDays.days
            colWidget.setValidator(QtGui.QDoubleValidator(
                bottom=0, top=totDays, decimals=0))
            return colWidget

        def split6():
            # QLineEdit split_n
            colWidget = QtWidgets.QLineEdit()
            return colWidget

        def split7():
            # QLineEdit split_n
            return split6()

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

    def fill(self, dbTxt: str):
        """Fill widget filters table
        """
        db = eval(f'self.db.{dbTxt}')
        widget = eval(f'self.view.{dbTxt}_view')
        cols = eval(f'cfg.{dbTxt}_col')
        # store filters if exists so to not reset when switching tabs
        fltr = self.__getFltrWidgets__(widget)
        if dbTxt == 'trans':
            hideCols = [self.db.TRANS_N]
        elif dbTxt == 'cat':
            db.setCat(self.curCat)
            hideCols = [self.db.FILTER_N, self.db.OPER_N,
                        self.db.CATEGORY, self.db.FILTER_ORIG]
        elif dbTxt == 'split':
            db.setSplit(self.curCat)
            hideCols = [self.db.SPLIT_N, self.db.CATEGORY]

        rows_n = len(db)
        widget.setColumnCount(len(cols))
        widget.setRowCount(0)  # reset table size
        widget.setRowCount(rows_n + 1)
        # set column labels
        widget.setHorizontalHeaderLabels(cols)
        # populate table
        for x in cols:
            # first row
            x_n = cols.index(x)
            widget.setCellWidget(0, x_n, self.__viewWidget__(dbTxt, x_n))
            # and the other
            for y in range(rows_n):
                cell = QtWidgets.QTableWidgetItem(str(db[y, x]))
                flag = cell.flags()
                flag ^= QtCore.Qt.ItemIsEditable
                cell.setFlags(flag)
                widget.setItem(y + 1, x_n, cell)

        # restore filters if existed
        self.__setFltrWidgets__(fltr, widget)
        # hide some columns and resize
        [widget.setColumnHidden(cols.index(i), True)
            for i in hideCols]
        col = widget.horizontalHeader()
        col.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        # select col_name column (anything but date in the split)
        ind = widget.model().index(0, cols.index(self.db.COL_NAME))
        widget.setCurrentIndex(ind)

    def fill_group(self):
        """fills group_view
        """
        self.view.group_view.setSortingEnabled(
            False)  # otherway we end up with mess
        row_n = 1
        self.view.group_view.setColumnCount(len(self.visCol))
        self.view.group_view.setRowCount(0)  # reset table
        self.view.group_view.setRowCount(row_n)
        # hide column and row header
        head = self.view.group_view.horizontalHeader()
        head.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        head.hide()
        row = self.view.group_view.verticalHeader()
        row.hide()
        # populate table
        for x in self.visCol:
            x_n = self.visCol.index(x)
            self.view.group_view.setCellWidget(
                0, x_n, self.__viewWidget__('DB', x_n))
        self.view.group_view.setSortingEnabled(True)

    def fill_DB(self, db):
        """fills DB tableView\n
        using op DB\n
        - will set new data in view model
        - if db not panda, will adjust columns only, used when changing cols visibility to speed up
        - hide/show columns based on self.visCol
        - resize columns
        - fill groups widgets
        """
        widget = self.view.DB_view
        if isinstance(db, type(self.db.op.op)):
            widget.setModel(DBmodelProxy(db))
            widget.setSortingEnabled(True)

        # hide some columns and resize
        [widget.setColumnHidden(cfg.op_col.index(i), i not in self.visCol)
         for i in cfg.op_col]

        self.headerSize()

        # update grouping table
        self.fill_group()

    def con_tree(self):
        """called when QTreeWidget selected item\n
        or also from also_not_cat QRadioButton signal\n
        set sub date from selected category and refresh tables
        """
        self.view.new_cat_name.blockSignals(True)
        it = self.view.tree_db.selectedItems()
        itText = [i.text(0) for i in it]

        # limit views to selected category
        if cfg.GRANDPA in itText:
            self.curCat = cfg.GRANDPA
            # don't mess up with GRANDPA
            self.view.also_not_cat.setChecked(False)
            self.view.also_not_cat.setDisabled(True)
            self.view.tree_db.clearSelection()
            self.view.tree_db.setCurrentItem(self.view.tree_db.topLevelItem(1))
            self.fill_DB(self.db.op.get(category=cfg.GRANDPA))
        elif itText:
            self.curCat = itText[-1]
            self.view.also_not_cat.setDisabled(False)
            self.fill_DB(self.db.op.get(category=self.curCat))

        self.alsoNoCatData()

        self.DBforTab()
        self.view.new_cat_name.blockSignals(False)

    def fill_tree(self):
        # DEBUG
        print(self.db.trans.trans)
        print(self.db.cat.cat)
        print(self.db.split.split)
        print(self.db.tree.tree)
        print(self.db.op.op.loc[self.db.op.op.loc[:, self.db.CATEGORY]
              != cfg.GRANDPA, self.db.CATEGORY].unique())
        #####
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        self.view.tree_db.blockSignals(True)
        # reset the tree
        self.view.tree_db.clear()
        # allow multiselection (it's limited for Grandpa in self.con_tree())
        self.view.tree_db.setSelectionMode(
            QtWidgets.QAbstractItemView.ExtendedSelection)

        # first row is combo box to choose summation column
        comboRow = QtWidgets.QTreeWidgetItem([self.db.CATEGORY, ''])
        comboRow.setFlags(comboRow.flags() & ~QtCore.Qt.ItemIsSelectable)
        comboRow.setBackground(0, QtGui.QColor(26, 255, 14))

        # add combo box
        colWidget = self.__viewWidget__(tabName='tree', col_n=1)
        colWidget.setCurrentText(self.summCol)
        colWidget.currentIndexChanged.connect(
            lambda: self.connectTreeWidget(colWidget.currentText()))
        self.view.tree_db.addTopLevelItem(comboRow)
        self.view.tree_db.setItemWidget(comboRow, 1, colWidget)

        # hide columns
        self.view.tree_db.setHeaderHidden(True)

        cats = self.db.tree.allChild()
        cats = list(cats.keys())
        cats.remove(cfg.GRANDPA)

        # second row is Grandpa
        summ = self.db.op.sum_data(
            op=colWidget.currentText(), category=cfg.GRANDPA)
        grandpa = QtWidgets.QTreeWidgetItem(
            self.view.tree_db, [cfg.GRANDPA, summ])
        self.view.tree_db.addTopLevelItem(grandpa)

        # than other
        match = QtCore.Qt.MatchExactly | QtCore.Qt.MatchRecursive
        for cat in cats:
            parent = self.view.tree_db.findItems(
                self.db.tree.parent(child=cat), match)
            summ = self.db.op.sum_data(
                op=colWidget.currentText(), category=cat)
            it = QtWidgets.QTreeWidgetItem(parent[0], [cat, summ])
            flag = QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled
            it.setFlags(flag)

        # expand and resize
        self.view.tree_db.expandAll()
        self.view.tree_db.resizeColumnToContents(0)

        # select curIt in the tree
        it = self.view.tree_db.findItems(self.curCat, match)
        if not it:
            self.curCat = cfg.GRANDPA
            it = self.view.tree_db.findItems(self.curCat, match)
        self.view.tree_db.setCurrentItem(it[0])

        self.con_tree()  # and adjust tables
        self.view.tree_db.blockSignals(False)
        self.setCatInput()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    # categorizing
    def setCatInput(self):
        # fill category completer for search QlineEdit
        cats = self.db.tree.allChild()
        cats = list(cats.keys())
        completer = QtWidgets.QCompleter(cats)
        completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        # PopupCompletion will freeze when hit enter on popup
        completer.setCompletionMode(QtWidgets.QCompleter.InlineCompletion)
        self.view.new_cat_name.setCompleter(completer)

    def addFltr_fromGR(self, txt):
        fltr = {}
        col_i = self.view.group_view.currentColumn()
        # remove no of occurence, leave only sentence
        txt = re.split(r'x\d+:\s+', txt)[-1]
        fltr[self.db.COL_NAME] = self.visCol[col_i]
        fltr[self.db.FILTER] = txt
        self.__setFltrWidgets__(fltr, self.view.cat_view)
        self.view.tabMenu.setCurrentIndex(2)  # categorize tab
        # copy to search QLineEdit, which will color selected items
        self.view.search.blockSignals(True)
        self.view.search.setText(txt)
        self.view.search.blockSignals(False)
        self.search()

    def catORsplit(self):
        '''
        triggered by enter in category name QLineEdit
        based on current tab will choose addCat or addSplit
        '''
        tabs = {0: 'data',
                1: 'trans',
                2: 'cat',
                3: 'split'}
        tab = self.view.tabMenu.currentIndex()
        if tabs[tab] == 'cat':
            self.addFltr()
        if tabs[tab] == 'split':
            self.addSplit()

    def addTrans(self):
        """triggered when trans add btn clicked\n
        """
        fltr = self.__getFltrWidgets__(self.view.trans_view)
        if all([fltr[i] for i in fltr
                if i in [self.db.VAL1]]):
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
            self.db.transAdd(fltr=fltr)
            self.fill('trans')
            self.fill_DB(self.db.op.get(self.curCat))
            self.fill_tree()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
            # reset filters
            self.__setFltrWidgets__({}, self.view.trans_view)

    def modTrans(self, oper: "edit|remove"):
        # modify row in trans db: edit or remove
        if not self.view.trans_view.currentItem():
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.trans_view.currentItem().row()
        col_n = cfg.trans_col.index(self.db.TRANS_N)
        trans_n = self.view.trans_view.item(row_n, col_n).text()

        self.db.transRm(trans_n=trans_n)
        if oper == 'edit':
            fltr = {}
            for col in range(self.view.trans_view.columnCount()):
                fltr[cfg.trans_col[col]] = self.view.trans_view.item(
                    row_n, col).text()

        # refresh views
        self.fill('trans')
        self.fill_DB(self.db.op.get(self.curCat))
        self.fill_tree()  # will set top item and filter tables accordingly
        self.setCatInput()

        # fill_tree() will reset cat view, so only after we can set widgets
        if oper == 'edit':
            self.__setFltrWidgets__(fltr, self.view.trans_view)

        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def mvTrans(self, direction: "up|down"):
        # move row in trans db
        if not self.view.trans_view.currentItem():
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.trans_view.currentItem().row()

        if direction == 'up':
            new_row_n = row_n - 1
        else:
            new_row_n = row_n + 1
        self.db.transMov(trans_n=row_n, new_trans_n=new_row_n)
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

        # refresh views
        self.fill('trans')
        self.fill_tree()  # will set top item and filter tables accordingly
        self.fill_DB(self.db.op.get(self.curCat))
        self.setCatInput()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def addFltr(self):
        """triggered when cat add btn clicked 
        """
        # read new filter
        fltr = self.__getFltrWidgets__(self.view.cat_view)
        if fltr == {}:
            return
        
        # clear search QEditLine
        self.view.search.setText('')
        
        # def category
        # if curCat == grandpa, take category from new_cat_name or filter or "new category"
        if self.curCat == cfg.GRANDPA:
            cat = self.view.new_cat_name.text()\
                or fltr[self.db.FILTER].strip()\
                or self.db.tree.new_cat()
            self.view.new_cat_name.setText(cat)
            self.curCat = cat

        # def parent
        parent = self.db.tree.parent(self.curCat)
        if not parent:
            parent = cfg.GRANDPA

        fltr[self.db.CATEGORY] = self.curCat
        if all([fltr[i] for i in fltr
                if i in [self.db.OPER, self.db.COL_NAME, self.db.FILTER]]):
            if not self.db.catAdd(fltr=fltr, parent=parent):
                self.curCat = cfg.GRANDPA  # something went wrong
                self.view.tree_db.selectionModel().clear()
                self.view.tree_db.topLevelItem(1).setSelected(True)
            else:
                # restore split if removed during edit
                if self.tmpSplit != []:
                    for split in self.tmpSplit:
                        self.db.splitAdd(split, parent=parent)
                    self.tmpSplit = []

            if self.curCat != cfg.GRANDPA:
                self.view.also_not_cat.setChecked(False)
                self.fill_tree()
            else:
                self.fill('cat')
                # enable radio button allowing show all data
                # and un check
                self.view.also_not_cat.setEnabled(True)
                self.view.also_not_cat.setChecked(False)
                self.fill_DB(self.db.op.get(self.curCat))
            # reset filters
            self.__setFltrWidgets__({}, self.view.cat_view)

    def modFltr(self, oper: "edit|remove"):
        # modify category filter: edit or remove
        if not self.view.cat_view.currentItem():
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.cat_view.currentItem().row()
        col_n = cfg.cat_col.index(self.db.OPER_N)
        oper_n = self.view.cat_view.item(row_n, col_n).text()

        self.tmpSplit = self.db.catRm(oper_n=oper_n, category=self.curCat)
        self.tmpSplit[self.db.FILTER_ORIG] = ''

        if oper == 'edit':
            fltr = {}
            for col in range(self.view.cat_view.columnCount()):
                fltr[cfg.cat_col[col]] = self.view.cat_view.item(
                    row_n, col).text()
        else:
            self.tmpSplit = []

        self.fill_tree()
        # fill_tree() will reset cat view, so only after we can set widgets
        if oper == 'edit':
            self.__setFltrWidgets__(fltr, self.view.cat_view)

        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def mvFltr(self, direction: "up|down"):
        if not self.view.cat_view.currentItem():
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.cat_view.currentItem().row()

        if direction == 'up':
            new_row_n = row_n - 1
        else:
            new_row_n = row_n + 1

        self.db.catMov(oper_n=row_n, new_oper_n=new_row_n,
                       category=self.curCat)

        self.fill('cat')
        self.fill_DB(self.db.op.get(self.curCat))
        self.setCatInput()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def addSplit(self):
        fltr = self.__getFltrWidgets__(self.view.split_view)
        if fltr == {}:
            return
        
        parent = self.curCat
        # take category from new_cat_name or "new category"
        self.curCat = self.view.new_cat_name.text() or self.db.tree.new_cat()
        self.view.new_cat_name.setText(self.curCat)
        fltr[self.db.CATEGORY] = self.curCat

        if all([fltr[i] for i in fltr
                if i in [self.db.COL_NAME, self.db.FILTER, self.db.CATEGORY]]):

            self.db.splitAdd(fltr, parent)

            if self.curCat != cfg.GRANDPA:
                self.view.also_not_cat.setChecked(False)
                self.fill_tree()
            else:
                self.fill('split')
                # enable radio button allowing show all data
                # and un check
                self.view.also_not_cat.setEnabled(True)
                self.view.also_not_cat.setChecked(False)
                self.fill_DB(self.db.op.get(self.curCat))
            # reset filters
            self.__setFltrWidgets__({}, self.view.split_view)

    def modSplit(self, oper: "edit|remove"):
        if not self.view.split_view.currentItem():
            return
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        row_n = self.view.split_view.currentItem().row()
        col_n = cfg.split_col.index(self.db.SPLIT_N)
        oper_n = self.view.split_view.item(row_n, col_n).text()

        self.db.splitRm(oper_n)

        if oper == 'edit':
            fltr = {}
            for i in range(len(cfg.split_col)):
                fltr[cfg.split_col[i]] = self.view.split_view.item(
                    row_n, i).text()

        self.fill_tree()

        if oper == 'edit':
            self.__setFltrWidgets__(fltr, self.view.split_view)
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def __getFltrWidgets__(self, source: QtWidgets.QTableWidget) -> dict:
        """collects text from first row widgets in cat or trans table
        """
        fltr = {}
        msg = ''
        for col_i in range(source.columnCount()):
            widget = source.cellWidget(0, col_i)
            widget_name = source.horizontalHeaderItem(col_i).text()

            if isinstance(widget, QtWidgets.QComboBox):
                widget_txt = widget.currentText()
            elif isinstance(widget, QtWidgets.QLineEdit):
                if widget.hasAcceptableInput() or widget.text() == '':
                    widget_txt = widget.text()
                else:
                    widget_txt = ''
                    msg = (f'shall have value between {widget.validator().bottom()} ' +
                           f'and {widget.validator().top()} ' +
                           f'with maximum {widget.validator().decimals()} decimals')
                    msgBox = QtWidgets.QMessageBox()
                    msgBox.setText(f'<b>column {widget_name}</b> ' + msg)
                    msgBox.exec()
                    return {}
            else:
                widget_txt = ''
            fltr[widget_name] = widget_txt
        return fltr

    def __setFltrWidgets__(self, fltr, source):
        """set cat or trans widgets according to requested
        if fltr={} set to default
        """
        for col_i in range(source.columnCount()):
            widget_name = source.horizontalHeaderItem(col_i).text()
            widget = source.cellWidget(0, col_i)
            if isinstance(widget, QtWidgets.QComboBox):
                if widget_name in fltr.keys():
                    widget.setCurrentText(fltr[widget_name])
            elif isinstance(widget, QtWidgets.QLineEdit):
                if widget_name in fltr.keys():
                    widget.setText(fltr[widget_name])
                else:
                    widget.setText('')

    def markFltrColors(self):
        # mark filter by color
        # make sure we already have model in view
        mod = self.view.DB_view.model() or None
        if mod:
            if self.view.markGrp.isChecked():
                it = self.view.cat_view.selectedItems()
                if it:
                    it = it[0]
                else:
                    mod.sourceModel().markFltr()
                    return

                row_i = it.row()

                colTxt = self.view.cat_view.item(
                    row_i, cfg.cat_col.index(self.db.COL_NAME)).text()
                # iterate through all columns or chosen only
                if colTxt in cfg.cat_col_names:  # chosen column name
                    colTxt = [colTxt]
                else:
                    colTxt = cfg.cat_col_names  # we search in ALL columns

                txt = self.view.cat_view.item(
                    row_i, cfg.cat_col.index(self.db.FILTER)).text()

                mod.sourceModel().markFltr(txt, colTxt)

            else:
                mod.sourceModel().markFltr()

    def search(self):
        # mark search result with color
        # make sure we already have model in view
        mod = self.view.DB_view.model() or None
        if mod:
            txt = self.view.search.text()
            if not mod.sourceModel().findRows(txt) and txt != '':
                # found nothing, mark search QLineEdit red
                self.view.search.setStyleSheet(
                    "background-color: rgb(255, 140, 0); ")
            else:
                self.view.search.setStyleSheet(
                    "background-color: rgb(255, 255, 255); ")
            mod.sort(0, QtCore.Qt.AscendingOrder)

    # file operations
    def openDB(self, file=''):
        if not file:
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
        else:
            self.fs.setDB(file)
        if self.db.open_db(self.fs.getDB()):  # return False if fail
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
            # self.disp_statusbar('openDB')
            self.fs.writeOpt("LastDB", self.fs.getDB())
            self.fill('trans')
            self.fill('split')
            self.fill_tree()  # will set top item and filter tables accordingly
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))

    def newDB(self):
        file = QtWidgets.QFileDialog.getSaveFileName(self.view,
                                                     caption='Choose name for new SQlite3 file',
                                                     directory='',
                                                     filter=self.fs.getDB(ext=True))
        # Qt lib returning always / as path separator
        # we need system specific, because we are checking for file existence
        path = QtCore.QDir.toNativeSeparators(file[0])
        if path:
            self.fs.setDB(path)
        else:  # operation canceled
            return
        self.fs.writeOpt("LastDB", self.fs.getDB())
        self.db = DB()
        # need to reset DB view model manually because
        # empty DB is ignored by fill_DB()
        self.view.DB_view.setModel(DBmodelProxy(
            self.db.op.get(category=cfg.GRANDPA)))
        self.curCat = cfg.GRANDPA
        self.fill('trans')
        self.fill('split')
        self.fill_tree()  # will set top item and filter tables accordingly
        self.setCatInput()
        self.showMsg('Created new empty DB')

    def save_asDB(self):
        self.db.write_db(self.fs.getDB())

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
        if self.db.imp_data(self.fs.getIMP()):
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
            # self.disp_statusbar('openDB')
            self.fill('trans')
            self.fill('split')
            self.fill_tree()  # will set top item and filter tables accordingly
            self.fill_DB(self.db.op.get(self.curCat))
            self.setCatInput()
            self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
            self.view.import_info.show()
            self.view.import_status_btn.show()

            # self disable DB (open, save, new) btns
            self.view.openDB_btn.setDisabled(True)
            self.view.newDB_btn.setDisabled(True)
            self.view.save_asDB_btn.setDisabled(True)

    def imp_commit(self, sig):
        # hide import widgets
        self.view.import_info.hide()
        self.view.import_status_btn.hide()
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        self.db.imp_commit(sig)
        # refresh views
        self.fill('trans')
        self.fill('split')
        self.fill_tree()  # will set top item and filter tables accordingly
        self.fill_DB(self.db.op.get(self.curCat))
        self.setCatInput()

        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
        # self enable DB (open, save, new) btns
        self.view.openDB_btn.setDisabled(False)
        self.view.newDB_btn.setDisabled(False)
        self.view.save_asDB_btn.setDisabled(False)

    def exp(self):
        # export data to csv
        file = QtWidgets.QFileDialog.getSaveFileName(self.view,
                                                     caption='Choose CSV file',
                                                     directory='',
                                                     filter=self.fs.getCSV(ext=True))
        # Qt lib returning always / as path separator
        # we need system specific, couse we are checking for file existence
        path = QtCore.QDir.toNativeSeparators(file[0])
        if path:
            self.fs.setCSV(path)
        else:  # operation canceled
            return
        self.db.exportCSV(self.fs.getCSV())

    # GUI
    def headerSize(self, tab_i=''):
        # spread the columns, get the size, change to interactive mode and set size manualy
        header = self.view.DB_view.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        for i in range(header.count()):
            # do not set the size on hidden columns
            if not header.isSectionHidden(i):
                size = header.sectionSize(i)
                header.setDefaultSectionSize(size)
        header.setSectionResizeMode(QtWidgets.QHeaderView.Interactive)

    def DBforTab(self):
        """adjust filter view according to active tab\n
        also show "new category" name QLineEdit only for proper tabs
        called by signal when tab changed
        """
        tabs = {0: 'data',
                1: 'trans',
                2: 'cat',
                3: 'split'}
        tab = self.view.tabMenu.currentIndex()

        self.view.new_cat_name.blockSignals(True)
        if tabs[tab] == 'cat' and self.curCat == cfg.GRANDPA or tabs[tab] == 'split':  # show QLineEdit
            self.view.new_cat_name.show()
            self.view.new_cat_name.setText('')
            self.view.label_3.show()
        else:
            self.view.new_cat_name.hide()
            self.view.label_3.hide()

        if tabs[tab] in ['cat', 'split', 'trans']:
            self.fill(dbTxt=tabs[tab])
        self.view.new_cat_name.blockSignals(False)

    def setStatusBar(self):
        # add text messages
        self.view.msg = QtWidgets.QLabel()
        self.view.msg.setStyleSheet(
            "background-color: rgb(255,202,11);")  # orangish
        self.view.statusbar.addWidget(self.view.msg)
        # add progress meter
        self.view.prog = QtWidgets.QProgressBar()
        sizePolicy = QtWidgets.QSizePolicy(
            QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        self.view.prog.setSizePolicy(sizePolicy)
        self.view.prog.setMinimumSize(QtCore.QSize(20, 10))
        self.view.prog.setRange(0, 100)
        self.view.statusbar.addPermanentWidget(self.view.prog)

    def showMsg(self, msg):
        # messages from DB
        if msg:
            self.view.msg.setText(msg)
            self.callDelay(self.resetMsg, 30000)

    def resetMsg(self):
        self.view.msg.setText('')

    def alsoNoCatData(self):
        mod = self.view.DB_view.model() or None
        if mod:
            if self.view.also_not_cat.isChecked():
                mod.sourceModel().markBlueAddRows(self.db.op.get(cfg.GRANDPA))
                self.search()
                self.fill_group()
            else:
                mod.sourceModel().markBlueAddRows(None)
                self.search()
                self.fill_group()

    def showCalendar(self):
        """show calendar widget when date column clicked
        """
        it = self.view.split_view.currentIndex()
        widget = self.view.split_view.focusWidget()
        if not it or not isinstance(widget, QtWidgets.QLineEdit):
            return

        if cfg.split_col_type[it.column()] == 'TIMESTAMP' and it.row() == 0:
            cal = GUICalendar()
            if cal.exec_():
                widget.setText(cal.dat)

    def readStat(self) -> {}:
        """read gui status
        """
        self.fs.writeOpt(op='visColumns', val=self.visCol)
        # self.fs.writeOpt(op='winSize', val=self.view.size())

    def setStat(self):
        """set gui status
        """
        self.visCol = self.fs.getOpt('visColumns')
        if not self.visCol:
            self.visCol = cfg.op_col
            self.fs.writeOpt(op='visColumns', val=self.visCol)

    def plot(self):
        GUIPlot_ctrl(self.db)
