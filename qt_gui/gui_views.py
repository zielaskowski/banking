"""
define view classes
control classes shall be named the same +suffix 'ctrl'
small classes are controled here
"""

from PyQt5 import QtWidgets

from qt_gui.design.main_window import Ui_banking
from qt_gui.design.stat import Ui_statistics
from qt_gui.design.log import Ui_log
from qt_gui.design.cat_tree import Ui_selTree
from qt_gui.design.plot import Ui_plot_win
import qt_gui.gui_widgets #Ui_classes use this


class GUISelTree(QtWidgets.QDialog, Ui_selTree):
    """Display modal dialog with tree copy to select a category
    """

    def __init__(self, tree: QtWidgets.QTreeWidget) -> None:
        super().__init__()
        self.setupUi(self)
        self.cat = ''  # store selected category
        self.tree_db.setModel(tree.model())
        # hide columns
        self.tree_db.setHeaderHidden(True)
        # expand and resize
        self.tree_db.expandAll()
        self.tree_db.resizeColumnToContents(0)
        self.connect()

    def connect(self):
        self.cancel_btn.clicked.connect(self.stop)
        self.tree_db.clicked.connect(self.selCat)

    def selCat(self):
        self.cat = self.tree_db.selectedIndexes()[0].data()
        self.accept()

    def stop(self):
        self.reject()


class GUILog(QtWidgets.QDialog, Ui_log):
    """
    display history of messages (log)
    """

    def __init__(self, txt):
        super().__init__()
        self.setupUi(self)
        self.logText.setReadOnly(True)
        self.logText.setText(txt)


class GUIStats(QtWidgets.QDialog, Ui_statistics):
    """
    display window with data statistics
    also alows delete selected import
    """

    def __init__(self):
        super().__init__()
        self.setupUi(self)


class GUIPlot(QtWidgets.QMainWindow, Ui_plot_win):
    """
    display separate window for visualizing
    and pass controll to logic class
    """

    def __init__(self):
        super().__init__()
        self.setupUi(self)



class GUIMainWin(QtWidgets.QMainWindow, Ui_banking):
    """
    create main window object
    and pass controll to logic class
    """

    def __init__(self):
        super().__init__()
        self.setupUi(self)
