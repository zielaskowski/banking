"""
define windows objects classes
control classes shall be named the same +suffix 'ctrl'
"""
from PyQt5 import QtWidgets, QtCore, QtGui
from qt_gui.design.calendar import Ui_calendar
from qt_gui.design.main_window import Ui_banking
from qt_gui.design.plot import Ui_plot_win
from qt_gui.design.stat import Ui_statistics
from qt_gui.design.log import Ui_log


class GUILog(QtWidgets.QDialog, Ui_log):
    """
    display history of messages (log)
    """

    def __init__(self, txt):
        super().__init__()
        self.setupUi(self)
        self.logText.setReadOnly(True)
        self.logText.setText(txt)


class moduleDelay():
    """
    inherit from QtCore.QObject, so can be used itself alone by GUIMainWindow
    """

    def __init__(self):
        super().__init__()
        self.timer = QtCore.QTimer()

    def setDelay(self, method, delay):
        # call when you want delay signal
        # can be one timer only :(

        try:
            self.timer.timeout.disconnect(method)
        except TypeError:
            pass

        self.timer.setSingleShot(True)
        self.timer.timeout.connect(method)
        self.timer.start(delay)


class statusQLabel(QtWidgets.QLabel, moduleDelay):
    """
    Reimplement QLabel, so to catch mouse click
    Launch modal dialog after click (log)
    """

    def __init__(self, fsRef: object):
        super().__init__()
        self.fs = fsRef

    def setMsg(self, txt):
        self.setDelay(self.clearMsg, 30000)
        self.setStyleSheet(
            "background-color: rgb(255,202,11);")  # orangish
        self.setText(txt)

    def clearMsg(self):
        self.setStyleSheet(
            "background-color: rgb(239,240,241);")  # no background
        self.setText('click to see log....')

    def mousePressEvent(self, ev: QtGui.QMouseEvent) -> None:
        log = GUILog(self.fs.getMsg(all=True))
        log.exec_()
        return super().mousePressEvent(ev)


class GUIStats(QtWidgets.QDialog, Ui_statistics):
    """
    display window with data statistics
    also alows delete selected import
    """

    def __init__(self):#, text):
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


class GUICalendar(QtWidgets.QDialog, Ui_calendar):
    """
    display small calendar widget (dialog) to select dates
    also basic logic (accept dialog)
    selected date is in self.dat
    """

    def __init__(self):
        super().__init__()
        self.dat = object
        self.setupUi(self)
        self.calendarWidget.clicked.connect(self.selDate)
        self.splitEmptyDate.clicked.connect(self.noDate)

    def selDate(self, data):
        self.dat = data.toString(QtCore.Qt.ISODate)
        self.accept()

    def noDate(self):
        self.dat = '-'
        self.accept()


class GUIMainWin(QtWidgets.QMainWindow, Ui_banking):
    """
    create main window object
    and pass controll to logic class
    """

    def __init__(self):
        super().__init__()
        self.setupUi(self)
