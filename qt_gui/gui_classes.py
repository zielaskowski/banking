"""
define windows objects classes
control classes shall be named the same +suffix 'ctrl'
"""
from PyQt5 import QtWidgets
from qt_gui.design.calendar import Ui_calendar
from qt_gui.design.main_window import Ui_banking
from qt_gui.design.plot import Ui_plot_win


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
        self.dat = data.toString()
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
