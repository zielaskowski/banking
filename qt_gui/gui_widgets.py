"""
re-define (modify) standard widgets
"""
from PyQt5 import QtWidgets, QtCore, QtGui
from qt_gui.design.calendar import Ui_calendar
from pandas import Timestamp
from typing import Union

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
        self.dat = '*'
        self.accept()


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


class calendarQDateEdit(QtWidgets.QDateEdit):
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self.isDisabled = False

    def getDateStr(self) -> str:
        """return date in string format
        """
        if self.isDisabled:
            return '*'
        else:
            return self.date().toString(QtCore.Qt.ISODate)

    def setDisabled(self, isDisabled: bool) -> None:
        self.isDisabled = isDisabled
        if isDisabled:
            self.cal = '*'
            self.setStyleSheet("background-color: rgb(146, 146, 146); "
                               "color: rgb(200,200,200); "
                               "selection-background-color: rgb(146, 146, 146); "
                               "selection-color: rgb(200, 200, 200);")
        else:
            self.cal = self.date().toString(QtCore.Qt.ISODate)
            self.setStyleSheet("background-color: rgb(255, 255, 255); "
                               "color: rgb(0,0,0); "
                               "selection-background-color: rgb(61, 174, 233); "
                               "selection-color: rgb(0, 0, 0);")

    def setStrDate(self, date: Union[str, Timestamp]) -> None:
        if isinstance(date, Timestamp):
            date = date.strftime('%Y-%m-%d')
        self.setDate(QtCore.QDate.fromString(
            date, QtCore.Qt.ISODate))

    def mousePressEvent(self, ev: QtGui.QMouseEvent) -> None:
        cal = GUICalendar()
        if cal.exec_():
            self.blockSignals(True)
            if cal.dat == '*':
                self.setDisabled(True)
            else:
                self.setDisabled(False)
                self.setStrDate(cal.dat)
            self.blockSignals(False)
            self.dateChanged.emit(self.date())


class calendarQLineEdit(QtWidgets.QLineEdit):
    """reimplement QLineWidget to add click event
    """

    def __init__(self):
        super().__init__()
        self.disabled = False

    def setDisabled(self, isDisable: bool) -> None:
        self.disabled = isDisable

    def mousePressEvent(self, ev: QtGui.QMouseEvent) -> None:
        if not self.disabled:
            cal = GUICalendar()
            if cal.exec_():
                self.setText(cal.dat)
        return super().mousePressEvent(ev)
