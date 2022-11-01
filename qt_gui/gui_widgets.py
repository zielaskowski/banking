"""
re-define (modify) standard widgets
"""
from PyQt5 import QtWidgets, QtCore, QtGui
from qt_gui.design.calendar import Ui_calendar
from pandas import Timestamp
from typing import Union


class Style():
    """set colors on widgets
    """
    def __init__(self) -> None:
        self.disabledStyle = "background-color: rgb(146, 146, 146); \
                            color: rgb(200,200,200); \
                            selection-background-color: rgb(146, 146, 146); \
                            selection-color: rgb(200, 200, 200);"
        self.activeStyle = "background-color: rgb(26, 255, 14); \
                            color: rgb(0,0,0); \
                            selection-background-color: rgb(255, 255, 255); \
                            selection-color: rgb(0, 0, 0);"

    def setWidgetColors(self) -> None:
        if self.isDisabled:
            self.setStyleSheet(self.disabledStyle)
        else:
            self.setStyleSheet(self.activeStyle)


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


class calendarQDateEdit(QtWidgets.QDateEdit, Style):
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self.setSpecialValueText('any')  # used when set to not limit date
        self.setMinimumDate(QtCore.QDate.fromString(
            '1901-01-01', QtCore.Qt.ISODate))
        self.isDisabled = False
        self.setWidgetColors()

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
        else:
            self.cal = self.date().toString(QtCore.Qt.ISODate)
        self.setWidgetColors()

    def setDate(self, date: Union[str, Timestamp, QtCore.QDate]) -> None:
        if isinstance(date, Timestamp):
            date = date.strftime('%Y-%m-%d')
        if isinstance(date, str):
            date = QtCore.QDate.fromString(date, QtCore.Qt.ISODate)
        self.setWidgetColors()
        return super().setDate(date)

    def mousePressEvent(self, ev: QtGui.QMouseEvent) -> None:
        if not self.isDisabled:
            cal = GUICalendar()
            if cal.exec_():
                self.blockSignals(True)
                if cal.dat == '*':
                    self.setDate('1900-01-01')
                else:
                    self.setDisabled(False)
                    self.setDate(cal.dat)
                self.blockSignals(False)
        self.dateChanged.emit(self.date())


class disQLineEdit(QtWidgets.QLineEdit, Style):
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self.isDisabled = False
        self.setWidgetColors()

    def setDisabled(self, isDisabled: bool) -> None:
        self.isDisabled = isDisabled
        self.setWidgetColors()
        return super().setDisabled(isDisabled)


class disQComboBox(QtWidgets.QComboBox, Style):
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self.isDisabled = False
        self.setWidgetColors()

    def setDisabled(self, isDisabled: bool) -> None:
        self.isDisabled = isDisabled
        self.setWidgetColors(self)
        return super().setDisabled(isDisabled)
