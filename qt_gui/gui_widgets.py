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
    delay execution of a method
    inherit from QtCore.QObject, so can be used by classes inherit from there
    """

    def __init__(self):
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
        self.setSpecialValueText('any')  # used when select to not limit date
        self.setMinimumDate(QtCore.QDate.fromString(
            '1901-01-01', QtCore.Qt.ISODate))
        self.isDisabled = False
        self.setWidgetColors()

    def setMaxWidget(self, widget: QtWidgets.QDateEdit):
        def updWidget():
            self.setMaximumDate(widget.date())
            if widget.date() < self.date():
                self.blockSignals(True)
                self.setDate(widget.date())
                self.blockSignals(False)
        widget.dateChanged.connect(updWidget)

    def setMinWidget(self, widget: QtWidgets.QDateEdit):
        def updWidget():
            self.setMinimumDate(widget.date())
            if widget.date() > self.date():
                self.blockSignals(True)
                self.setDate(widget.date())
                self.blockSignals(False)
        widget.dateChanged.connect(updWidget)

    def setLimits(self, start: QtCore.QDate, end: QtCore.QDate) -> None:
        self.setMaximumDate(end)
        self.setMinimumDate(start)

    def setMaximumDate(self, end: QtCore.QDate) -> None:
        return super().setMaximumDate(end.addDays(+1))

    def setMinimumDate(self, min: QtCore.QDate) -> None:
        return super().setMinimumDate(min.addDays(-1))

    def getDateStr(self) -> str:
        """return date in string format
        """
        if self.isDisabled:
            return '*'
        else:
            return self.date().toString(QtCore.Qt.ISODate)

    def setDisabled(self, isDisabled: bool) -> None:
        self.isDisabled = isDisabled
        self.setWidgetColors()

    def setDate(self, date: Union[str, Timestamp, QtCore.QDate]) -> None:
        if isinstance(date, Timestamp):
            date = date.strftime('%Y-%m-%d')
        if isinstance(date, str):
            date = QtCore.QDate.fromString(date, QtCore.Qt.ISODate)
        self.setWidgetColors()
        #self.blockSignals(True)
        super().setDate(date)
        #self.blockSignals(False)

    def mousePressEvent(self, ev: QtGui.QMouseEvent) -> None:
        if not self.isDisabled:
            cal = GUICalendar()
            cal.calendarWidget.setDateRange(
                self.minimumDate(), self.maximumDate())
            cal.calendarWidget.setSelectedDate(self.date())
            if cal.exec_():
                if cal.dat == '*':
                    # outside limit so special text will be displayed
                    self.setDate('1900-01-01')
                else:
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


class calendarQSlider(QtWidgets.QSlider):
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)
        self.startDate = QtCore.QDate()

    # def setMinWidget(self, widget: QtWidgets.QSlider):
    #     widget.valueChanged.connect(lambda:self.setMaxVal)

    # def setMaxWidget(self, widget: QtWidgets.QSlider):
    #     pass

    def setSlider(self, start: QtCore.QDate, end: QtCore.QDate, val=None):
        """set limits for slider based on provided dates
        tick is one month, page is year
        Args:
            start (str): start date as str ISO format yyy-mm-dd
            end (str): end date as str ISO format yyy-mm-dd
        """
        self.startDate = start
        self.setMinimum(0)
        self.setMaximum(self.months(end) - self.months(start))
        if val:
            self.update(val)

    def months(self, date: QtCore.QDate) -> int:
        """calculate total number of months since Jesus birth
        """
        return (date.year() * 12 + date.month())

    def update(self, date: QtCore.QDate) -> None:
        """set position of the slider based on provided date
        """
        self.blockSignals(True)
        self.setValue(self.months(date) - self.months(self.startDate))
        self.blockSignals(False)

    def getDate(self) -> QtCore.QDate:
        """return slider position as QDate
        """
        return self.startDate.addMonths(self.value())
