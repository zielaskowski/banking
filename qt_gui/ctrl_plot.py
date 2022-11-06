"""
controll logic for plot window
"""
from typing import Union, List
from PyQt5 import QtCore
import plotly.express as pex
from db import DB
from qt_gui.gui_views import GUIPlot
from qt_gui.gui_widgets import moduleDelay


class GUIPlot_ctrl(QtCore.QObject, moduleDelay):
    """
    logic for plot window
    """

    def __init__(self, db: DB):
        self.view = GUIPlot()
        super().__init__(self.view)
        self.view.show()
        self.db = db
        self.startDate, self.endDate = self.str2Qdate(
            db.dataRange())
        self.view.startDate.setSpecialValueText('')
        self.view.endDate.setSpecialValueText('')
        # set min max dates
        self.view.startDate.setDate(self.startDate)
        self.view.startDate.setDateRange(
            self.startDate,
            self.endDate)
        self.view.endDate.setDate(self.endDate)
        self.view.endDate.setDateRange(
            self.startDate,
            self.endDate)
        self.view.startDateSlide.setSlider(
            start=self.startDate,
            end=self.endDate)
        self.view.endDateSlide.setSlider(
            start=self.startDate,
            end=self.endDate,
            val=self.endDate)
        # relate widgets between each other
        self.view.startDate.setMaxWidget(self.view.endDate)
        self.view.endDate.setMinWidget(self.view.startDate)
        self.view.startDateSlide.setMaxValWidget(self.view.endDateSlide)
        self.view.endDateSlide.setMinValWidget(self.view.startDateSlide)

        self.connectSignals()
        self.plot_test()

    def str2Qdate(self, date: Union[List, str]) -> QtCore.QDate:
        """convert string to QDate

        Args:
            date (str): date string in ISO format yyyy-mm-dd
        """
        if isinstance(date, str):
            date = [date]
        return [QtCore.QDate.fromString(d, QtCore.Qt.ISODate) for d in date]

    def Qdate2str(self, date: QtCore.QDate) -> str:
        """convert QDate to string 

        Args:
            date (str): date in QDate format
        """
        return date.toString(QtCore.Qt.ISODate)

    def connectSignals(self):
        self.view.startDate.dateChanged.connect(self.changeDate)
        self.view.endDate.dateChanged.connect(self.changeDate)
        self.view.startDateSlide.valueChanged.connect(
            lambda: self.view.startDate.setDate(self.view.startDateSlide.getDate()))
        self.view.endDateSlide.valueChanged.connect(
            lambda: self.view.endDate.setDate(self.view.endDateSlide.getDate(40)))

    def changeDate(self):
        # self.startDate = self.view.startDate.date()
        # self.endDate = self.view.endDate.date()
        # self.view.startDateSlide.update(self.startDate)
        # self.view.endDateSlide.update(self.endDate)
        # self.setDelay(self.plot_test, 800)
        pass

    def plot_test(self):
        df = pex.data.iris()
        fig = pex.scatter(df, x="sepal_width", y="sepal_length")
        self.view.plot.setHtml(fig.to_html(include_plotlyjs='cdn'))
