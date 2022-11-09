"""
controll logic for plot window
"""
from typing import Union, List
import pandas as pd
from PyQt5 import QtCore
import plotly.express as pex
import plotly.graph_objects as go
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
        self.cat = [cat for cat, lev
                    in self.db.tree.allChild().items()
                    if lev == 0][0]
        self.lev = 0

        self.view.startDate.setSpecialValueText('')
        self.view.endDate.setSpecialValueText('')
        # set min max dates
        self.view.startDate.setText(self.startDate)
        self.view.startDate.setDateRange(
            self.startDate,
            self.endDate)
        self.view.endDate.setText(self.endDate)
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
        # fill combo box
        [self.view.plotLevel.addItem(cat) for cat, lev
            in self.db.tree.allChild(catStart=self.cat).items()
            if lev == self.lev+1]
        self.view.plotLevel.addItem('--back--')

        self.connectSignals()
        self.plot()

    def str2Qdate(self, date: Union[List, str]) -> QtCore.QDate:
        """convert string to QDate

        Args:
            date (str): date string in ISO format yyyy-mm-dd
        """
        if isinstance(date, str):
            date = [date]
        return [QtCore.QDate.fromString(d, QtCore.Qt.ISODate) for d in date]

    def Qdate2str(self, date: QtCore.QDate) -> str:
        """convert QDate to pandas timestamp 

        Args:
            date (Timestamp from pandas): date in QDate format
        """
        return date.toString(QtCore.Qt.ISODate)

    def connectSignals(self):
        self.view.startDate.dateChanged.connect(self.changeDate)
        self.view.endDate.dateChanged.connect(self.changeDate)
        self.view.startDateSlide.valueChanged.connect(self.changeDateSlide)
        self.view.endDateSlide.valueChanged.connect(self.changeDateSlide)
        self.view.plotLevel.currentTextChanged.connect(self.plot)

    def changeDateSlide(self, a):
        # set date widgets to match slider
        # the order is important, endDate will change startDate maxDate
        self.view.endDate.setText(self.view.endDateSlide.getDate(31))
        self.view.startDate.setText(self.view.startDateSlide.getDate())

    def changeDate(self, a):
        # change slider widgets to match data
        self.view.startDateSlide.update(self.view.startDate.date())
        self.view.endDateSlide.update(self.view.endDate.date())

        # changeDate is trigered alwas (for slider also)
        self.startDate = self.view.startDate.date()
        self.endDate = self.view.endDate.date()
        self.setDelay(self.plot, 800)

    def plot(self):
        selCat = self.view.plotLevel.currentText()
        ignoreCat = ['wlasne', 'dochody']

        kidsCats = self.db.tree.flatChild(catStart=selCat, ignoreCat=ignoreCat)

        # categories
        df = self.db.op.get(category=list(kidsCats.keys()),
                            startDate=self.Qdate2str(self.startDate),
                            endDate=self.Qdate2str(self.endDate),
                            cols=[self.db.DATA_OP,
                                  self.db.AMOUNT,
                                  self.db.CATEGORY])
        df.loc[:, 'parCat'] = [kidsCats[i] for i in df.loc[:, self.db.CATEGORY]]

        # bilans
        catBilans = list(self.db.tree.allChild().keys())
        [catBilans.remove(c) for c in ignoreCat if c in catBilans]
        df_bilans = self.db.op.get(category=catBilans,
                                   startDate=self.Qdate2str(self.startDate),
                                   endDate=self.Qdate2str(self.endDate),
                                   cols=[self.db.DATA_OP,
                                         self.db.AMOUNT,
                                         self.db.CATEGORY])
        df_bilans.loc[:, 'parCat'] = df_bilans.loc[:, self.db.CATEGORY]

        # group
        timeGrp = pd.Grouper(key=self.db.DATA_OP, freq='M')
        df = df.groupby([timeGrp, 'parCat']).sum()
        df_bilans = df_bilans.groupby(timeGrp).sum()
        df.reset_index(inplace=True)
        df_bilans.reset_index(inplace=True)

        fig = go.Figure()
        fig.add_traces(list(pex.bar(df,
                                x=self.db.DATA_OP,
                                y=self.db.AMOUNT,
                                color='parCat',
                                barmode='group').select_traces()))

        fig.add_traces(list(pex.line(df_bilans,
                                    x=self.db.DATA_OP,
                                    y=self.db.AMOUNT).select_traces()))

        self.view.plot.setHtml(fig.to_html(include_plotlyjs='cdn'))
