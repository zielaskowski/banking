"""
control logic for stats window
"""

import pandas
import plotly as pex
from bs4 import BeautifulSoup as bs
from PyQt5 import QtCore, QtGui, QtWidgets
from db import DB
from qt_gui.gui_classes import GUIStats


class GUIStats_ctrl(QtCore.QObject):
    """
    logic for stat window
    """

    def __init__(self, db: DB) -> None:
        self.view = GUIStats()
        super().__init__(self.view)
        self.db = db  # reference to class logic of db
        self.redraw = False  # if we deleted, parent can check and redraw
        # connect signals
        self.connectSignals()
        # fill banks
        self.bankRe()

    def connectSignals(self):
        self.view.del_btn.clicked.connect(self.rmImp)
        self.view.bankList_combo.currentTextChanged.connect(self.redrawAll)

    def bankRe(self):
        banks = ["ALL"] + self.db.dataBanks()
        self.view.bankList_combo.clear()
        self.view.bankList_combo.insertItems(0, banks)

    def redrawAll(self, bank):
        self.drawStats(bank)
        self.plotStats(bank)

    def drawStats(self, bank: str):
        """
        display data statistics
        highlight bank
        """
        statDb = {'bank': [], 'start_date': [], 'end_date': []}
        for b in self.db.dataBanks():
            statDb['bank'].append(b)
            dats = self.db.dataRange(b)
            statDb['start_date'].append(dats[0])
            statDb['end_date'].append(dats[1])
        statDb = pandas.DataFrame(statDb)
        statTxt = statDb.to_html()
        # color row with selected bank
        statTxt = bs(statTxt, 'html.parser')
        row = statTxt.find('td', string=bank)
        if row:
            row = row.find_parent('tr')
            row['style'] = 'background: green'

        self.view.statTab.setText(str(statTxt))

    def plotStats(self, bank: str):
        """
        plot histogram: no of operations vs day
        highlight data for selected bank
        """
        fig = pex.graph_objs.Figure()
        for b in self.db.dataBanks():
            f = self.db.dataHist(b)
            if b == bank:
                f.data[0].marker.color = 'red'
            else:
                f.data[0].marker.color = 'green'
            f.data[0].xbins.size = 604800000  # bin weekly (in ms)
            fig.add_trace(f.data[0])
        # remove all layouts, etc
        fig.update_traces(showlegend=False)\
            .update_yaxes(visible=False)\
            .update_layout(margin={'l': 5, 'r': 5, 't': 5, 'b': 5})
        self.view.statPlot.setHtml(fig.to_html(include_plotlyjs='cdn'))

    def rmImp(self):
        """remove data for selected bank
        """
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.WaitCursor))
        bank = self.view.bankList_combo.currentText()
        self.db.impRm(bank)
        self.bankRe()
        self.redraw = True
        self.view.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
