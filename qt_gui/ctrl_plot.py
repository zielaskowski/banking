"""
controll logic for plot window
"""

import imp
from PyQt5 import QtCore
import plotly.express as pex
from db import DB
from qt_gui.gui_classes import GUIPlot


class GUIPlot_ctrl(QtCore.QObject):
    """
    logic for plot window
    """

    def __init__(self, db: DB):
        self.view = GUIPlot()
        super().__init__(self.view)
        self.view.show()
        self.db = db
        self.plot_test()

    def plot_test(self):
        df = pex.data.iris()
        fig = pex.scatter(df, x="sepal_width", y="sepal_length")
        self.view.plot.setHtml(fig.to_html(include_plotlyjs='cdn'))
