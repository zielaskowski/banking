from PyQt5 import QtWidgets, QtGui
from qt_gui.gui_views import GUILog
from qt_gui.gui_widgets import moduleDelay

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

