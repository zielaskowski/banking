import sys
from gui import GUIMainWin, GUIMainWin_ctrl
from PyQt5.QtWidgets import QApplication

app = QApplication([])
view = GUIMainWin()
view.show()
GUIMainWin_ctrl(view)
sys.exit(app.exec())