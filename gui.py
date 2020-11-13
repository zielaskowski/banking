from PyQt5 import QtWidgets
from qt_gui.main_window import Ui_banking


class App(QtWidgets.QMainWindow, Ui_banking):
    def __init__(self):
        super().__init__()
        self.setupUi(self)


app = QtWidgets.QApplication([])
application = App()
application.show()
app.exec()
