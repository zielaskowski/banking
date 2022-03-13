import sys
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QApplication
from qt_gui.plot import Ui_plot_win
import plotly.express as px


class main_win(QMainWindow, Ui_plot_win):
    def __init__(self) -> None:
        super().__init__()
        self.setupUi(self)
        self.plot_chart()

    def plot_chart(self):
        df = px.data.iris()  # iris is a pandas DataFrame
        fig = px.scatter(df, x="sepal_width", y="sepal_length")
        self.plot.setHtml(fig.to_html(include_plotlyjs='cdn'))


app = QApplication([])
view = main_win()
view.show()
sys.exit(app.exec())
