#!/home/mi/.backup/venv/banking/bin/python
"""
Start banking application. Loads PyQT5 GUI.
project structure:
./main.py     - this file. Make sure you point
                to right environment after shebang
                if you want to start from command line
        args:
            plot - goes directly to plot window
./modules.py  - have FileSystem() class to manage db file
./db.py       - classes to manage all operation on db
./models.py   - data models used by GUI to render tables
./qt_gui/design         - folder with definition of all windows
                          design with qt5designer, convert with convert.sh
./qt_gui/views.py       - view classes initilizing windows in ./qt_gui/design
./qt_gui/widgets.py     - extended widgets
./qt_gui/ctrl_stats.py  - control class for statistic window
./qt_gui/ctrl_plot.py   - control class for plot window (analitycs charts)
./qt_gui/ctrl_mainWin.py- control class for main window
./qt_gui/ctrl_log.py    - control class for log window
"""
import sys
from PyQt5.QtWidgets import QApplication
from qt_gui.ctrl_mainWin import GUIMainWin_ctrl

app = QApplication([])
GUIMainWin_ctrl(sys.argv)
sys.exit(app.exec())
