#!/home/mi/.backup/venv/banking/bin/python
"""
Start banking application. Loads PyQT5 GUI.
project structure:
./main.py     - this file. Make sure you point
                to right environment after shebang
                if you want to start from command line
./modules.py  - have FileSystem() class to manage db file
./db.py       - classes to manage all operation on db
./gui.py      - GUI logic and description.
                Window definitions are in qt_gui folder
./models.py   - data models used by GUI
"""
import sys
from PyQt5.QtWidgets import QApplication
from qt_gui.ctrl_mainWin import GUIMainWin_ctrl

app = QApplication([])
GUIMainWin_ctrl()
sys.exit(app.exec())
