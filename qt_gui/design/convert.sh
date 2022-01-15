#!/bin/bash
/home/mi/.backup/venv/banking/bin/pyuic5 main_window.ui -o main_window.py --import-from=qt_gui.design
/home/mi/.backup/venv/banking/bin/pyuic5 import.ui -o import.py --import-from=qt_gui.design
/home/mi/.backup/venv/banking/bin/pyuic5 calendar.ui -o calendar.py --import-from=qt_gui.design
/home/mi/.backup/venv/banking/bin/pyuic5 split.ui -o split.py --import-from=qt_gui.design
/home/mi/.backup/venv/banking/bin/pyuic5 plot.ui -o plot.py --import-from=qt_gui.design

/home/mi/.backup/venv/banking/bin/pyrcc5 res.qrc -o res_rc.py 