#!/bin/bash
/home/mi/.backup/venv/bin/pyuic5 main_window.ui -o main_window.py --import-from=qt_gui
/home/mi/.backup/venv/bin/pyuic5 import.ui -o import.py --import-from=qt_gui

/home/mi/.backup/venv/words/bin/pyrcc5 resources.qrc -o resources_rc.py 