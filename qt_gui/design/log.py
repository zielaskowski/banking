# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'log.ui'
#
# Created by: PyQt5 UI code generator 5.15.4
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_log(object):
    def setupUi(self, log):
        log.setObjectName("log")
        log.resize(516, 327)
        self.verticalLayout = QtWidgets.QVBoxLayout(log)
        self.verticalLayout.setObjectName("verticalLayout")
        self.logText = QtWidgets.QTextEdit(log)
        self.logText.setEnabled(True)
        self.logText.setLineWrapMode(QtWidgets.QTextEdit.NoWrap)
        self.logText.setObjectName("logText")
        self.verticalLayout.addWidget(self.logText)

        self.retranslateUi(log)
        QtCore.QMetaObject.connectSlotsByName(log)

    def retranslateUi(self, log):
        _translate = QtCore.QCoreApplication.translate
        log.setWindowTitle(_translate("log", "Dialog"))
