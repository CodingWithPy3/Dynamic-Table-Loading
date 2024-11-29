import os.path

from PyQt5.QtWidgets import QMainWindow, QFrame, QMenu, QMenuBar, QVBoxLayout, QAction, QWidget, \
    QPushButton, QApplication, QHBoxLayout, QLabel, QSpacerItem, QSizePolicy, QTextEdit, QCheckBox, QFileDialog, \
    QMessageBox, qApp, QDialog, QLineEdit
from PyQt5.QtCore import Qt, QRect, QMetaObject, QCoreApplication, QThread, pyqtSignal
from PyQt5.QtGui import QPixmap, QTextCursor, QTextCharFormat, QFont, QColor
from time import sleep
import secureInfo

class QueryExecutor(QThread):
    """
    # Upload the DataFrame to the existing Snowflake table
    # The table should already exist in the schema
    success, nchunks, nrows, _ = write_pandas(conn, df, snowflake_table)

    if success:
        print(f"Successfully loaded {nrows} rows into {snowflake_schema}.{snowflake_table}")
    else:
        print("Failed to load data into Snowflake")

    """

    query_completion = pyqtSignal(int)
    folder_error = pyqtSignal()

    def __init__(self):
        super(QueryExecutor, self).__init__()
        self.TotalQuery = 3
        self.QueryNo = 0

    def run(self):
        print('In Query Executer class')

        try:
            EncryptedPwd = secureInfo.read_from_file(file_path)
            print(EncryptedPwd)
            decryptedPwd = secureInfo.decrypt_string(encrypted_data=EncryptedPwd)
            print(decryptedPwd)

        except:
            print("Except block")
            self.folder_error.emit()

        else:
            for _ in range(self.TotalQuery):
                self.QueryNo += 1
                # self.query_completion.emit(1)
                # print(f"Executing query {self.QueryNo} out of 3")
                sleep(4)  # Simulating a delay for the query execution
                self.query_completion.emit(self.QueryNo)
                sleep(1)


class Logger(QThread):
    message = pyqtSignal(str)

    def __init__(self):
        super(Logger, self).__init__()

    def run(self):
        while not self.isInterruptionRequested():
            sleep(2)
            self.message.emit("Executing the query...")
            # print("Executing the query logger class...")


class UpdatePasswordDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle('Update Password')
        self.setGeometry(100, 100, 300, 150)
        self.setFixedSize(400, 150)
        layout = QVBoxLayout()

        self.label = QLabel('Enter new password:')
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)

        self.update_button = QPushButton('Update')
        self.update_button.clicked.connect(self.update_password)

        layout.addWidget(self.label)
        layout.addWidget(self.password_input)
        layout.addWidget(self.update_button)

        self.setLayout(layout)

    def update_password(self):
        if not os.path.exists(file_path):
            os.mkdir(file_path.split('/')[0])

        newPassword = self.password_input.text()
        if len(newPassword) != 0:
            encryptedPwd = secureInfo.encrypt_string(newPassword)
            success = secureInfo.save_to_file(file_path, encryptedPwd)
            if success:
                QMessageBox.Information(self, "Update Password", "Password Updated Successfully")
            else:
                QMessageBox.warning(self, "Warning", "Unable to update the Password")

            # self.accept()  # Close the dialog
            pass
        else:
            QMessageBox.warning(self, "Warning", "Please Enter the Password")


class Ui_MainWindow(QMainWindow):
    def __init__(self, parent=None):
        super(Ui_MainWindow, self).__init__(parent)
        self.setObjectName("MainWindow")
        self.resize(1000, 800)

        self.LoadExcelFilePath = ''

        self.centralwidget = QWidget(self)
        self.centralwidget.setObjectName("centralwidget")
        self.CentralWidget_VL = QVBoxLayout(self.centralwidget)
        self.CentralWidget_VL.setObjectName("CentralWidget_VL")

        self.MainFrame = QFrame(self.centralwidget)
        self.MainFrame.setFrameShape(QFrame.StyledPanel)
        self.MainFrame.setFrameShadow(QFrame.Raised)
        self.MainFrame.setObjectName("MainFrame")
        self.MainFrame_HL = QHBoxLayout(self.MainFrame)
        self.MainFrame_HL.setObjectName("MainFrame_HL")

        # --------------------------------------------------------------------------------------------------- #
        # --------------------------------------- LEFT SIDE WIDGETS ----------------------------------------- #
        # --------------------------------------------------------------------------------------------------- #

        self.LeftFrame = QFrame(self.MainFrame)
        self.LeftFrame.setFrameShape(QFrame.StyledPanel)
        self.LeftFrame.setFrameShadow(QFrame.Raised)
        self.LeftFrame.setObjectName("LeftFrame")
        self.LeftFrameVL = QVBoxLayout(self.LeftFrame)
        self.LeftFrameVL.setObjectName("LeftFrameVL")

        self.LoadFileBtn = QPushButton(self.LeftFrame)
        self.LoadFileBtn.setObjectName("LoadFileBtn")
        self.LoadFileBtn.setProperty('clicked', False)
        self.LeftFrameVL.addWidget(self.LoadFileBtn, 0, Qt.AlignCenter)

        self.LeftTopFrame = QFrame(self.LeftFrame)
        self.LeftTopFrame.setFrameShape(QFrame.StyledPanel)
        self.LeftTopFrame.setFrameShadow(QFrame.Raised)
        self.LeftTopFrame.setObjectName("LeftTopFrame")
        self.LeftTopVL = QVBoxLayout(self.LeftTopFrame)
        self.LeftTopVL.setObjectName("LeftTopVL")

        self.LeftTopFrameLbl = QLabel(self.LeftTopFrame)
        self.LeftTopFrameLbl.setObjectName("LeftTopFrameLbl")
        self.LeftTopFrameLbl.setText("Load Stage Table - September 2024")
        self.LeftTopFrameLbl.setAlignment(Qt.AlignRight)
        # self.LeftTopFrameLbl.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Expanding)
        self.LeftTopVL.addWidget(self.LeftTopFrameLbl, 0, Qt.AlignTop)

        self.LeftTopSpacer1 = QSpacerItem(40, 30, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.LeftTopVL.addSpacerItem(self.LeftTopSpacer1)

        self.LeftTopFrameHL1 = QHBoxLayout(self.LeftTopFrame)
        self.LeftTopFrameHL1.setObjectName("self.LeftTopFrameHL1")
        self.LeftTopVL.addLayout(self.LeftTopFrameHL1)

        self.PickFileBtn = QPushButton()
        self.PickFileBtn.setFixedWidth(160)
        self.PickFileBtn.setObjectName("PickFileBtn")
        self.LeftTopFrameHL1.addWidget(self.PickFileBtn)

        self.LeftTopFrameTextBox = QTextEdit()
        self.LeftTopFrameTextBox.setObjectName("LeftTopFrameTextBox")
        self.LeftTopFrameTextBox.setReadOnly(True)
        self.LeftTopFrameTextBox.setFixedHeight(self.PickFileBtn.sizeHint().height() - 5)
        self.LeftTopFrameHL1.addWidget(self.LeftTopFrameTextBox)

        self.LeftTopFrameLoadBtn = QPushButton(self.LeftTopFrame)
        self.LeftTopFrameLoadBtn.setObjectName("LeftTopFrameLoadBtn")
        self.LeftTopFrameLoadBtn.setText("Load File")
        self.LeftTopFrameLoadBtn.setFixedWidth(160)
        self.LeftTopVL.addWidget(self.LeftTopFrameLoadBtn, 0, Qt.AlignCenter)

        self.LeftFrameVL.addWidget(self.LeftTopFrame)
        self.LeftTopVL.addStretch()

        self.LeftBottomFrame = QFrame(self.LeftFrame)
        self.LeftBottomFrame.setFrameShape(QFrame.StyledPanel)
        self.LeftBottomFrame.setFrameShadow(QFrame.Raised)
        self.LeftBottomFrame.setObjectName("LeftBottomFrame")
        self.LeftBottomVL = QVBoxLayout(self.LeftBottomFrame)
        self.LeftBottomVL.setObjectName("LeftBottomVL")
        self.LeftFrameVL.addWidget(self.LeftBottomFrame)

        self.LeftBottomLbl = QLabel(self.LeftBottomFrame)
        self.LeftBottomLbl.setObjectName("LeftBottomLbl")
        # self.LeftBottomLbl.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.LeftBottomLbl.setText("Image")
        self.LeftBottomVL.addWidget(self.LeftBottomLbl)

        self.LeftBottomHL = QHBoxLayout(self.LeftBottomFrame)
        self.LeftBottomHL.setObjectName("LeftBottomHL")
        self.LeftBottomVL.addLayout(self.LeftBottomHL)

        self.LeftBottomCheckBox = QCheckBox()
        self.LeftBottomCheckBox.setObjectName("LeftBottomCheckBox")
        self.LeftBottomCheckBox.setFixedWidth(20)
        self.LeftBottomHL.addWidget(self.LeftBottomCheckBox)

        self.LeftBottomCheckBoxLbl = QLabel()
        self.LeftBottomCheckBoxLbl.setText("Already Loaded")
        self.LeftBottomCheckBoxLbl.setObjectName("LeftBottomCheckBoxLbl")
        self.LeftBottomHL.addWidget(self.LeftBottomCheckBoxLbl, 0, Qt.AlignLeft)
        # print(self.LeftBottomCheckBox.geometry())
        self.LeftBottomCheckBoxLbl.setFixedSize(150, self.LeftBottomCheckBox.sizeHint().height())
        self.LeftTopFrame.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
        self.LeftBottomFrame.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)

        self.MainFrame_HL.addWidget(self.LeftFrame)

        # --------------------------------------------------------------------------------------------------- #
        # --------------------------------------- RIGHT SIDE WIDGETS ---------------------------------------- #
        # --------------------------------------------------------------------------------------------------- #

        self.RightFrame = QFrame(self.MainFrame)
        self.RightFrame.setFrameShape(QFrame.StyledPanel)
        self.RightFrame.setFrameShadow(QFrame.Raised)
        self.RightFrame.setObjectName("RightFrame")
        self.RightFrameVL = QVBoxLayout(self.RightFrame)
        self.RightFrameVL.setObjectName("RightFrameVL")

        self.LoadTblBtn = QPushButton(self.RightFrame)
        self.LoadTblBtn.setObjectName("LoadTblBtn")
        self.LoadTblBtn.setProperty('clicked', False)
        self.RightFrameVL.addWidget(self.LoadTblBtn, 0, Qt.AlignCenter)

        self.RightTopFrame = QFrame(self.RightFrame)
        self.RightTopFrame.setFrameShape(QFrame.StyledPanel)
        self.RightTopFrame.setFrameShadow(QFrame.Raised)
        self.RightTopFrame.setObjectName("RightTopFrame")
        self.RightTopVL = QVBoxLayout(self.RightTopFrame)
        self.RightTopVL.setObjectName("RightTopVL")

        self.RightTopFrameLbl = QLabel(self.RightTopFrame)
        self.RightTopFrameLbl.setObjectName("RightTopFrameLbl")
        self.RightTopFrameLbl.setText("Load Report Table")
        self.RightTopFrameLbl.setAlignment(Qt.AlignLeft)
        self.RightTopVL.addWidget(self.RightTopFrameLbl, 0, Qt.AlignTop)

        self.LoadBaseTbl = QPushButton(self.RightTopFrame)
        self.LoadBaseTbl.setObjectName("LoadBaseTbl")
        self.LoadBaseTbl.setFixedWidth(160)
        self.RightTopVL.addWidget(self.LoadBaseTbl, 0, Qt.AlignCenter)
        self.RightFrameVL.addWidget(self.RightTopFrame)

        self.RightTopFrameNote = QLabel(self.RightTopFrame)
        self.RightTopFrameNote.setObjectName("RightTopFrameNote")
        self.RightTopFrameNote.setText("*Ensure staging tables are loaded before proceeding to load the base tables.")
        self.RightTopFrameNote.setAlignment(Qt.AlignLeft)
        self.RightTopVL.addWidget(self.RightTopFrameNote, 0, Qt.AlignLeft)

        self.RightTopSpacer1 = QSpacerItem(40, 200, QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.RightTopVL.addSpacerItem(self.RightTopSpacer1)

        self.RightBottomFrame = QFrame(self.RightFrame)
        self.RightBottomFrame.setFrameShape(QFrame.StyledPanel)
        self.RightBottomFrame.setFrameShadow(QFrame.Raised)
        self.RightBottomFrame.setObjectName("RightBottomFrame")
        self.RightBottomVL = QVBoxLayout(self.RightBottomFrame)
        self.RightBottomVL.setObjectName("RightBottomVL")
        self.RightFrameVL.addWidget(self.RightBottomFrame)
        # self.RightFrameVL.setStretch(0, 1)
        # self.RightFrameVL.setStretch(1, 4)

        self.RightFrameVLSpacer = QSpacerItem(20, 60, QSizePolicy.Minimum, QSizePolicy.Expanding)
        self.RightFrameVLSpacer_EnableFlag = False

        self.RightBottomLogBox = QTextEdit(self.RightBottomFrame)
        self.RightBottomLogBox.setObjectName("RightBottomLogBox")
        self.RightBottomVL.addWidget(self.RightBottomLogBox)
        self.RightBottomLogBox.setReadOnly(True)
        # self.RightBottomLogBox.setLi

        self.MainFrame_HL.addWidget(self.RightFrame)
        self.CentralWidget_VL.addWidget(self.MainFrame)
        self.setCentralWidget(self.centralwidget)

        # --------------------------------------------------------------------------------------------------- #
        # -------------------------------------------- MENU BAR --------------------------------------------- #
        # --------------------------------------------------------------------------------------------------- #

        self.menubar = QMenuBar(self)
        self.menubar.setGeometry(QRect(0, 0, 550, 18))
        self.menubar.setObjectName("menubar")
        self.menuFile = QMenu(self.menubar)
        self.menuFile.setObjectName("menuFile")
        self.menuSettings = QMenu(self.menubar)
        self.menuSettings.setObjectName("menuSettings")
        self.setMenuBar(self.menubar)

        self.actionExit = QAction(self)
        self.actionExit.setObjectName("actionExit")
        self.actionExit.triggered.connect(self.CloseApp)

        self.actionUpdatePassword = QAction(self)
        self.actionUpdatePassword.setObjectName("actionUpdatePassword")
        self.actionUpdatePassword.triggered.connect(self.UpdateSFPassword)

        self.menuFile.addAction(self.actionExit)
        self.menuSettings.addAction(self.actionUpdatePassword)

        self.menubar.addAction(self.menuFile.menuAction())
        self.menubar.addAction(self.menuSettings.menuAction())

        # --------------------------------------------------------------------------------------------------- #

        self.MainFrame_HL.setContentsMargins(0, 0, 0, 0)
        self.LeftFrameVL.setContentsMargins(0, 0, 0, 0)
        self.RightFrameVL.setContentsMargins(0, 0, 0, 0)
        self.RightTopVL.setContentsMargins(0, 0, 0, 0)
        self.RightBottomVL.setContentsMargins(0, 0, 0, 0)
        self.LeftBottomVL.setContentsMargins(6, 0, 6, 0)
        self.LeftTopVL.setContentsMargins(6, 0, 6, 0)
        self.LeftTopFrameLbl.setContentsMargins(0, 10, 40, 0)
        self.LeftBottomHL.setContentsMargins(20, 0, 0, 20)
        self.RightTopFrameLbl.setContentsMargins(40, 10, 0, 0)

        self.InitialLayoutSetting()
        self.query_executor = QueryExecutor()
        self.log = Logger()

        # -----Left Side Functions
        self.LoadFileBtn.clicked.connect(self.LoadFile)
        self.PickFileBtn.clicked.connect(self.PickExcelFile)
        self.LeftTopFrameLoadBtn.clicked.connect(self.StageTableLoad)

        # -----Right Side Functions
        self.LoadTblBtn.clicked.connect(self.ReportTableLoad)
        self.LoadBaseTbl.clicked.connect(self.LoadReportTables)

        # -----Default Functions
        self.retranslateUi()
        self.SetCssStyle()
        QMetaObject.connectSlotsByName(self)

    def retranslateUi(self):
        _translate = QCoreApplication.translate
        self.setWindowTitle(_translate("MainWindow", "DGN"))
        self.LoadFileBtn.setText(_translate("MainWindow", "Load File"))
        self.PickFileBtn.setText(_translate("MainWindow", "Pick File"))
        self.LoadTblBtn.setText(_translate("MainWindow", "Load Report Tables"))
        self.LoadBaseTbl.setText(_translate("MainWindow", "Load Tables Now"))
        self.menuFile.setTitle(_translate("MainWindow", "File"))
        self.menuSettings.setTitle(_translate("MainWindow", "Settings"))
        self.actionExit.setText(_translate("MainWindow", "Exit"))
        self.actionUpdatePassword.setText(_translate("MainWindow", "Update Password"))

    def SetCssStyle(self):
        with open('style.qss', 'r') as file:
            app.setStyleSheet(file.read())

    def InitialLayoutSetting(self):
        print("InitialLayoutSetting Called")
        self.LeftTopFrame.setVisible(False)
        self.LeftBottomFrame.setVisible(False)
        self.RightTopFrame.setVisible(False)
        self.RightBottomFrame.setVisible(False)

    def LoadFile(self):
        print("LoadFile")
        self.LoadTblBtn.setProperty('clicked', True)
        self.LoadTblBtn.style().unpolish(self.LoadTblBtn)
        self.LoadTblBtn.style().polish(self.LoadTblBtn)
        self.LoadTblBtn.update()

        self.LoadTblBtn.setVisible(True)
        self.LoadFileBtn.setVisible(False)
        self.RightTopFrame.setVisible(False)
        self.RightBottomFrame.setVisible(False)
        self.LeftTopFrame.setVisible(True)
        self.LeftBottomFrame.setVisible(True)

        if self.RightFrameVLSpacer_EnableFlag:
            self.RightFrameVL.removeItem(self.RightFrameVLSpacer)
            self.RightFrameVLSpacer_EnableFlag = False

        self.MainFrame_HL.setStretchFactor(self.LeftFrame, 9)
        self.MainFrame_HL.setStretchFactor(self.RightFrame, 1)

    def StageTableLoad(self):
        self.LeftBottomLbl.setScaledContents(True)
        pixmap = QPixmap("<--------------------------PATH------------------------->")
        scaledPixmap = pixmap.scaled(self.LeftBottomLbl.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.LeftBottomLbl.setPixmap(scaledPixmap)

    def ReportTableLoad(self):
        self.LoadFileBtn.setProperty('clicked', True)
        self.LoadFileBtn.style().unpolish(self.LoadFileBtn)
        self.LoadFileBtn.style().polish(self.LoadFileBtn)
        self.LoadFileBtn.update()

        self.LoadTblBtn.setVisible(False)
        self.LoadFileBtn.setVisible(True)
        self.LeftTopFrame.setVisible(False)
        self.LeftBottomFrame.setVisible(False)
        self.RightTopFrame.setVisible(True)
        self.RightBottomFrame.setVisible(True)

        if not self.RightFrameVLSpacer_EnableFlag:
            self.RightFrameVL.addSpacerItem(self.RightFrameVLSpacer)
            self.RightFrameVLSpacer_EnableFlag = True

        self.MainFrame_HL.setStretchFactor(self.LeftFrame, 1)
        self.MainFrame_HL.setStretchFactor(self.RightFrame, 9)
        self.RightFrameVL.setStretchFactor(self.RightTopFrame, 1)
        self.RightFrameVL.setStretchFactor(self.RightBottomFrame, 1)

    def PickExcelFile(self):
        options = QFileDialog.Options()
        file_filter = "Excel Files (*.xlsx *.xls)"

        # Open file dialog and get selected file path
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel File", "", file_filter, options=options)

        # If a file is selected, display its path
        if file_path:
            self.LoadExcelFilePath = file_path
            self.LeftTopFrameTextBox.setPlainText(file_path)
            cursor = self.LeftTopFrameTextBox.textCursor()

            cursor.setPosition(0)
            cursor.movePosition(QTextCursor.Right, QTextCursor.KeepAnchor, len(file_path))

            char_format = QTextCharFormat()
            char_format.setFontWeight(QFont.Bold)
            char_format.setForeground(QColor("Black"))
            cursor.setCharFormat(char_format)

    def LoadReportTables(self):
        if not self.LeftBottomCheckBox.isChecked():
            print('Checked')
            self.query_executor.start()
            self.query_executor.started.connect(self.startThread)
            self.query_executor.finished.connect(self.endThread)
            self.query_executor.query_completion.connect(self.appendlog)
            self.query_executor.folder_error.connect(self.FolderError)

            self.log.message.connect(self.logMessage)

        else:
            QMessageBox.warning(self, "Warning", "Staging tables are not loaded. Please load staging tables before "
                                                 "proceeding with base table loading.")

    def FolderError(self):
        self.log.requestInterruption()
        self.log.wait()
        self.log.quit()
        self.query_executor.quit()
        reply = QMessageBox.question(self, "Error", "Configuration file does not exists. Click on Yes to Update "
                                                    "the Password.", QMessageBox.Yes | QMessageBox.No,
                                     QMessageBox.Yes)

        if reply == QMessageBox.Yes:
            dialog = UpdatePasswordDialog(self)
            dialog.setWindowModality(Qt.ApplicationModal)
            dialog.exec_()

    def logMessage(self, message):
        self.RightBottomLogBox.append(message)

    def startThread(self):
        print("Query Thread Started")
        self.log.start()

    def endThread(self):
        print("Query Thread Ended")
        self.log.requestInterruption()
        self.log.wait()
        self.log.quit()
        print("Logger Ended")

    def appendlog(self, a):
        self.RightBottomLogBox.append(f"Completed {a} out of 3")
        print(f'Completed emit')

    def CloseApp(self):
        reply = QMessageBox.question(self, "Confirm Exit", "Are you sure you want to exit?",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            qApp.quit()

    def UpdateSFPassword(self):
        dialog = UpdatePasswordDialog(self)
        dialog.setWindowModality(Qt.ApplicationModal)
        dialog.exec_()


if __name__ == "__main__":
    import sys

    global file_path
    file_path = 'EncryptedData/config.txt'

    app = QApplication(sys.argv)
    ui = Ui_MainWindow()
    ui.show()
    sys.exit(app.exec_())
