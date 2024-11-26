import sys
import os
# os.environ["QT_QPA_PLATFORM"] = "xcb"
import shutil
# from pathlib import Path
# import webbrowser
# import pandas as pd
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QSizePolicy, QProgressBar
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon, QPixmap, QFont, QFontDatabase
from dotenv import load_dotenv
from qasync import QEventLoop
import asyncio
# from olive_table import authenticate_gsheets, upload_to_gsheets, set_column_order
# from statistics_calculator import calculate_statistics
import app_functions
from utils import resource_path


class CSVUploaderApp(QWidget):
    def __init__(self):
        super().__init__()
        load_dotenv(resource_path('.env'))
        self.json_keyfile = os.getenv('JSON_KEYFILE')
        self.sheet_url = os.getenv('SHEET_URL')
        font_id = QFontDatabase.addApplicationFont(resource_path("VarelaRound-Regular.ttf"))
        self.font_name = QFontDatabase.applicationFontFamilies(font_id)[0]
        self.setFont(QFont(self.font_name))
        self.initUI()

    def initUI(self):
        # Window settings
        self.setGeometry(300, 300, 1200, 800)
        self.setWindowTitle('Google Sheets - העלאה ל')
        self.setWindowIcon(QIcon(resource_path('logo.png')))
        self.setStyleSheet(f"""
            QWidget {{
                background-color: #3d5544;
                color: #ffffff;
                font-family: '{self.font_name}';
                font-size: 12pt;
            }}
            QPushButton {{
                background-color: #2d4735;
                border: 1px solid #ffffff;
                padding: 10px;
                border-radius: 5px;
                font-size: 12pt;
            }}
            QPushButton:hover {{
                background-color: #4b7155;
            }}
            QPushButton:disabled {{
                background-color: #5c5c5c;
                border-color: #5c5c5c;
            }}
            QLabel {{
                margin: 10px 0;
            }}
        """)

        # Main layout
        mainLayout = QVBoxLayout()

        # Logo layout
        logoLayout = QHBoxLayout()
        logoLayout.addStretch(1)
        iconLabel = QLabel(self)
        pixmap = QPixmap(resource_path('logo.png'))
        scaled_pixmap = pixmap.scaled(250, 250, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        iconLabel.setPixmap(scaled_pixmap)
        logoLayout.addWidget(iconLabel)
        logoLayout.addStretch(1)
        mainLayout.addLayout(logoLayout)

       
        # Buttons layout
        buttonLayout = QHBoxLayout()
        self.showSummaryButton = QPushButton('הדפס סיכום', self)
        self.downloadButton = QPushButton('Arbox - הורד דוחות אוטומטית מ', self)
        self.uploadButton = QPushButton('בחירת קבצים', self)
        self.processButton = QPushButton('Google Sheets - העלה ל', self)
        self.openSheetButton = QPushButton('Google Sheets - פתח את', self)
        self.processButton.setEnabled(False)
        self.openSheetButton.setEnabled(True)
        buttonLayout.addWidget(self.showSummaryButton)
        buttonLayout.addWidget(self.downloadButton)
        buttonLayout.addWidget(self.processButton)
        buttonLayout.addWidget(self.uploadButton)
        buttonLayout.addWidget(self.openSheetButton)
        mainLayout.addLayout(buttonLayout)

        # Success label
        self.successLabel = QLabel('', self)
        self.successLabel.setAlignment(Qt.AlignCenter)
        self.successLabel.setStyleSheet("color: white; font-size: 14pt; font-weight: bold;")
        mainLayout.addWidget(self.successLabel)


        self.progressBar = QProgressBar(self)
        self.progressBar.setFixedSize(400, 50)
        self.progressBar.setValue(0)  
        self.progressBar.setVisible(False)
        mainLayout.addWidget(self.progressBar, alignment=Qt.AlignCenter) 

        self.progressBar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #3D5544;  /* Border color */
                border-radius: 5px;  /* Rounded corners */
                text-align: center;  /* Text in the center */
                background-color: #e0eae3;  /* Background color */
                color: black;
            }
            QProgressBar::chunk {
                border: 2px 2px solid #3D5544;
                background-color: #64c228;  /* Progress bar color */
                width: 20px;  /* Chunk size */
            }
        """)

        # Stats text area
        self.statsText = QTextEdit(self)
        self.statsText.setReadOnly(True)
        self.statsText.setMinimumSize(800, 400)
        self.statsText.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        mainLayout.addWidget(self.statsText)

        # Set the main layout
        self.setLayout(mainLayout)

        
        # Connect buttons
        self.showSummaryButton.clicked.connect(self.wrap_async(app_functions.display_summary))
        self.downloadButton.clicked.connect(self.wrap_async(app_functions.start_download))
        self.uploadButton.clicked.connect(self.wrap_async(app_functions.upload_files))
        self.processButton.clicked.connect(self.wrap_async(app_functions.process_files))
        self.openSheetButton.clicked.connect(self.wrap_async(app_functions.open_sheet))

        # Initialize data directory and file list
        self.data_directory = self.ensure_data_directory_exists()
        self.files = []

    def wrap_async(self, async_func):
        """Wrap the async function to be used with a button click."""
        def wrapper():
            loop = asyncio.get_running_loop()
            loop.create_task(async_func(self))
        return wrapper
    
    # @staticmethod
    # def resource_path(relative_path):
    #     try:
    #         base_path = sys._MEIPASS
    #     except Exception:
    #         base_path = os.path.abspath(".")
        
    #     return os.path.join(base_path, relative_path)

    @staticmethod
    def get_downloads_folder():
        home = os.path.expanduser("~")
        return os.path.join(home, 'Downloads')

    def ensure_data_directory_exists(self):
        data_directory = resource_path('data')
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        return data_directory

    def clear_data_directory(self):
        for filename in os.listdir(self.data_directory):
            file_path = os.path.join(self.data_directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

if __name__ == "__main__":
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    ex = CSVUploaderApp()
    ex.show()
    with loop:
        sys.exit(loop.run_forever())
