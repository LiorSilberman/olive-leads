import sys
import os
os.environ["QT_QPA_PLATFORM"] = "xcb"
import shutil
import webbrowser
import pandas as pd
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog, QMessageBox, QTextEdit
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon, QPixmap, QFont, QFontDatabase
from dotenv import load_dotenv
from qasync import QEventLoop, asyncSlot
import asyncio
from olive_table import merge_csv_files, authenticate_gsheets, upload_to_gsheets, set_column_order

class CSVUploaderApp(QWidget):
    def __init__(self):
        super().__init__()
        load_dotenv()
        self.json_keyfile = os.getenv('JSON_KEYFILE')
        self.sheet_url = os.getenv('SHEET_URL')
        font_id = QFontDatabase.addApplicationFont("./VarelaRound-Regular.ttf")
        self.font_name = QFontDatabase.applicationFontFamilies(font_id)[0]
        self.setFont(QFont(self.font_name))
        self.initUI()

    def initUI(self):
        self.setGeometry(300, 300, 1200, 800)
        self.setWindowTitle('Google Sheets - העלאה ל')
        self.setWindowIcon(QIcon('./logo.png'))

        self.setStyleSheet(f"QWidget {{ background-color: #3d5544; color: #ffffff; font-family: '{self.font_name}'; font-size: 12pt; }}")

        mainLayout = QVBoxLayout()
        topLayout = QHBoxLayout()  
        topLayout.addStretch(1)  
        
        iconLabel = QLabel(self)
        pixmap = QPixmap('./logo.png')
        scaled_pixmap = pixmap.scaled(300, 300, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        iconLabel.setPixmap(scaled_pixmap)
        iconLabel.setFixedSize(300, 300)
        topLayout.addWidget(iconLabel)
        topLayout.addStretch(1)  
        mainLayout.addLayout(topLayout)

        self.label = QLabel('בחר קבצים להעלאה:', self)
        mainLayout.addWidget(self.label)

        self.label = QLabel(f'1. לקוחות פעילים\n 2. מנויים פעילים \n 3. מתעניינים שהומרו ללקוחות \n 4. כל המתעניינים \n 5. שיעורי ניסיון \n 6. מתעניינים אבודים \n7. לקוחות לא פעילים', self)
        mainLayout.addWidget(self.label)

        self.uploadButton = QPushButton('בחירת קבצים', self)
        self.uploadButton.clicked.connect(self.upload_files)
        mainLayout.addWidget(self.uploadButton)

        self.processButton = QPushButton('Google Sheets - העלה ל', self)
        self.processButton.clicked.connect(self.process_files)
        self.processButton.setEnabled(False)
        mainLayout.addWidget(self.processButton)

        self.openSheetButton = QPushButton('Google Sheets - פתח את', self)
        self.openSheetButton.clicked.connect(self.open_sheet)
        self.openSheetButton.setEnabled(False)
        mainLayout.addWidget(self.openSheetButton)

        self.statsText = QTextEdit(self)
        self.statsText.setReadOnly(True)
        
        # self.statsText.setLayoutDirection(Qt.RightToLeft)
        mainLayout.addWidget(self.statsText)

        self.setLayout(mainLayout)
        self.data_directory = self.ensure_data_directory_exists()
        self.files = []

    @asyncSlot()
    async def upload_files(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        files, _ = QFileDialog.getOpenFileNames(self, "Select one or more files to open", self.get_downloads_folder(), "CSV Files (*.csv)", options=options)
        if files:
            self.clear_data_directory()
            self.files = []
            for file_path in files:
                shutil.copy(file_path, self.data_directory)
                self.files.append(os.path.join(self.data_directory, os.path.basename(file_path)))
            self.processButton.setEnabled(True)
            self.openSheetButton.setEnabled(False)
            await asyncio.sleep(0)
            QMessageBox.information(self, 'קבצים נבחרו', f'הועתקו {len(self.files)} קבצים לתיקייה.')
        else:
            QMessageBox.critical(self, 'לא נבחרו קבצים', 'נא לבחור לפחות קובץ אחד.')

        
    @asyncSlot()
    async def process_files(self):
        if not self.files:
            QMessageBox.critical(self, 'לא נבחרו קבצים', 'לא נבחרו קבצים לעיבוד.')
            return

        merged_df = merge_csv_files(self.data_directory)
        if merged_df is not None:
            stats = await self.calculate_statistics(merged_df)
            gc = authenticate_gsheets(self.json_keyfile)
            column_order = ['נוצר בתאריך', 'שם', 'טלפון', 'מקור', 'סטטוס', 'סיבות התנגדות', 'מפגש ניסיון', 'עשו ניסיון', 'רלוונטי', 'מנוי', 'גיל', 'קובץ מקור']
            final_df = set_column_order(merged_df, column_order)
            upload_to_gsheets(final_df, gc, self.sheet_url)
            self.statsText.setHtml(stats)  # Display the statistics
            QMessageBox.information(self, 'העלאה הושלמה', 'Google Sheets - הנתונים הועלו ל\nכעת ניתן לפתוח את הגיליון.')
            self.openSheetButton.setEnabled(True)
        else:
            QMessageBox.critical(self, 'שגיאה', 'נכשל בתהליך העיבוד וההעלאה של הקבצים.')

    def open_sheet(self):
        webbrowser.open_new(self.sheet_url)

    async def calculate_statistics(self, df):
        if df.empty:
            return "<p style='color: red; text-align: right;'>אין נתונים לחישוב סטטיסטיקה.</p>"

        await asyncio.sleep(1)
        # Enhanced CSS for better table readability with visible borders
        css = """
        <style>
            table {
                width: 100%;
                border-collapse: collapse;
                text-align: center;
                border: 1px solid black; /* Adds a border around the table */
                margin-bottom: 20px; /* Adds spacing after the table */
            }
            th, td {
                padding: 8px;
                border: 1px solid black; /* Adds visible borders for table cells */
                vertical-align: middle; /* Ensures text is centered vertically in cells */
            }
            tr:nth-child(even) {
                background-color: #f2f2f2; /* Alternating row colors for better readability */
            }
            tr:hover {
                background-color: #f5f5f5; /* Optional: highlights row on hover */
            }
            h2 {
                
                margin: 10px 0 10px 20px; /* Adds space above and below the heading */
            }
        </style>
        """

        # Convert data frames to HTML with enhanced styling
        source_effectiveness = df['מקור'].value_counts(normalize=True).sort_values(ascending=False) * 100
        source_html = source_effectiveness.to_frame().to_html(header=False, border=0)
        source_html = source_html.replace('<table>', "<table>")

        subscription_types = df['מנוי'].value_counts()
        subscriptions_html = subscription_types.to_frame().to_html(header=False, border=0)
        subscriptions_html = subscriptions_html.replace('<table>', "<table>")

        if df['עשו ניסיון'].eq('V').sum() > 0:
            trial_success_rate = ((df[(df['עשו ניסיון'] == 'V') & df['מנוי'].notna() & (df['מנוי'] != 'ללא')].shape[0]) / df['עשו ניסיון'].eq('V').sum()) * 100
        else:
            trial_success_rate = 0


        age_distribution = df['גיל'].mean()

        # Combine all HTML parts with the CSS header
        stats = f"{css}<div><h2>מקורות הפעילות:</h2>{source_html}</div>" \
                f"<div><h2>סוגי מנויים:</h2>{subscriptions_html}</div>" \
                f"<div><h2>הצלחת שיעורי המרה: {trial_success_rate:.2f}%</h2></div>" \
                f"<div><h2>ממוצע גילאים: {age_distribution:.2f}</h2></div>"
        return stats


    @staticmethod
    def get_downloads_folder():
        home = os.path.expanduser("~")
        return os.path.join(home, 'Downloads')

    @staticmethod
    def ensure_data_directory_exists():
        data_directory = os.path.join(os.getcwd(), 'data')
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
