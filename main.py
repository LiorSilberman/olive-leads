import sys
import os
# os.environ["QT_QPA_PLATFORM"] = "xcb"
import shutil
from pathlib import Path
import webbrowser
import pandas as pd
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QFileDialog, QMessageBox, QTextEdit, QSizePolicy
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon, QPixmap, QFont, QFontDatabase
from dotenv import load_dotenv
from qasync import QEventLoop, asyncSlot
import asyncio
from concurrent.futures import ThreadPoolExecutor
from olive_table import merge_csv_files, authenticate_gsheets, upload_to_gsheets, set_column_order
from auto_download import login_and_download
from datetime import datetime

class CSVUploaderApp(QWidget):
    def __init__(self):
        super().__init__()
        load_dotenv(self.resource_path('.env'))
        self.json_keyfile = os.getenv('JSON_KEYFILE')
        self.sheet_url = os.getenv('SHEET_URL')
        font_id = QFontDatabase.addApplicationFont(self.resource_path("VarelaRound-Regular.ttf"))
        self.font_name = QFontDatabase.applicationFontFamilies(font_id)[0]
        self.setFont(QFont(self.font_name))
        self.initUI()

    def initUI(self):
        # Window settings
        self.setGeometry(300, 300, 1200, 800)
        self.setWindowTitle('Google Sheets - העלאה ל')
        self.setWindowIcon(QIcon(self.resource_path('logo.png')))
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
        pixmap = QPixmap(self.resource_path('logo.png'))
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
        self.processButton.setEnabled(False)  # Disabled by default
        self.openSheetButton.setEnabled(True)  # Disabled by default
        buttonLayout.addWidget(self.showSummaryButton)
        buttonLayout.addWidget(self.downloadButton)
        buttonLayout.addWidget(self.processButton)
        buttonLayout.addWidget(self.uploadButton)
        buttonLayout.addWidget(self.openSheetButton)
        mainLayout.addLayout(buttonLayout)

        # Success label
        self.successLabel = QLabel('', self)
        self.successLabel.setAlignment(Qt.AlignCenter)
        self.successLabel.setStyleSheet("color: green; font-size: 14pt; font-weight: bold;")
        mainLayout.addWidget(self.successLabel)

        # Instructions label
        instructionsLabel = QLabel(
            'בחר קבצים להעלאה:\n'
            '1. לקוחות פעילים | 2. מנויים פעילים | 3. מתעניינים שהומרו ללקוחות |\n'
            '4. כל המתעניינים | 5. שיעורי ניסיון | 6. מתעניינים אבודים | 7. לקוחות לא פעילים',
            self
        )
        instructionsLabel.setAlignment(Qt.AlignCenter)
        instructionsLabel.setStyleSheet("margin-top: 10px; font-size: 12pt;")
        mainLayout.addWidget(instructionsLabel)

        # Stats text area
        self.statsText = QTextEdit(self)
        self.statsText.setReadOnly(True)
        self.statsText.setMinimumSize(800, 400)
        self.statsText.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        mainLayout.addWidget(self.statsText)

        # Set the main layout
        self.setLayout(mainLayout)

        # Connect buttons
        self.showSummaryButton.clicked.connect(self.display_summary)
        self.downloadButton.clicked.connect(self.start_download)
        self.uploadButton.clicked.connect(self.upload_files)
        self.processButton.clicked.connect(self.process_files)
        self.openSheetButton.clicked.connect(self.open_sheet)

        # Initialize data directory and file list
        self.data_directory = self.ensure_data_directory_exists()
        self.files = []

   

    @asyncSlot()
    async def start_download(self):
        QMessageBox.information(self, 'התחלת הורדה', 'ההורדות מתחילות כעת...')
        await asyncio.sleep(0)  # Yield control to ensure UI updates before starting the download
        try:
            # Use ThreadPoolExecutor to run the blocking function in a separate thread
            executor = ThreadPoolExecutor(max_workers=1)
            loop = asyncio.get_running_loop()

            self.clear_data_directory()
            
            await loop.run_in_executor(executor, login_and_download)
            self.files = [os.path.join(self.data_directory, f) for f in os.listdir(self.data_directory) if os.path.isfile(os.path.join(self.data_directory, f))]

            await asyncio.sleep(1)
            start_date = "01/10/2024"
            today_date = datetime.now().strftime("%d/%m/%Y")
            self.successLabel.setText(f"הנתונים מעודכנים מתאריך {start_date} עד {today_date}")
            QMessageBox.information(self, 'הורדה הושלמה', 'כל הדוחות הורדו בהצלחה - לחץ אשר והמתן בסבלנות לסיום התהליך.')

            await self.process_files()
            
        except Exception as e:
            QMessageBox.critical(self, 'שגיאה בהורדה', f'אירעה שגיאה במהלך ההורדה: באפשרותך לנסות שוב או להעלות קבצים בצורה ידנית')

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
        else:
            QMessageBox.critical(self, 'שגיאה', 'נכשל בתהליך העיבוד וההעלאה של הקבצים.')

    @asyncSlot()
    async def display_summary(self):
        sheets_data_dir = Path(self.resource_path('sheets_data'))
        output_file = sheets_data_dir / 'cleaned_data_corrected.csv'
        df = pd.read_csv(self.resource_path(output_file))
        stats = await self.calculate_statistics(df)
        self.statsText.setHtml(stats)
        QMessageBox.information(self, 'הדפסה הושלמה', 'כעת תוכל לצפות בסיכומים')

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
                border: 1px solid white; /* Adds visible borders for table cells */
                vertical-align: middle; /* Ensures text is centered vertically in cells */
                text-align: center;
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

        # Source effectiveness calculation
        source_effectiveness = df['מקור'].value_counts(normalize=True).sort_values(ascending=False) * 100
        source_quantity = df['מקור'].value_counts().sort_values(ascending=False)
        source_effectiveness = pd.DataFrame({
            'מקור': source_effectiveness.index,
            'אחוזים': source_effectiveness.values,
            'כמות': source_quantity.values
        })

        # source_effectiveness = source_effectiveness.reset_index(name='אחוזים')
        source_html = source_effectiveness.to_html(index=False, header=True, border=0)
        source_html = source_html.replace('<table>', "<table>")

        # Subscription types calculation
        subscription_types = df['מנוי'].value_counts().reset_index(name='כמות')
        
        subscriptions_html = subscription_types.to_html(index=False ,header=True, border=0)
        subscriptions_html = subscriptions_html.replace('<table>', "<table>")
        
        # Trial success rate calculation
        if df['עשו ניסיון'].eq('V').sum() > 0:
            did_trial = df['עשו ניסיון'].eq('V').sum()
            did_trial_and_members = (df[(df['עשו ניסיון'] == 'V') & df['מנוי'].notna() & (df['מנוי'] != 'ללא')].shape[0])
            trial_success_rate = (did_trial_and_members / did_trial) * 100
        else:
            trial_success_rate = 0


        age_distribution = df['גיל'].mean()

        # Trials by source calculation
        trial_data = df[df['עשו ניסיון'] == 'V']
        trial_by_source = trial_data.groupby('מקור').size().reset_index(name='מספר מתאמנות')

        subscription_count = (
            trial_data.groupby('מקור')['מנוי']
            .apply(lambda x: x.notna().sum() - (x == "ללא").sum())
            .reset_index(name='כמות מנויים')
        )

        trial_by_source = pd.merge(trial_by_source, subscription_count, on='מקור', how='left')
        trial_by_source_html = trial_by_source.to_html(index=False, header=True, border=0)
        trial_by_source_html = trial_by_source_html.replace('<table>', "<table>")
        

        coaches_count = df['מאמנים'].value_counts().reset_index(name='כמות')
        coaches_count.columns = ['מאמנים', 'כמות']
        subscription_count = (
            df.groupby('מאמנים')['מנוי']
            .apply(lambda x: x.notna().sum()) 
            .reset_index(name='כמות מנויים שסגרו') 
        )

        coaches_count = pd.merge(coaches_count, subscription_count, on='מאמנים', how='left')
        
        coaches_html = coaches_count.to_html(index=False, header=True, border=0)
        coaches_html = coaches_html.replace('<table>', "<table>")



        # Combine all HTML parts with the CSS header
        stats = f"{css}<div><h2>אחוזי קליטה עבור כל מקור: </h2>{source_html}</div>" \
                f"<div><h2>מספר אימוני ניסיון שהגיעו עבור כל מקור: </h2>{trial_by_source_html}</div>" \
                f"<div><h2>סוגי מנויים:</h2>{subscriptions_html}</div>" \
                 f"<div><h2>מאמנות:</h2>{coaches_html}</div>" \
                f"<div><h2>הצלחת שיעורי המרה:</h2> <ul><li><h3>מספר המתאמנים שעשו אימון ניסיון: {did_trial}</h3></li><li><h3>מספר מנויים שעשו אימון ניסיון: {did_trial_and_members}</h3></li> <li><h3>הצלחת שיעורי המרה באחוזים: {trial_success_rate:.2f}%</h3></li></ul></div>" \
                f"<div><h2>ממוצע גילאים: {age_distribution:.2f}</h2></div>"
        return stats

    
    @staticmethod
    def resource_path(relative_path):
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        
        return os.path.join(base_path, relative_path)

    @staticmethod
    def get_downloads_folder():
        home = os.path.expanduser("~")
        return os.path.join(home, 'Downloads')

    def ensure_data_directory_exists(self):
        data_directory = self.resource_path('data')
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
