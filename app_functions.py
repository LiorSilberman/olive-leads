import asyncio
import os
import sys
import pandas as pd
from pathlib import Path
import shutil
import webbrowser
from PyQt5.QtWidgets import QMessageBox, QFileDialog, QApplication
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from olive_table import merge_csv_files, authenticate_gsheets, upload_to_gsheets, set_column_order
from auto_download import login_and_download
from statistics_calculator import calculate_statistics
from utils import resource_path


async def start_download(app):
    """
    Initiates the process to download files asynchronously from Arbox and updates the application state.

    Args:
        app (QWidget): An instance of your application's main QWidget, expected to have certain properties and methods like progressBar, clear_data_directory, etc.

    This function handles errors and updates the application's UI components like the progress bar and status labels.
    """
    await asyncio.sleep(0)
    try:
        executor = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_running_loop()
        # app.statusLabel.setVisible(True)
        app.progressBar.setVisible(True)
        app.progressBar.setValue(0)

        def update_message(progress=None):
            """Update the status label and progress bar."""
            if progress is not None:
                app.progressBar.setValue(progress)
            QApplication.processEvents() 

        app.clear_data_directory()
        
        await loop.run_in_executor(executor, lambda: login_and_download(update_message))
        app.files = [os.path.join(app.data_directory, f) for f in os.listdir(app.data_directory) if os.path.isfile(os.path.join(app.data_directory, f))]

        await asyncio.sleep(1)

        start_date = "01/09/2024"
        today_date = datetime.now().strftime("%d/%m/%Y")
        app.successLabel.setText(f"הנתונים מעודכנים מתאריך {start_date} עד {today_date}")

        await process_files(app, update_message)
        
    except Exception as e:
        QMessageBox.critical(app, 'שגיאה בהורדה', f'אירעה שגיאה במהלך ההורדה: {str(e)}')
    finally:
        app.progressBar.setVisible(False)  # Hide the progress bar when done


async def upload_files(app):
    """
    Opens a file dialog to let the user select CSV files to upload, and copies these files to a designated data directory.

    Args:
        app (QWidget): An instance of your application's main QWidget that provides the data directory and UI methods to interact with the user.

    Shows messages to the user about the status of file selection and copying.
    """
    options = QFileDialog.Options()
    options |= QFileDialog.DontUseNativeDialog
    files, _ = QFileDialog.getOpenFileNames(app, "Select one or more files to open", app.get_downloads_folder(), "CSV Files (*.csv)", options=options)
    if files:
        app.clear_data_directory()
        app.files = []
        for file_path in files:
            shutil.copy(file_path, app.data_directory)
            app.files.append(os.path.join(app.data_directory, os.path.basename(file_path)))
        app.processButton.setEnabled(True)
        await asyncio.sleep(0)
        QMessageBox.information(app, 'קבצים נבחרו', f'הועתקו {len(app.files)} קבצים לתיקייה.')
    else:
        QMessageBox.critical(app, 'לא נבחרו קבצים', 'נא לבחור לפחות קובץ אחד.')


async def display_summary(app):
    """
    Displays summary statistics of the data contained in a pre-defined CSV file.

    Args:
        app (QWidget): An instance of the application that has methods to access application resources and UI components to display the data.

    The summary is displayed in a QTextEdit component within the application.
    """
    sheets_data_dir = Path(resource_path('sheets_data'))
    output_file = sheets_data_dir / 'cleaned_data_corrected.csv'
    df = pd.read_csv(resource_path(output_file))
    stats = await calculate_statistics(df)
    app.statsText.setHtml(stats)
    QMessageBox.information(app, 'הדפסה הושלמה', 'כעת תוכל לצפות בסיכומים')


async def process_files(app, update_message=None):
    """
    Processes selected files by merging, calculating statistics, and uploading them to Google Sheets.

    Args:
        app (QWidget): The main application instance with access to app data and methods.
        update_message (Callable[[int], None]): Optional; A callback function to update the progress displayed to the user.

    Handles the full lifecycle of file processing from reading, merging, calculating statistics, and uploading to Google Sheets.
    """
    if not app.files:
        QMessageBox.critical(app, 'לא נבחרו קבצים', 'לא נבחרו קבצים לעיבוד.')
        return
    
    # update_message('מייצר קובץ אחיד..')
    update_message(0)
    merged_df = merge_csv_files(app.data_directory)
    update_message(30)
    if merged_df is not None:
        update_message(50)
        stats = await calculate_statistics(merged_df)
        update_message(70)
        gc = authenticate_gsheets(app.json_keyfile)
        column_order = ['נוצר בתאריך', 'שם', 'טלפון', 'מקור', 'סטטוס', 'סיבות התנגדות', 'מפגש ניסיון', 'עשו ניסיון', 'רלוונטי','יש מנוי', 'מנוי', 'גיל', 'קובץ מקור']
        final_df = set_column_order(merged_df, column_order)
        update_message(95)
        upload_to_gsheets(final_df, gc, app.sheet_url)
        update_message(100)
        app.statsText.setHtml(stats)
    else:
        QMessageBox.critical(app, 'שגיאה', 'נכשל בתהליך העיבוד וההעלאה של הקבצים.')


def open_sheet(app):
    """
    Opens the Google Sheets URL specified in the application's environment settings in the user's default web browser.

    Args:
        app (QWidget): The main application instance where the sheet URL is stored.
    """
    webbrowser.open_new(app.sheet_url)


