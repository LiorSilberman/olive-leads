import pandas as pd
import numpy as np
import gspread
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
import re

load_dotenv()

def set_column_order(df, column_order):
    """
    Reorder the DataFrame columns based on a list of column names.
    
    Parameters:
        df (DataFrame): The original DataFrame.
        column_order (list): A list of column names in the desired order.

    Returns:
        DataFrame: A new DataFrame with columns ordered as specified.
    """
    reordered_df = df[column_order] if all(col in df.columns for col in column_order) else df
    
    return reordered_df




def column_to_letter(n):
    """Convert a column index to an Excel column letter (1-indexed)."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def resource_path(relative_path):
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        
        return os.path.join(base_path, relative_path)


def merge_csv_files(directory):
    data_dir = Path(directory)
    dataframes = []
    trial_leads = None
    resource_temp_file = resource_path('resource_fix.csv')
    files_translate = {
        'active-members-report': 'לקוחות פעילים',
        'active-memberships-report': 'מנויים פעילים',
        'converted-leads-report': 'מתעניינים שהומרו ללקוחות',
        'all-leads-report': 'כל המתעניינים', 
        'trial-classes-report': 'שיעורי ניסיון',
        'lost-leads-report': 'מתעניינים אבודים',
        'inactive-members-report': 'לקוחות לא פעילים',
        'future-memberships-report': 'מנויים עתידיים'
    }

    # Iterate over each file in the directory and append it to the dataframe list
    for file in data_dir.glob('*.csv'):
        base_name = re.sub(r'(\s+\(\d+\))$', '', file.stem) 
        translated_name = files_translate.get(base_name, file.stem)
        df = pd.read_csv(file)
        df['קובץ מקור'] = translated_name
        if 'trial' in base_name:
            trial_leads = df
            trial_leads['Normalized Phone'] = trial_leads['טלפון'].astype(str).apply(lambda x: x[-6:])
            trial_leads['תאריך'] = pd.to_datetime(trial_leads['תאריך'], format='%d/%m/%Y', errors='coerce')
            trial_leads = (
                trial_leads.sort_values(by='תאריך', ascending=False)  # Sort by the date column
                .drop_duplicates(subset=['Normalized Phone'], keep='first')  # Drop duplicates
            )
            df = trial_leads
        dataframes.append(df)

    if not dataframes:
        return None


    # read from resource_temp.csv and add it to dataframes
    resource_df = pd.read_csv(resource_temp_file)
    resource_df['טלפון'] = resource_df['טלפון'].apply(lambda x: '0' + x if x.startswith('5') else x)
    dataframes.append(resource_df)

    merged_df = pd.concat(dataframes, ignore_index=True)
    merged_df['Normalized Phone'] = merged_df['טלפון'].astype(str).apply(lambda x: x[-6:])

    

    merged_df['נוצר בתאריך'] = pd.to_datetime(merged_df['נוצר בתאריך'], format='%d/%m/%Y', errors='coerce')
    aggregations_corrected = {
        col: (
            lambda x: ', '.join(sorted(set(map(str, x.dropna()))))  # Convert all values to strings before joining
            if x.dtype == object and not x.dropna().empty else
            min(x.dropna()) if not x.dropna().empty and np.issubdtype(x.dtype, np.datetime64) else
            x.dropna().iloc[0] if not x.dropna().empty else
            np.nan
        )
        for col in merged_df.columns if col != 'Normalized Phone'
    }
    cleaned_data_corrected = merged_df.groupby('Normalized Phone').agg(aggregations_corrected).reset_index()

    cleaned_data_corrected['מנוי'] = cleaned_data_corrected.apply(
        lambda row: f"{row['חברות']}, {row['מנוי']}" if pd.notna(row['חברות']) and pd.notna(row['מנוי']) and row['חברות'] != row['מנוי']
        else row['חברות'] if pd.notna(row['חברות'])
        else row['מנוי'] if pd.notna(row['מנוי'])
        else np.nan,
        axis=1
    )   

    cleaned_data_corrected.drop('חברות', axis=1, inplace=True)
    
     # Clean "מקור" column
    cleaned_data_corrected['מקור'] = cleaned_data_corrected['מקור'].apply(
        lambda x: ', '.join(
            [item.strip().lower().replace('website', 'whatsapp') if item.isascii() else item.strip() for item in x.split(',') if item.strip() != 'ללא מקור']
        ) if isinstance(x, str) and any(item.strip() != 'ללא מקור' for item in x.split(',')) 
        else x.lower().replace('website', 'whatsapp') if isinstance(x, str) and x.isascii()
        else x
    )


    # Replace ages under 15 with the mean age
    if 'גיל' in cleaned_data_corrected.columns:
        mean_age = cleaned_data_corrected['גיל'].mean(skipna=True)
        cleaned_data_corrected['גיל'] = cleaned_data_corrected['גיל'].apply(
            lambda age: mean_age if age < 13 else age
        )

    cleaned_data_corrected['רלוונטי'] = np.where(
        cleaned_data_corrected['סטטוס'] == 'סומן כאבוד', 'לא',
        np.where(
            cleaned_data_corrected['סטטוס'].isna() | (cleaned_data_corrected['סטטוס'].str.strip() == ''), '', 'כן'
        )
    )
    
    if trial_leads is not None:
        trial_phones = set(trial_leads['טלפון'].astype(str).apply(lambda x: x[-6:]))  # Normalize phone numbers in trial_leads
        cleaned_data_corrected['עשו ניסיון'] = cleaned_data_corrected['Normalized Phone'].apply(lambda x: 'V' if x in trial_phones else '')


    sheets_data_dir = Path(resource_path('sheets_data'))
    sheets_data_dir.mkdir(parents=True, exist_ok=True)

    output_file = sheets_data_dir / 'cleaned_data_corrected.csv'
    cleaned_data_corrected.to_csv(output_file, index=False, encoding='utf-8-sig')

    return cleaned_data_corrected



def authenticate_gsheets(json_keyfile):
    # Authenticate with Google Sheets
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
    gc = gspread.authorize(credentials)
    return gc


def upload_to_gsheets(merged_df, gc, sheet_url):
    # Sort the table by create date
    merged_df = merged_df.sort_values(by='נוצר בתאריך', ascending=True)
    merged_df.reset_index(drop=True, inplace=True)

   # Check and convert all datetime columns to string format
    for col in merged_df.columns:
        if pd.api.types.is_datetime64_any_dtype(merged_df[col]):
            # Ensure all datetime data is converted to strings in the ISO 8601 format.
            merged_df[col] = merged_df[col].dt.strftime('%d/%m/%Y') if merged_df[col].notna().any() else merged_df[col]

    # Replace infinite and NaN values with None for JSON serialization
    merged_df = merged_df.replace([float('inf'), -float('inf'), float('nan')], None)
    
    worksheet = gc.open_by_url(sheet_url).sheet1
    worksheet.clear()

    # Convert DataFrame to list of lists, replacing NaN with None for JSON serialization
    data = [merged_df.columns.tolist()] + merged_df.where(pd.notnull(merged_df), None).values.tolist()
    worksheet.update(data)
    
    # Formatting the header
    header_format = {
        "textFormat": {"bold": True, "fontSize": 12, "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}},
        "backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.5},
        "horizontalAlignment": "CENTER",
        "verticalAlignment": "MIDDLE"
    }
    
    end_col_letter = column_to_letter(len(merged_df.columns))
    start_range = 'A1'
    end_range = gspread.utils.rowcol_to_a1(len(merged_df) + 1, len(merged_df.columns))
    worksheet.set_basic_filter(f'{start_range}:{end_range}')
    worksheet.format(f'A1:{end_col_letter}1', header_format)

# directory = os.getenv('DIRECTORY')
# json_keyfile = os.getenv('JSON_KEYFILE')
# sheet_url = os.getenv('SHEET_URL')

# gc = authenticate_gsheets(json_keyfile)
# merged_df = merge_csv_files(directory)

# column_order = ['נוצר בתאריך', 'שם', 'טלפון',  'מקור', 'סטטוס', 'סיבות התנגדות', 'מפגש ניסיון','עשו ניסיון', 'רלוונטי' ,'מנוי', 'גיל', 'קובץ מקור']
# final = set_column_order(merged_df, column_order)
# upload_to_gsheets(final, gc, sheet_url)
