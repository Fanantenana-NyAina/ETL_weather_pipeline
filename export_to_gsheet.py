import sys
from google.oauth2.service_account import Credentials
import gspread

def export_dataframe_to_gsheet(df, spreadsheet_name, worksheet_name, creds_json_path):
    """
    Export a DataFrame to a specific Google Sheet worksheet.
    """
    
    # Needed Permerssions (edition & access)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Connection by using the service account JSON file
    credentials = Credentials.from_service_account_file(creds_json_path, scopes=scope)
    gc = gspread.authorize(credentials)

    # Opening spreadsheet
    spreadsheet = gc.open(spreadsheet_name)

    # Try to open worksheet or create it (if exist clear() if not create())
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="30")

    # Preparing the data: first the headers, then all rows
    data = [df.columns.values.tolist()] + df.values.tolist()

    # Write the data starting at cell A1
    worksheet.update("A1", data)

    # confirmation message
    print(f"✅ Exported to Google Sheet: {spreadsheet_name} / {worksheet_name}")
