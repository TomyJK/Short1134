from datetime import datetime
import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import GeneralException, TokenException, InputException
import requests.exceptions
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import time




api_key = 'hwdft0qevt0p4vxb'
access_token = 'sR97GSOj4v1Q23h0uecZngMBRcyEAjfy'

try:
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    profile = kite.profile()
    print("KiteConnect client initialized and access token set successfully!")

except TokenException as e:
    print(f"Authentication Failed (TokenException): {e}")
    print("Your access token might be invalid or expired. Please re-authenticate.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred during KiteConnect initialization: {e}")
    exit()




scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

# creds_file = r'C:\Users\HP\PycharmProjects\credentials.json'
creds_file = r'C:\Users\HP\Desktop\Downloads\credentials.json'
sheet_name = "Consolidated list of Scrips - Allowed by RMS for Intraday"

# --- Main Script Logic ---
def fetch_and_process_symbols():
    try:

        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)

        # Open the Google Sheet by its exact title
        # .sheet1 accesses the first worksheet in the spreadsheet
        # If your data is on a different sheet (e.g., "Sheet2"), use .worksheet("Sheet2")
        # TEMP DEBUGGING BLOCK - Check accessible sheets
        sheets = client.openall()
        print("\nAccessible spreadsheets:")
        for s in sheets:
            print(" -", s.title)

        sheet = client.open(sheet_name).worksheet("MIS")

        # Get all values from the sheet
        # This returns a list of lists, where each inner list is a row.
        data = sheet.get_all_values()

        # Extract the stock symbols
        # Assumes symbols are in the first column (index 0) and the first row is a header.
        # [row[0] for row in data[1:] if row] means:
        # - data[1:]: Start from the second row (skipping header)
        # - if row: Ignore any completely empty rows
        # - row[0]: Take the first element (column) of each row
        stock_symbols = [row[0].strip() for row in data[1:] if row and row[0].strip()] # .strip() to remove whitespace

        print(f"--- Fetched Stock Symbols ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---")
        print("Raw Symbols List:", stock_symbols)

        # Create a Pandas DataFrame for easier handling and analysis (optional but recommended)
        df = pd.DataFrame({'Symbol': stock_symbols})
        print("\nDataFrame:")
        print(df)

        # --- You can add code here to save your DataFrame ---
        # Example: Save to a CSV file
        # df.to_csv('intraday_symbols.csv', index=False)
        # print("\nSymbols saved to intraday_symbols.csv")

        # Example: Save to an Excel file
        # df.to_excel('intraday_symbols.xlsx', index=False)
        # print("\nSymbols saved to intraday_symbols.xlsx")

        return df # Return the DataFrame if you want to use it further

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Google Sheet '{sheet_name}' not found. Check the name and sharing permissions.")
    except gspread.exceptions.NoValidUrlKeyFound:
        print("Error: Invalid URL or key for the Google Sheet. Double-check your sheet name/URL.")
    except Exception as e:
        print(f"An unexpected error occurred during fetching: {e}")
        print("Please ensure:")
        print("  - Your 'creds_file' path is correct.")
        print("  - The service account email is shared as 'Viewer' with the Google Sheet.")
        print("  - Both Google Sheets API and Google Drive API are enabled for your project.")
    return None

# --- Schedule for Daily Updates ---
# This part makes the script run continuously and update at a specific time.
# For production, consider using system-level schedulers (cron, Windows Task Scheduler).

# Define the target time for updates (e.g., 9:00 AM)
# Adjust these values (hour, minute) as needed for your desired update time.
TARGET_HOUR = 12
TARGET_MINUTE = 6

# A flag to ensure the update only runs once per target time
updated_today = False

print("\n--- Script started. Waiting for update time... ---")

while True:
    now = datetime.datetime.now()

    # Check if it's the target time and if we haven't updated yet today
    if now.hour == TARGET_HOUR and now.minute == TARGET_MINUTE and not updated_today:
        print(f"\n--- It's {TARGET_HOUR:02d}:{TARGET_MINUTE:02d}. Fetching updated symbols... ---")
        fetched_df = fetch_and_process_symbols()
        if fetched_df is not None:
            # You can now use 'fetched_df' for further processing in your script
            pass # Or do something with the updated DataFrame
        updated_today = True # Mark as updated for today

    # Reset the flag at the beginning of a new day (after TARGET_HOUR)
    if now.hour > TARGET_HOUR or (now.hour == TARGET_HOUR and now.minute > TARGET_MINUTE):
        updated_today = False

    # Wait for a short interval before checking the time again
    # Use a shorter sleep time (e.g., 60 seconds) to be more precise about the update time,
    # but be mindful of CPU usage for very frequent checks.
    # For daily updates, you could sleep for longer periods (e.g., 5-10 minutes)
    time.sleep(10) # Sleep for 60 seconds (1 minute)