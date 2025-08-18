from datetime import datetime
import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import GeneralException, TokenException, InputException
import requests.exceptions
import os
import time
from config import downloads_path
# from check import access_token1
from s_fun import download_gsheet
from s_fun import create_stocks_file
from get_latest_token import get_access_token

download_gsheet()
create_stocks_file()


api_key = 'hwdft0qevt0p4vxb'
access_token = get_access_token()

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

# --- File Paths ---
downloads_path = downloads_path
file_name = "stocks.xlsx"
STOCKS_FILE_PATH = os.path.join(downloads_path, file_name)

# Ensure the Downloads directory exists
os.makedirs(downloads_path, exist_ok=True)

# --- Helper Function for Retries ---
def get_kite_instruments_with_retry(kite_client, exchange: str = "NSE", max_retries: int = 5, initial_delay_sec: float = 0.3, initial_timeout_sec: int = 5):
    """
    Attempts to download the latest instrument list from Kite API with retries on failure.

    Args:
        kite_client: An authenticated KiteConnect object.
        exchange (str): The exchange to filter instruments for (e.g., "NSE", "BSE").
        max_retries (int): Maximum number of retry attempts.
        initial_delay_sec (float): Initial delay between retries in seconds.
        initial_timeout_sec (int): Initial timeout for the API call in seconds.

    Returns:
        list: A list of instrument dictionaries from Kite, or an empty list if all retries fail.
    """
    current_delay = initial_delay_sec
    current_timeout = initial_timeout_sec

    for attempt in range(1, max_retries + 1):
        print(f"Attempt {attempt}/{max_retries}: Downloading instrument list for {exchange} (Timeout: {current_timeout}s)...")
        try:

            instruments_list = kite_client.instruments(exchange=exchange)

            print(f"Successfully downloaded instrument list on attempt {attempt}.")
            return instruments_list

        except requests.exceptions.ReadTimeout as e:
            print(f"  Attempt {attempt} failed: Read timed out. Retrying in {current_delay:.2f} seconds...")
        except requests.exceptions.ConnectionError as e:
            print(f"  Attempt {attempt} failed: Connection error. Retrying in {current_delay:.2f} seconds...")
        except GeneralException as e:
            print(f"  Attempt {attempt} failed (KiteConnect General Error): {e}. Retrying in {current_delay:.2f} seconds...")
        except TokenException as e:
            print(f"  Authentication error (TokenException): {e}. Cannot retry. Please re-authenticate your main client.")
            return [] # Exit if authentication is the issue
        except InputException as e:
            print(f"  API Input error (InputException): {e}. Cannot retry as it's likely a bad request.")
            return [] # Exit if it's an input issue
        except Exception as e:
            print(f"  Attempt {attempt} failed with unexpected error: {e}. Retrying in {current_delay:.2f} seconds...")

        if attempt < max_retries:
            time.sleep(current_delay)
            # Exponential backoff: double the delay for the next attempt
            current_delay *= 2
            # Optionally, slightly increase timeout for subsequent retries if still timing out
            current_timeout += 5
        else:
            print(f"  All {max_retries} attempts failed to download instrument list.")
    return [] # Return empty list if all retries fail



def update_tokens(kite_client, stocks_file_path: str, exchange: str = "NSE") -> pd.DataFrame:
    """
    Loads symbols from a local XLSX, downloads fresh instrument tokens from Kite API,
    and returns an updated DataFrame. Excludes symbols without valid tokens.

    Args:
        kite_client: An authenticated KiteConnect object.
        stocks_file_path (str): Full path to the XLSX file containing 'Symbol' and 'Token' columns.
        exchange (str): The exchange to filter instruments for (e.g., "NSE", "BSE").

    Returns:
        pd.DataFrame: A DataFrame with 'Symbol' and 'Token' for valid, updated instruments.
                      Returns an empty DataFrame if an error occurs.
    """
    print(f"Loading local stocks from: {stocks_file_path}")
    local_stocks_df = pd.DataFrame(columns=['symbol', 'token']) # Initialize empty DataFrame

    try:
        if not os.path.exists(stocks_file_path):
            print(f"Warning: Local stocks file '{stocks_file_path}' not found. Starting with an empty list for merge.")
            # If you want to create a dummy file if not found for initial run, uncomment below:
            # pd.DataFrame(columns=['Symbol', 'Token']).to_excel(stocks_file_path, index=False, engine='openpyxl')
        else:
            local_stocks_df = pd.read_excel(stocks_file_path, engine='openpyxl')
            if 'symbol' not in local_stocks_df.columns: # Token column might not be there if it's a list of just symbols
                raise ValueError("Excel file must contain at least a 'symbol' column.")
            local_stocks_df['symbol'] = local_stocks_df['symbol'].astype(str).str.upper() # Standardize symbol case and ensure string
            print(f"Loaded {len(local_stocks_df)} symbols from local file.")
            print("Local Stocks Head:\n", local_stocks_df.head())

    except Exception as e:
        print(f"Error reading local stocks file: {e}. Returning empty DataFrame.")
        return pd.DataFrame(columns=['symbol', 'token'])

    # --- Downloading latest instrument list from Kite API with retry logic ---
    kite_instruments_list = get_kite_instruments_with_retry(kite_client, exchange=exchange)

    if not kite_instruments_list:
        print("Failed to download instruments after multiple retries. Cannot update tokens.")
        return pd.DataFrame(columns=['symbol', 'token']) # Return empty if no instruments from API

    kite_instruments_df = pd.DataFrame(kite_instruments_list)

    # Filter for the specific exchange (if not already filtered by API call)
    if not kite_instruments_df.empty:
        kite_instruments_df = kite_instruments_df[kite_instruments_df['exchange'] == exchange]
    file_names = "instruments.xlsx"
    instruments_filepath = os.path.join(downloads_path, file_names)
    kite_instruments_df.to_excel(instruments_filepath, index=False, engine='openpyxl')

    # Select relevant columns and rename for merging
    # Ensure these columns exist before attempting to select
    required_kite_cols = ['tradingsymbol', 'instrument_token']
    if not all(col in kite_instruments_df.columns for col in required_kite_cols):
        print(f"Error: Missing required columns in Kite instruments data. Expected: {required_kite_cols}")
        return pd.DataFrame(columns=['symbol', 'token'])

    kite_instruments_df = kite_instruments_df[required_kite_cols].copy()
    kite_instruments_df.rename(columns={
        'tradingsymbol': 'symbol',
        'instrument_token': 'token'
    }, inplace=True)
    kite_instruments_df['symbol'] = kite_instruments_df['symbol'].astype(str).str.upper() # Standardize symbol case and ensure string

    # Ensure 'Token' is numeric, coerce errors will turn non-numeric into NaN
    kite_instruments_df['token'] = pd.to_numeric(kite_instruments_df['token'], errors='coerce')
    # Drop rows where Token could not be converted to a number (i.e., invalid tokens)
    kite_instruments_df.dropna(subset=['token'], inplace=True)
    # Convert tokens to integer type for consistency
    kite_instruments_df['token'] = kite_instruments_df['token'].astype(int)

    print(f"Processed {len(kite_instruments_df)} valid instruments from Kite API for {exchange}.")
    # print("Kite Instruments Head:\n", kite_instruments_df.head()) # Uncomment for debugging

    # --- Merge the two DataFrames ---
    # We'll perform a left merge from local_stocks_df to kite_instruments_df
    merged_df = pd.merge(
        local_stocks_df[['symbol']], # Only bring Symbol from local_stocks_df to ensure we match only those
        kite_instruments_df,
        on='symbol',
        how='left'
    )
    # print("Merged DataFrame (with potential NaNs):\n", merged_df.to_string()) # Uncomment for debugging

    # --- Identify symbols without valid tokens ---
    # A symbol won't have a token if:
    # 1. It wasn't found in the Kite instruments list for the specified exchange.
    # 2. Its token was non-numeric/invalid after Kite's response.
    missing_tokens_df = merged_df[merged_df['token'].isnull()]

    if not missing_tokens_df.empty:
        print(f"\n--- {len(missing_tokens_df)} Symbols without valid Tokens (excluded) ---")
        for symbol in missing_tokens_df['symbol']:
            print(f"- {symbol}")
    else:
        print("\nAll symbols from your local file found valid tokens in the Kite API list.")

    # --- Exclude symbols that don't have tokens and finalize ---
    updated_stocks_df = merged_df.dropna(subset=['token']).reset_index(drop=True)
    updated_stocks_df['token'] = updated_stocks_df['token'].astype(int) # Ensure token is integer

    print(f"\nSuccessfully updated tokens for {len(updated_stocks_df)} symbols.")
    # print("Updated Stocks DataFrame Head:\n", updated_stocks_df.head()) # Uncomment for debugging

    return updated_stocks_df



# --- Main Script Execution ---
if __name__ == "__main__":
    # Call the function to get the updated DataFrame
    print("\n--- Starting Token Update Process ---")
    updated_symbols_df = update_tokens(kite, STOCKS_FILE_PATH, exchange="NSE")

    # --- Code to save the updated DataFrame ---
    if not updated_symbols_df.empty:
        timestamp = datetime.now().strftime("%Y%m%d") # Format for date only
        output_filename = f"stocks_updated_{timestamp}.xlsx"
        output_file_path = os.path.join(downloads_path, output_filename)

        try:
            updated_symbols_df.to_excel(output_file_path, index=False, engine='openpyxl')
            print(f"\nSuccessfully saved updated symbols to: {output_file_path}")
            print(f"Total updated symbols saved: {len(updated_symbols_df)}")
        except Exception as e:
            print(f"Error saving updated symbols to Excel: {e}")
    else:
        print("\nNo updated symbols to save as the DataFrame is empty.")

    print("\n--- Token Update Process Complete ---")