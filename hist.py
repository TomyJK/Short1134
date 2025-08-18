from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import tkinter as tk
from tkinter import messagebox
from kiteconnect.exceptions import InputException
# from check import access_token1
from get_latest_token import get_access_token

# Initialize KiteConnect
# start_time = time.time()
api_key = 'hwdft0qevt0p4vxb'
access_token = get_access_token()
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)


def fetch_historical_data(instrument_token, from_date, to_date, interval='5minute'):
    try:
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )
        return data
    except InputException as e:
        # print(f"Error fetching data for token{instrument_token}: {e}. Invalid token or parameters.")
        return []  # Return an empty list to allow the process to continue
    except Exception as e:  # Catch other potential errors
        # print(f"An unexpected error occurred for token {instrument_token}: {e}")
        return []


def fetch_data_range(instrument_token, start_date, end_date, interval='5minute'):
    all_data = []
    while start_date < end_date:
        next_date = min(start_date + timedelta(days=90), end_date)
        from_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        to_date_str = next_date.strftime('%Y-%m-%d %H:%M:%S')
        data_chunk = fetch_historical_data(instrument_token, from_date_str, to_date_str, interval)
        if data_chunk:
         all_data.extend(data_chunk)
        start_date = next_date
    return all_data

def get_ohlc(stock_df,blank_df):

    global stock_df_count
    stock_df_count = 0
    stock1_df = stock_df
    stocklist_len = len(stock_df)
    count_del = 0

    for i in range(1):
        counter = 0
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        start_year = today.year
        start_month = today.month
        start_day = today.day
        yester_day = yesterday.day
        end_year = yesterday.year
        tomorrow = today + timedelta(days=1)
        end_day = tomorrow.day
        end_month = tomorrow.month
        current_time = datetime.now()
        target_time = current_time.replace(hour=9, minute=15, second=0, microsecond=0)
        if current_time >= target_time:
           pass
        else:
            start_day = yester_day
            start_month = yesterday.month
        start_date = datetime(start_year,start_month,start_day)
        end_date = datetime(end_year,end_month,end_day)
        print(start_date)
        print(end_date)
        # Loop through each stock in the list
        all_datas = []
        for index, row in stock_df.iterrows():
            instrument_token = row["token"]
            symbol = row["symbol"]


            historical_data = fetch_data_range(instrument_token, start_date, end_date)
            df0 = pd.DataFrame(historical_data)

            # if df0.empty:
            #     print(f"No data fetched for {symbol}")
            if historical_data:
                for candle_data in historical_data:
                    candle_data['symbol'] = symbol
            counter += 1
            # print(f'{counter} stocks data has been downloaded.')
            all_datas.extend(historical_data)
            # df1 = pd.DataFrame(all_datas)

            # if counter == 1000:
                # print(df1.to_string())

            # else:
            #     print(f"  No data fetched or error for {symbol}.")
            # df = pd.DataFrame(historical_data)

            # if 'date' in df.columns:
            #     df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
                # file_path = f'C:\\Users\\HP\\Desktop\\Downloads\\stocks\\{symbol}.xlsx'
                # df.to_excel(file_path, index=False)
                # print(f"Data saved to {file_path}")
                # counter += 1
                # print(f'{counter} stocks data has been downloaded.')
                # all_datas.extend(historical_data)


            # else:
            #     print(f"No data fetched for {symbol}")
        # print(all_datas)
        df1 = pd.DataFrame(all_datas)
        if not df1.empty:
            df1['date'] = pd.to_datetime(df1['date']).dt.tz_localize(None)
            existing_columns = [col for col in df1.columns if col != 'symbol']
            desired_column_order = ['symbol'] + existing_columns
            df1 = df1.reindex(columns=desired_column_order)
            con_df = pd.concat([blank_df,df1], ignore_index=True)
            blank_df = con_df
        else:
            # print("No new data was successfully fetched and added to df1.")
            pass



        stock_df = get_missing(stock1_df, blank_df)
        if stock_df.empty:
            break

    blank_df = blank_df.sort_values(by=['symbol', 'date'])
    # print(blank_df.to_string())
    if not stock_df.empty:

        stock_df_count = len(stock_df)
        # print(f'Number of stocks with missing data: {stock_df_count}')
        # print(stock_df.to_string())

    return blank_df, stock_df_count,stock_df,stocklist_len

def delete_symbol(stock_df, blank_df):

    if blank_df.empty:
        print("blank_df is empty. No data to delete.")
        return blank_df

    if stock_df.empty:
        print("stock_df is empty. Cannot select random symbols to delete.")
        return blank_df

    symbols_in_blank_df = blank_df['symbol'].unique().tolist()
    available_symbols_for_deletion = [s for s in stock_df['Symbol'].unique().tolist() if s in symbols_in_blank_df]


    if len(available_symbols_for_deletion) >= 3:
        # Select 3 random symbols from the available ones
        symbols_to_delete = random.sample(available_symbols_for_deletion, 3)
        print(f"\nAttempting to delete data for random symbols: {symbols_to_delete}")

        # Filter blank_df to remove rows corresponding to the selected symbols
        if 'symbol' in blank_df.columns:
            # Create a new DataFrame without the specified symbols to avoid SettingWithCopyWarning
            modified_blank_df = blank_df[~blank_df['symbol'].isin(symbols_to_delete)].copy()
            # Calculate how many rows were removed
            rows_removed = len(blank_df) - len(modified_blank_df)
            print(f"Successfully deleted {rows_removed} rows for the specified symbols.")
            return modified_blank_df
        else:
            print("Warning: 'symbol' column not found in blank_df. Cannot delete specified symbols.")
            return blank_df
    elif len(available_symbols_for_deletion) > 0:
        print(f"Not enough common symbols between stock_df and blank_df to pick 3 random ones. Found only {len(available_symbols_for_deletion)} symbols that could be deleted.")
        return blank_df
    else:
        print("No common symbols found between stock_df and blank_df. Nothing to delete.")
        return blank_df

def get_missing(stock_df: pd.DataFrame, ohlc_df: pd.DataFrame) -> pd.DataFrame:

    # 1. Basic validation of input DataFrames and required columns
    if stock_df.empty or 'symbol' not in stock_df.columns or 'token' not in stock_df.columns:
        print("Error: stock_df is empty or missing 'symbol' or 'token' columns.")
        return pd.DataFrame(columns=['symbol', 'token'])

    if ohlc_df.empty or 'symbol' not in ohlc_df.columns:
        # If ohlc_df is empty or lacks 'symbol' column, all symbols from stock_df are "missing"
        print("Warning: ohlc_df is empty or missing 'symbol' column. Returning all symbols from stock_df.")
        return stock_df[['symbol', 'token']].copy()

    # 2. Get unique symbols from both DataFrames
    all_stock_symbols = set(stock_df['symbol'].unique())
    ohlc_symbols = set(ohlc_df['symbol'].unique())

    # 3. Find symbols that are in stock_df but not in ohlc_df
    missing_symbols = all_stock_symbols - ohlc_symbols

    if not missing_symbols:
        print("No symbols found in stock_df that are missing from ohlc_df.")
        return pd.DataFrame(columns=['symbol', 'token'])

    # 4. Filter the original stock_df to get the Symbol and Token for the missing symbols
    missing_info_df = stock_df[stock_df['symbol'].isin(list(missing_symbols))][['symbol', 'token']].copy()

    # print(f"Found {len(missing_symbols)} symbols in stock_df that are not in ohlc_df.")
    return missing_info_df

def show_info(mess):
    """Displays a simple informational message box."""
    # Create the main window, but hide it as we only need the messagebox
    root = tk.Tk()
    root.withdraw() # This hides the main window

    # Display the message box
    messagebox.showinfo("Information", mess)

    # Destroy the root window after the messagebox is closed
    root.destroy()

def count_datarows(df):
    symbol_counts = df.groupby('symbol').size()
    symbols_less75rows = symbol_counts[symbol_counts < 75]
    print("--- Symbols with LESS THAN 75 rows of data ---")
    print(f'Number of stocks without full 75 rows: {len(symbols_less75rows)}')
    if not symbols_less75rows.empty:
        print(symbols_less75rows)
    else:
        print("All symbols have 75 or more rows of data.")
    print("\n" + "=" * 30 + "\n")