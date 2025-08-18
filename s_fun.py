import os
import pandas as pd
from datetime import datetime, timedelta
from config import save_to_excel
import time
import requests
pd.set_option('future.no_silent_downcasting', True)

def load_excel(filename):
    downloads_path = r"C:\Users\HP\Desktop\Downloads"
    file_path = os.path.join(downloads_path, filename)
    try:
        df = pd.read_excel(file_path)
        print(f"Loaded '{filename}' successfully!")
        return df
    except Exception as e:
        print(f"Error loading file: {e}")
        return None

def find_amount(df):

    global top_stock_name
    ONE_CRORE = 10_000_000
    df['date'] = pd.to_datetime(df['date'])
    numeric_cols = ['open', 'close', 'volume', 'high', 'low']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df_clean = df.dropna(subset=numeric_cols)
    df_clean = df_clean[
        (df_clean['open'] > 0) &
        (df_clean['close'] > 0) &
        (df_clean['volume'] > 0) &
        (df_clean['high'] > 0) &
        (df_clean['low'] > 0)
        ]
    valid_stocks_counts = df_clean['symbol'].value_counts()
    valid_stocks = valid_stocks_counts[valid_stocks_counts >= 2].index
    df_valid = df_clean[df_clean['symbol'].isin(valid_stocks)].copy()
    df_915 = df_valid[df_valid['date'].dt.time == pd.to_datetime("09:15:00").time()].copy()
    df_915['screening_trade_value'] = (df_915['volume'] * ((df_915['open'] + df_915['close']) / 2)) / ONE_CRORE
    high_value_stocks = df_915[
        (df_915['screening_trade_value'] >= 25) &  # 25 Crore
        (df_915['screening_trade_value'] <= 2000)  # 2000 Crore
        ]['symbol']
    result_df = df_valid[df_valid['symbol'].isin(high_value_stocks)].sort_values(['symbol', 'date']).copy()
    result_df['trade_value'] = (result_df['volume'] * ((result_df['open'] + result_df['close']) / 2)) / ONE_CRORE
    result_df['trade_value'] = result_df['trade_value'].round(2)
    result_df['diff915'] = pd.NA
    result_df['diff920'] = pd.NA
    for stock_name, group in result_df.groupby('symbol'):
        if len(group) >= 2:  # Ensure we have both 9:15 and 9:20 rows
            row_915 = group.iloc[0]
            row_920 = group.iloc[1]
            open_915 = row_915['open']
            close_915 = row_915['close']
            if open_915 != 0:
                pct_diff_915_oc = ((close_915 - open_915) / open_915) * 100
                result_df.loc[row_915.name, 'diff915'] = pct_diff_915_oc
            else:
                result_df.loc[row_915.name, 'diff915'] = 0.0
            close_920 = row_920['close']
            if close_920 != 0:
                pct_diff_915_to_920_oc = ((close_920 - open_915) / open_915) * 100
                result_df.loc[row_920.name, 'diff920'] = pct_diff_915_to_920_oc
            else:
                result_df.loc[row_920.name, 'diff920'] = 0.0
    result_df['diff915'] = result_df['diff915'].fillna(0.00)
    result_df['diff920'] = result_df['diff920'].fillna(0.00)
    result_df[['diff915', 'diff920']] = result_df[['diff915', 'diff920']].infer_objects(copy=False)
    result_df['diff915'] = result_df['diff915'].round(2)
    result_df['diff920'] = result_df['diff920'].round(2)

    # Step 1: Create two DataFrames, one for 9:15 and one for 9:20
    df_915 = result_df[result_df['date'].dt.time == pd.to_datetime("09:15:00").time()]
    df_920 = result_df[result_df['date'].dt.time == pd.to_datetime("09:20:00").time()]

    # Step 2: Merge on 'symbol' to get both times' data side by side
    merged = pd.merge(
        df_915[['symbol', 'diff915']],
        df_920[['symbol', 'diff920']],
        on='symbol',
        how='inner'
    )

    # Step 3: Filter stocks based on the conditions
    stocks_to_remove = merged[
        (merged['diff915'] < -1) &
        ((merged['diff920'] < -7) | (merged['diff920'] > -0.1))
        ]['symbol'].unique()

    final_result_df = result_df[~result_df['symbol'].isin(stocks_to_remove)].copy()

    diff915_for_stocks = final_result_df.loc[
        final_result_df['date'].dt.time == pd.to_datetime("09:15:00").time(),
        ['symbol', 'diff915']
    ].set_index('symbol')['diff915']

    status_map = {}
    for stock_name, diff_val in diff915_for_stocks.items():

        if pd.isna(
                diff_val):  # If diff915 is NA (e.g., if open was zero and resulted in NA earlier, though mostly filtered)
            status_map[stock_name] = "green"  # NA is not less than -1
        elif diff_val < -1:
            status_map[stock_name] = "red"
        else:  # diff_val is >= -1
            status_map[stock_name] = "green"

    final_result_df['trade_status'] = final_result_df['symbol'].map(status_map)

    stocks_to_remove_by_red_status_and_diff920 = final_result_df[
        (final_result_df['trade_status'] == "red") &  # Check for 'red' status
        (final_result_df['date'].dt.time == pd.to_datetime("09:20:00").time()) &  # Look only at 9:20 rows
        (final_result_df['diff920'] > -0.1)  # Check if diff920 is greater than -0.1
        ]['symbol'].unique()  # Get unique stock names that meet these criteria
    final_result_df = final_result_df[~final_result_df['symbol'].isin(stocks_to_remove_by_red_status_and_diff920)].copy()
    sorted_df_asc = final_result_df[final_result_df['date'].dt.time == pd.to_datetime("09:15:00").time()]
    sorted_df_asc = sorted_df_asc.sort_values(by='trade_value')
    save_to_excel(sorted_df_asc,'final_result')
    if not sorted_df_asc.empty:
        idx_max_trade_value = sorted_df_asc['trade_value'].idxmax()
        top_stock_name = sorted_df_asc.loc[idx_max_trade_value, 'symbol']

        print(f"\n--- Top Traded Value Stock: {top_stock_name} ---")
        # Print all rows for this specific stock
        # print(final_result_df[final_result_df['symbol'] == top_stock_name].to_string())
    else:
        print("\nNo stocks qualified based on the given criteria to find a top traded value stock.")

    return sorted_df_asc, top_stock_name

def find_amount_org(df):
    downloads_folder = r"C:\Users\HP\Desktop\Downloads"
    ONE_CRORE = 10_000_000
    df['date'] = pd.to_datetime(df['date'])
    numeric_cols = ['open', 'close', 'volume', 'high', 'low']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df_clean = df.dropna(subset=numeric_cols)
    df_clean = df_clean[
        (df_clean['open'] > 0) &
        (df_clean['close'] > 0) &
        (df_clean['volume'] > 0) &
        (df_clean['high'] > 0) &
        (df_clean['low'] > 0)
        ]
    valid_stocks_counts = df_clean['symbol'].value_counts()
    valid_stocks = valid_stocks_counts[valid_stocks_counts >= 1].index
    df_valid = df_clean[df_clean['symbol'].isin(valid_stocks)].copy()
    df_915 = df_valid[df_valid['date'].dt.time == pd.to_datetime("09:15:00").time()].copy()
    df_915['screening_trade_value'] = (df_915['volume'] * ((df_915['open'] + df_915['close']) / 2)) / ONE_CRORE
    high_value_stocks = df_915[
        (df_915['screening_trade_value'] >= 25) &  # 25 Crore
        (df_915['screening_trade_value'] <= 2000)  # 2000 Crore
        ]['symbol']
    result_df = df_valid[df_valid['symbol'].isin(high_value_stocks)].sort_values(['symbol', 'date']).copy()
    result_df['trade_value'] = (result_df['volume'] * ((result_df['open'] + result_df['close']) / 2)) / ONE_CRORE
    result_df['trade_value'] = result_df['trade_value'].round(2)
    result_df_filtered = result_df[result_df['date'].dt.time == pd.to_datetime("09:15:00").time()].copy()
    result_df_filtered = result_df_filtered.sort_values(by='trade_value', ascending=False)
    if not result_df_filtered.empty:
        print("✅Top trade Value stocks:")
        print(result_df_filtered.head(5).to_string())
    else:
        print("There is no stocks for trade.")

    try:
        file_names = "amount_filtered.xlsx"
        instruments_filepath = os.path.join(downloads_folder, file_names)
        result_df_filtered.to_excel(instruments_filepath, index=False, engine='openpyxl')
    except Exception as e:
        print(f"\nError saving new_df to file: {e}")



    return result_df

def filtered_symbols(result_df):
    downloads_folder = r"C:\Users\HP\Desktop\Downloads"
    timestamp = datetime.now().strftime("%Y%m%d")
    token_file_name = f"stocks_updated_{timestamp}.xlsx"  # Change this if the file has a different name
    token_file_path = os.path.join(downloads_folder, token_file_name)


    try:
        # Load the instruments Excel file
        instruments_df = pd.read_excel(token_file_path, engine='openpyxl')



        # Rename to match your symbols
        # if 'tradingsymbol' in instruments_df.columns:
        #     instruments_df.rename(columns={'tradingsymbol': 'symbol'}, inplace=True)

        # Get unique symbols from result_df
        unique_symbols = result_df['symbol'].unique()

        # Filter instrument tokens for matching symbols
        matched_df = instruments_df[instruments_df['symbol'].isin(unique_symbols)][['symbol', 'token']].copy()

        # Drop duplicates if any
        matched_df = matched_df.drop_duplicates()

        # print("\nMatched symbols and tokens:")
        # print(matched_df.to_string())
        print(f'Number of eligible stocks filtered first round: {len(matched_df)}')

        return matched_df

    except Exception as e:
        print(f"\nError reading or matching token file: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# if __name__ == "__main__":
#     sample_df = load_excel('ohlc2.xlsx')
#     sample_df1 = find_amount(sample_df)


def time_pause1():

    target_time_str = "09:20:05"
    target_time = datetime.strptime(target_time_str, "%H:%M:%S").time()

    now = datetime.now()


    target_datetime = datetime.combine(now.date(), target_time)

    if now > target_datetime:
        pass
    else:

        # Calculate how many seconds to sleep
        sleep_seconds = (target_datetime - now).total_seconds()

        print(f"Waiting until {target_datetime.strftime('%Y-%m-%d %H:%M:%S')} ({int(sleep_seconds)} seconds)...")
        time.sleep(sleep_seconds)

    # Your main script starts here
    print("It's 9:20:05! Running the script now...")


def time_pause2():

    target_time_str = "09:25:02"
    target_time = datetime.strptime(target_time_str, "%H:%M:%S").time()

    now = datetime.now()


    target_datetime = datetime.combine(now.date(), target_time)

    if now > target_datetime:
        pass
    else:

        # Calculate how many seconds to sleep
        sleep_seconds = (target_datetime - now).total_seconds()

        print(f"Waiting until {target_datetime.strftime('%Y-%m-%d %H:%M:%S')} ({int(sleep_seconds)} seconds)...")
        time.sleep(sleep_seconds)

    # Your main script starts here
    print("It's 9:25:02! Running the script now...")


def download_gsheet():
    # Public Google Sheet ID
    sheet_id = "1XwWNCASDmrXfx5LtFNna0Kmkt5vHtqkjICvVcUZaQhw"
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

    # Today's date
    today = datetime.now().strftime("%Y%m%d")
    filename = f"Gsheet_{today}.xlsx"

    # Path to Downloads folder
    downloads_folder = r"C:\Users\HP\Desktop\Downloads"
    file_path = os.path.join(downloads_folder, filename)


    # Download and save
    response = requests.get(export_url)

    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"✅ File saved as: {file_path}")
    else:
        print("❌ Failed to download the sheet.")

def create_stocks_file():
    downloads_path = r"C:\Users\HP\Desktop\Downloads"
    today = datetime.now().strftime("%Y%m%d")
    filename = f"Gsheet_{today}.xlsx"

    # Full file paths
    file_with_margins = f"{downloads_path}\\{filename}"
    all_symbols_file = f"{downloads_path}\\all_symbols.xlsx"

    # Load the Excel with margin data
    df_margins = pd.read_excel(file_with_margins)

    # Rename the relevant columns only
    df_margins = df_margins.rename(columns={
        "Stocks allowed for MIS": "symbol",
        "Margin allowed": "margin"
    })

    # Load the all_symbols file (assume only one column: symbol)
    df_all_symbols = pd.read_excel(all_symbols_file)
    df_all_symbols.columns = ['symbol']  # force rename for consistency

    # Filter only rows where margin is '5x'
    df_5x = df_margins[df_margins['margin'].astype(str).str.strip().str.lower() == '5x']

    # Match symbols from all_symbols
    filtered_df = df_all_symbols[df_all_symbols['symbol'].isin(df_5x['symbol'])]

    # Output path
    output_file = f"{downloads_path}\\stocks.xlsx"
    filtered_df.to_excel(output_file, index=False)

    print("✅ Done! File 'stocks.xlsx' is saved in your Downloads folder.")

# download_gsheet()











