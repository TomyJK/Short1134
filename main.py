import os
from datetime import datetime
from s_fun import find_amount_org, filtered_symbols, find_amount, time_pause1, time_pause2
import pandas as pd
import time
from config import save_to_excel
from hist import get_ohlc

time_pause1()

timestamp = datetime.now().strftime("%Y%m%d")
input_filename = f"stocks_updated_{timestamp}.xlsx"
# input_filename = "stocks.xlsx"
downloads_folder = r"C:\Users\HP\Desktop\Downloads"
input_file_path = os.path.join(downloads_folder, input_filename)

symbol_df = pd.read_excel(input_file_path)

ohlc_cols = ['symbol','date', 'open', 'high', 'low', 'close', 'volume']
ohlc_df = pd.DataFrame(columns=ohlc_cols, dtype=object)
ohlc_df['symbol'] = pd.Series(dtype='object')
ohlc_df['date'] = pd.to_datetime(ohlc_df['date'])
ohlc_df['open'] = pd.Series(dtype='float64')
ohlc_df['high'] = pd.Series(dtype='float64')
ohlc_df['low'] = pd.Series(dtype='float64')
ohlc_df['close'] = pd.Series(dtype='float64')
ohlc_df['volume'] = pd.Series(dtype='int64')
new_df, stock_df_count,stock_df,symbol_len = get_ohlc(symbol_df,ohlc_df)
stocks_dl = (new_df['symbol'].nunique())
# print(new_df)
print(f'Number of Total symbols: {symbol_len}')
print(f'Number of stocks downloaded with data: {stocks_dl}')
print(f'Number of stocks with no data (First round): {stock_df_count}')
print(f'List of stocks with no data: {stock_df}')
# count_datarows(new_df)

# print shape.
# print(f"new_df shape: {new_df.shape}")
# if not new_df.empty:
#     print("new_df head:\n", new_df.head(2)) # Print just a couple of rows for quick check
# else:
#     print("new_df is empty.")
#
try:
    file_names = "ohlc1.xlsx"
    instruments_filepath = os.path.join(downloads_folder, file_names)
    new_df.to_excel(instruments_filepath, index=False, engine='openpyxl')
except Exception as e:
    print(f"\nError saving new_df to file: {e}")

df_amount = find_amount_org(new_df)
# print(df_amount.to_string())
eligible_list1 = filtered_symbols(df_amount)

time_pause2()
start_time = time.time()

ohlc_cols1 = ['symbol','date', 'open', 'high', 'low', 'close', 'volume']
ohlc_df1 = pd.DataFrame(columns=ohlc_cols, dtype=object)
ohlc_df1['symbol'] = pd.Series(dtype='object')
ohlc_df1['date'] = pd.to_datetime(ohlc_df['date'])
ohlc_df1['open'] = pd.Series(dtype='float64')
ohlc_df1['high'] = pd.Series(dtype='float64')
ohlc_df1['low'] = pd.Series(dtype='float64')
ohlc_df1['close'] = pd.Series(dtype='float64')
ohlc_df1['volume'] = pd.Series(dtype='int64')
new_df1,stock_df_count1,stock_df1,symbol_len1 = get_ohlc(eligible_list1,ohlc_df1)
# print(new_df1.to_string())
save_to_excel(new_df1, "ohlc2")
unique_count = new_df1["symbol"].nunique()
mis_data = len(eligible_list1) - unique_count
print(f'Number of Stocks in eligible list first round that don,t have data:{mis_data}')

final_result_df,top_traded_stock = find_amount(new_df1)
stock_to_trade = top_traded_stock
end_time = time.time()
elapsed_time = (end_time - start_time) * 1000
print(f"Script execution time: {elapsed_time:.2f} milli seconds")

