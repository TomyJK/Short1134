from s_fun import load_excel, find_amount
import os
import gc
import pandas as pd
import time
start_time = time.time()
df = load_excel("stocks1134.xlsx")
df_amount = find_amount(df)
del df
gc.collect()

print(df_amount.to_string())
# print(df_amount)
# downloads_path = r"C:\Users\HP\Desktop\Downloads"
# file_path = os.path.join(downloads_path, "df_amount.xlsx")
# df_amount.to_excel(file_path, index=False)

end_time = time.time()
elapsed_ms = (end_time - start_time) * 1000
print(f"Execution time: {elapsed_ms:.2f} ms")




