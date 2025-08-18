import pandas as pd
import os

def save_to_excel(df, filename):
    downloads_path  = r"C:\Users\HP\Desktop\Downloads"
    file_path = os.path.join(downloads_path, f"{filename}.xlsx")
    df.to_excel(file_path, index=False)
    # print(f"File saved to: {file_path}")


downloads_path = r"C:\Users\HP\Desktop\Downloads"





