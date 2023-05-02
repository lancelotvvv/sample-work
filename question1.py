import os
import pandas as pd
import pyarrow.parquet as pq

# Set the folder paths and file names
symbols_file = 'symbols_valid_meta.csv'
etfs_folder = 'etfs'
stocks_folder = 'stocks'
output_file = 'processed_data.parquet'

# Read the symbols file into a DataFrame
symbols_df = pd.read_csv(symbols_file)
symbols_df = symbols_df[['Symbol', 'Security Name']]


data = []
for folder in [etfs_folder, stocks_folder]:
    for filename in os.listdir(folder):
        if filename.endswith('.csv'):
            symbol = filename[:-4]
            file_path = os.path.join(folder, filename)
            df = pd.read_csv(file_path)
            df['Symbol'] = symbol
            data.append(df)

data = pd.concat(data, axis=0, ignore_index=True)
data = pd.merge(data, symbols_df, on='Symbol')

# Convert the Date Format
data['Date'] = data['Date'].astype(str)
data['Volume'] = data['Volume'].fillna(0).astype(int)

# Rearrange and Save the merged data to a Parquet file
data = data[['Symbol', 'Security Name','Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
# data.to_parquet(output_file, engine='pyarrow')

data.to_csv("result.csv",index=False)