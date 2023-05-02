import pandas as pd

df = pd.read_csv("result.csv")

# convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'])

# sort the DataFrame by 'Symbol' and 'Date'
df = df.sort_values(by=['Symbol', 'Date'])

# calculate the moving average and moving median
df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).\
    mean().reset_index(level=0, drop=True)
df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).\
    median().reset_index(level=0, drop=True)

# save data to csv file 
df.to_csv("question2.csv")






