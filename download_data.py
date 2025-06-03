import yfinance as yf
import pandas as pd
import time
from datetime import datetime, timedelta # Added for dynamic date range

# --- Configuration ---
ticker_symbol = 'NQ=F'      # Yahoo Finance ticker for E-mini Nasdaq 100 Futures

# For 15m data, Yahoo typically provides the last 60 days.
# Set start_date and end_date accordingly.
# Example: Fetch data for the last 59 days up to today.
end_date_dt = datetime.now()
start_date_dt = end_date_dt - timedelta(days=59) # Data is usually available for the last 60 days

start_date = start_date_dt.strftime('%Y-%m-%d')
end_date = end_date_dt.strftime('%Y-%m-%d')

interval_to_fetch = '15m'   # Set interval to 15 minutes
output_csv_filename = 'nq_15m_data.csv'

desired_columns_output_order = ['time', 'open', 'high', 'low', 'close', 'volume']


def download_yahoo_finance_data(symbol, start, end, interval, filename):
    """
    Downloads historical market data from Yahoo Finance for a specific interval and saves it to a CSV file.
    The output CSV will have 'time' (Unix timestamp in seconds), 'open', 'high', 'low', 'close', 'volume'.
    """
    try:
        print(f"Downloading data for {symbol} from {start} to {end} (interval: {interval})...")
        data_df = yf.download(symbol, start=start, end=end, interval=interval, progress=True)

        if data_df.empty:
            print(f"No data found for {symbol} for the given period and interval.")
            return

        print("Data downloaded successfully. Processing...")

        # For intraday data, the index is already a DatetimeIndex with time information.
        # It's usually timezone-aware (e.g., America/New_York for NQ=F).
        # We need to convert it to Unix timestamp.
        data_df.reset_index(inplace=True) # Moves 'Datetime' or 'Date' from index to a column

        # Figure out the name of the datetime column (yf might name it 'Datetime' or 'Date')
        datetime_col_name = None
        if 'Datetime' in data_df.columns:
            datetime_col_name = 'Datetime'
        elif 'Date' in data_df.columns: # Less likely for intraday but check
            datetime_col_name = 'Date'
        else:
            print("Error: Could not find the Datetime column in the downloaded data.")
            return

        # Convert datetime column to Unix timestamp (seconds)
        # If the datetime is timezone-aware, timestamp() gives UTC Unix time.
        # If it's naive, it assumes local time. yfinance usually provides tz-aware for intraday.
        data_df['time'] = data_df[datetime_col_name].apply(lambda x: int(x.timestamp()))

        data_df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)

        final_columns = []
        for col in desired_columns_output_order:
            if col in data_df.columns:
                final_columns.append(col)
            else:
                print(f"Warning: Column '{col}' not found in downloaded data. It will be skipped.")
        
        output_df = data_df[final_columns]

        output_df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        print("\nFirst 5 rows of the downloaded and processed data:")
        print(output_df.head())

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Ensure you set the start_date and end_date to be within the last 60 days for 15m interval data
    # For example:
    # end_date_dt = datetime.now()
    # start_date_dt = end_date_dt - timedelta(days=59)
    # current_start_date = start_date_dt.strftime('%Y-%m-%d')
    # current_end_date = end_date_dt.strftime('%Y-%m-%d')
    # download_yahoo_finance_data(ticker_symbol, current_start_date, current_end_date, interval_to_fetch, output_csv_filename)
    
    # Using the globally defined start_date, end_date from configuration
    download_yahoo_finance_data(ticker_symbol, start_date, end_date, interval_to_fetch, output_csv_filename)