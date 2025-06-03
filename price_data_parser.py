# Combined Script: download_and_convert_data.py

import yfinance as yf
import pandas as pd
import time # Retained from download_data.py, though not directly used in combined main logic
from datetime import datetime, timedelta
import pytz # From converter.py
# import traceback # Only needed inside specific exception blocks

# --- Configuration for Data Download ---
DOWNLOAD_TICKER_SYMBOL = 'NQ=F'      # Yahoo Finance ticker (e.g., E-mini Nasdaq 100 Futures)
# For 15m data, Yahoo typically provides the last 60 days.
DOWNLOAD_END_DATE_DT = datetime.now()
DOWNLOAD_START_DATE_DT = DOWNLOAD_END_DATE_DT - timedelta(days=59) # Data is usually available for the last 60 days
DOWNLOAD_START_DATE = DOWNLOAD_START_DATE_DT.strftime('%Y-%m-%d')
DOWNLOAD_END_DATE = DOWNLOAD_END_DATE_DT.strftime('%Y-%m-%d')
DOWNLOAD_INTERVAL_TO_FETCH = '15m'   # Set interval (e.g., 15 minutes)
# This will be the intermediate CSV, used by the converter part
DOWNLOAD_OUTPUT_CSV_FILENAME = 'nq_15m_intermediate_data.csv'
# Columns to be saved in the downloaded CSV
DOWNLOAD_DESIRED_COLUMNS_OUTPUT_ORDER = ['time', 'open', 'high', 'low', 'close', 'volume']

# --- Configuration for Data Conversion ---
# CONVERTER_CSV_FILENAME will be set to DOWNLOAD_OUTPUT_CSV_FILENAME in the main logic
CONVERTER_UNIX_TIMESTAMP_UNIT = 's' # 's' for seconds, 'ms' for milliseconds
CONVERTER_OUTPUT_SHEET_NAME = "PriceData_NY" # Sheet name in the output Excel file
CONVERTER_GAP_INTERVAL_MINUTES = 15 # Expected interval in minutes for gap filling
# Columns the converter expects/needs from the CSV (subset of downloaded columns is fine)
CONVERTER_INPUT_COLUMNS_FROM_CSV = ['time', 'open', 'high', 'low', 'close']
# Final desired order of columns in the Excel output
CONVERTER_FINAL_EXCEL_ORDERED_COLUMNS = ['Date', 'Time', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
# Output Excel filename will be derived from DOWNLOAD_OUTPUT_CSV_FILENAME


# --- Function from download_data.py (Modified for better integration) ---
def download_yahoo_finance_data(symbol, start, end, interval, filename, desired_columns):
    """
    Downloads historical market data from Yahoo Finance for a specific interval,
    saves it to a CSV file, and returns True if successful and data is not empty,
    False otherwise.
    """
    try:
        print(f"Downloading data for {symbol} from {start} to {end} (interval: {interval})...")
        data_df = yf.download(symbol, start=start, end=end, interval=interval, progress=True)

        if data_df.empty:
            print(f"No data found for {symbol} for the given period and interval.")
            return False

        print("Data downloaded successfully. Processing...")
        data_df.reset_index(inplace=True) # Moves 'Datetime' or 'Date' from index to a column

        datetime_col_name = None
        if 'Datetime' in data_df.columns:
            datetime_col_name = 'Datetime'
        elif 'Date' in data_df.columns: # Less likely for intraday but check
            datetime_col_name = 'Date'
        else:
            print("Error: Could not find the Datetime column in the downloaded data.")
            return False

        # Convert datetime column to Unix timestamp (seconds)
        data_df['time'] = data_df[datetime_col_name].apply(lambda x: int(x.timestamp()))

        data_df.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume'
        }, inplace=True)

        final_columns_for_csv = []
        for col in desired_columns:
            if col in data_df.columns:
                final_columns_for_csv.append(col)
            else:
                print(f"Warning: Column '{col}' not found in downloaded data. It will be skipped for CSV output.")
        
        if not final_columns_for_csv:
            print("Error: No desired columns were found in the downloaded data. Cannot create CSV.")
            return False
        
        output_df = data_df[final_columns_for_csv]
        output_df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        print("\nFirst 5 rows of the downloaded and processed data:")
        print(output_df.head())
        return True

    except Exception as e:
        print(f"An error occurred during data download: {e}")
        return False

# --- Function from converter.py (Modified for better integration) ---
def convert_csv_to_excel_ny_time(csv_filepath, excel_filepath,
                                 unix_timestamp_unit, output_sheet_name,
                                 gap_interval_minutes, csv_input_columns,
                                 final_excel_columns):
    """
    Reads a CSV file with time, open, high, low, close data,
    fills time gaps, converts timestamps to New York time, formats data,
    and saves it to an Excel file. Returns True if successful, False otherwise.
    """
    try:
        print(f"\nStarting conversion process for {csv_filepath}...")
        df = pd.read_csv(csv_filepath)
        print("CSV file read successfully for conversion.")

        missing_cols = [col for col in csv_input_columns if col not in df.columns]
        if missing_cols:
            print(f"Error: Missing expected columns in CSV for conversion: {', '.join(missing_cols)}")
            print(f"Available columns are: {', '.join(df.columns)}")
            return False

        df_processed = df[csv_input_columns].copy()

        print(f"Converting 'time' column (assuming Unix timestamp in '{unix_timestamp_unit}') to datetime (UTC)...")
        df_processed['time_dt_utc'] = pd.to_datetime(df_processed['time'], unit=unix_timestamp_unit, utc=True, errors='coerce')
        df_processed.dropna(subset=['time_dt_utc'], inplace=True)
        if df_processed.empty:
            print("No valid timestamps found after conversion. Exiting conversion.")
            return False
        print("Timestamp conversion to UTC successful.")

        print("Converting datetime to New York timezone...")
        ny_timezone = pytz.timezone('America/New_York')
        df_processed['time_dt_ny'] = df_processed['time_dt_utc'].dt.tz_convert(ny_timezone)
        print("Timezone conversion to New York successful.")

        print("Identifying and filling time gaps...")
        if not df_processed.empty:
            df_processed.sort_values(by='time_dt_ny', inplace=True)
            gap_freq = f'{gap_interval_minutes}T' # Pandas frequency string
            df_processed.set_index('time_dt_ny', inplace=True)

            if not df_processed.empty:
                min_time = df_processed.index.min()
                max_time = df_processed.index.max()
                full_range = pd.date_range(start=min_time, end=max_time, freq=gap_freq)
                df_reindexed = df_processed.reindex(full_range)

                ohlc_cols_original_case = [col for col in ['open', 'high', 'low', 'close'] if col in csv_input_columns]
                for col_name in ohlc_cols_original_case:
                    if col_name in df_reindexed.columns:
                        df_reindexed[col_name].fillna("GAP", inplace=True)
                    else: # Should not happen if csv_input_columns are correctly set and present
                        df_reindexed[col_name] = "GAP" 
                
                df_reindexed.reset_index(inplace=True)
                df_processed = df_reindexed.rename(columns={'index': 'time_dt_ny'})
                print("Time gaps filled.")
            else:
                print("DataFrame became empty before gap filling (e.g. after sorting). Skipping gap filling.")
        else:
            print("DataFrame is empty, skipping gap filling.")

        print("Formatting Date and Time columns...")
        df_processed['Date'] = df_processed['time_dt_ny'].dt.strftime('%d/%m/%Y')
        df_processed['Time'] = df_processed['time_dt_ny'].dt.strftime('%H:%M:%S')
        print("Date and Time columns formatted.")

        df_processed.rename(columns={
            'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE'
        }, inplace=True)
        
        # Ensure all columns in final_excel_columns exist, create if not (though they should from prior steps)
        for col in final_excel_columns:
            if col not in df_processed.columns:
                 df_processed[col] = None # Or some default, though 'Date', 'Time', 'OPEN', etc. should exist

        output_df = df_processed[final_excel_columns].copy()
        print(f"Prepared data for Excel with columns: {', '.join(final_excel_columns)}")

        price_cols_to_format = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
        print("Formatting price columns (to 3 decimal places, comma as separator)...")
        for col in price_cols_to_format:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(object) # Ensure column can hold mixed types (numbers and "GAP")
                def format_price_or_gap(value):
                    if isinstance(value, str) and value == "GAP":
                        return "GAP"
                    if isinstance(value, (int, float)):
                        return f"{value:.3f}".replace('.', ',')
                    if pd.notnull(value):
                        try:
                            num_val = float(str(value))
                            return f"{num_val:.3f}".replace('.', ',')
                        except (ValueError, TypeError):
                            return str(value) 
                    return '' # For NaN/None values that are not "GAP"
                output_df[col] = output_df[col].apply(format_price_or_gap)
        print("Price columns formatted.")

        print(f"Writing data to Excel file: {excel_filepath} (Sheet: {output_sheet_name})...")
        output_df.to_excel(excel_filepath, sheet_name=output_sheet_name, index=False)
        print(f"Successfully created Excel file: {excel_filepath}")
        return True

    except FileNotFoundError:
        print(f"Error: The CSV file {csv_filepath} was not found for conversion.")
        return False
    except KeyError as e:
        print(f"Error during conversion: A required column was not found. Details: {e}")
        print(f"Please ensure your CSV has the columns: {', '.join(csv_input_columns)}")
        return False
    except pytz.exceptions.UnknownTimeZoneError as e:
        print(f"Error during conversion: Timezone 'America/New_York' is unknown. Details: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during conversion: {e.__class__.__name__} - {e}")
        import traceback
        traceback.print_exc()
        return False

# --- Main Execution Logic ---
if __name__ == "__main__":
    print("--- Starting Data Download and Conversion Process ---")

    # Step 1: Download data
    download_successful = download_yahoo_finance_data(
        symbol=DOWNLOAD_TICKER_SYMBOL,
        start=DOWNLOAD_START_DATE,
        end=DOWNLOAD_END_DATE,
        interval=DOWNLOAD_INTERVAL_TO_FETCH,
        filename=DOWNLOAD_OUTPUT_CSV_FILENAME,
        desired_columns=DOWNLOAD_DESIRED_COLUMNS_OUTPUT_ORDER
    )

    if download_successful:
        print(f"\n--- Data download successful. Proceeding to conversion. ---")
        
        # Step 2: Convert the downloaded CSV to Excel
        input_csv_for_conversion = DOWNLOAD_OUTPUT_CSV_FILENAME
        # Derive output Excel filename from the CSV filename
        output_excel_filename = input_csv_for_conversion.rsplit('.', 1)[0] + '.xlsx'

        conversion_successful = convert_csv_to_excel_ny_time(
            csv_filepath=input_csv_for_conversion,
            excel_filepath=output_excel_filename,
            unix_timestamp_unit=CONVERTER_UNIX_TIMESTAMP_UNIT,
            output_sheet_name=CONVERTER_OUTPUT_SHEET_NAME,
            gap_interval_minutes=CONVERTER_GAP_INTERVAL_MINUTES,
            csv_input_columns=CONVERTER_INPUT_COLUMNS_FROM_CSV,
            final_excel_columns=CONVERTER_FINAL_EXCEL_ORDERED_COLUMNS
        )

        if conversion_successful:
            print(f"\n--- Data conversion successful. Excel file created: {output_excel_filename} ---")
        else:
            print("\n--- Data conversion failed. ---")
    else:
        print("\n--- Data download failed. Skipping conversion. ---")

    print("\n--- Process Finished ---")