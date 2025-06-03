import pandas as pd
import pytz
from datetime import datetime

# --- Configuration ---
CSV_FILENAME = "price_data.csv"
# Assumes the Unix timestamps in the CSV are in seconds.
# If they are in milliseconds, change 's' to 'ms' in pd.to_datetime unit parameter below.
UNIX_TIMESTAMP_UNIT = 's'
OUTPUT_SHEET_NAME = "PriceData_NY" # You can change the sheet name if you like

# Define the input columns from CSV and output column names for Excel
CSV_COLUMNS = ['time', 'open', 'high', 'low', 'close']
EXCEL_COLUMNS = {
    'time_dt_ny': 'Timestamp_NY', # Temporary column, will be split
    'open': 'OPEN',
    'high': 'HIGH',
    'low': 'LOW',
    'close': 'CLOSE'
}
FINAL_EXCEL_ORDERED_COLUMNS = ['Date', 'Time', 'OPEN', 'HIGH', 'LOW', 'CLOSE']

def convert_csv_to_excel_ny_time(csv_filepath, excel_filepath):
    """
    Reads a CSV file with time, open, high, low, close data,
    converts timestamps to New York time, formats date and time,
    and saves it to an Excel file.
    """
    try:
        # 1. Read the CSV file
        print(f"Reading CSV file: {csv_filepath}...")
        df = pd.read_csv(csv_filepath)
        print("CSV file read successfully.")

        # 2. Verify necessary columns exist
        missing_cols = [col for col in CSV_COLUMNS if col not in df.columns]
        if missing_cols:
            print(f"Error: Missing expected columns in CSV: {', '.join(missing_cols)}")
            return

        # Keep only the necessary columns initially to avoid processing extra data
        df = df[CSV_COLUMNS].copy() # Use .copy() to avoid SettingWithCopyWarning

        # 3. Convert Unix timestamp to datetime objects (assuming UTC)
        # The 'time' column from CSV is used here.
        print(f"Converting 'time' column (assuming Unix timestamp in '{UNIX_TIMESTAMP_UNIT}') to datetime (UTC)...")
        df['time_dt_utc'] = pd.to_datetime(df['time'], unit=UNIX_TIMESTAMP_UNIT, utc=True)
        print("Timestamp conversion to UTC successful.")

        # 4. Convert UTC datetime to New York time
        print("Converting datetime to New York timezone...")
        ny_timezone = pytz.timezone('America/New_York')
        df['time_dt_ny'] = df['time_dt_utc'].dt.tz_convert(ny_timezone)
        print("Timezone conversion to New York successful.")

        # 5. Create 'Date' (DD/MM/YYYY) and 'Time' (hh:mm:ss) columns
        print("Formatting Date and Time columns...")
        df['Date'] = df['time_dt_ny'].dt.strftime('%d/%m/%Y')
        df['Time'] = df['time_dt_ny'].dt.strftime('%H:%M:%S')
        print("Date and Time columns formatted.")

        # 6. Prepare the DataFrame for Excel output
        # Rename OHLC columns to uppercase as requested
        df.rename(columns={
            'open': 'OPEN',
            'high': 'HIGH',
            'low': 'LOW',
            'close': 'CLOSE'
        }, inplace=True)
        
        # Select and order columns for the final Excel sheet
        output_df = df[FINAL_EXCEL_ORDERED_COLUMNS]
        print(f"Prepared data for Excel with columns: {', '.join(FINAL_EXCEL_ORDERED_COLUMNS)}")

        # In the section where you prepare output_df, before writing to excel:
        price_cols_in_df = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
        for col in price_cols_in_df:
            if col in output_df.columns:
                output_df[col] = output_df[col].apply(lambda x: f"{x:.3f}".replace('.', ',') if pd.notnull(x) else '')

        # 7. Write to Excel
        print(f"Writing data to Excel file: {excel_filepath} (Sheet: {OUTPUT_SHEET_NAME})...")
        output_df.to_excel(excel_filepath, sheet_name=OUTPUT_SHEET_NAME, index=False)
        print(f"Successfully created Excel file: {excel_filepath}")

    except FileNotFoundError:
        print(f"Error: The file {csv_filepath} was not found.")
    except KeyError as e:
        print(f"Error: A required column was not found in the CSV. Details: {e}")
        print(f"Please ensure your CSV has the columns: {', '.join(CSV_COLUMNS)}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    input_csv_file = CSV_FILENAME
    output_excel_file = CSV_FILENAME.replace('.csv', '.xlsx')
    
    convert_csv_to_excel_ny_time(input_csv_file, output_excel_file)