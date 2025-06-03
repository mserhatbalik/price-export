import pandas as pd
import pytz
from datetime import datetime

# --- Configuration ---
CSV_FILENAME = "price_data.csv"
# Assumes the Unix timestamps in the CSV are in seconds.
# If they are in milliseconds, change 's' to 'ms' in pd.to_datetime unit parameter below.
UNIX_TIMESTAMP_UNIT = 's'
OUTPUT_SHEET_NAME = "PriceData_NY" # You can change the sheet name if you like
GAP_INTERVAL_MINUTES = 15 # <<< NEW: Define the expected interval in minutes for gap filling

# Define the input columns from CSV
CSV_COLUMNS = ['time', 'open', 'high', 'low', 'close']
FINAL_EXCEL_ORDERED_COLUMNS = ['Date', 'Time', 'OPEN', 'HIGH', 'LOW', 'CLOSE']

def convert_csv_to_excel_ny_time(csv_filepath, excel_filepath):
    """
    Reads a CSV file with time, open, high, low, close data,
    fills time gaps, converts timestamps to New York time, formats data,
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
            print(f"Available columns are: {', '.join(df.columns)}")
            return

        # Keep only the necessary columns initially
        df_processed = df[CSV_COLUMNS].copy()

        # 3. Convert Unix timestamp to datetime objects (assuming UTC)
        print(f"Converting 'time' column (assuming Unix timestamp in '{UNIX_TIMESTAMP_UNIT}') to datetime (UTC)...")
        df_processed['time_dt_utc'] = pd.to_datetime(df_processed['time'], unit=UNIX_TIMESTAMP_UNIT, utc=True, errors='coerce')
        # Drop rows where original timestamp could not be converted
        df_processed.dropna(subset=['time_dt_utc'], inplace=True)
        if df_processed.empty:
            print("No valid timestamps found after conversion. Exiting.")
            return
        print("Timestamp conversion to UTC successful.")

        # 4. Convert UTC datetime to New York time
        print("Converting datetime to New York timezone...")
        ny_timezone = pytz.timezone('America/New_York')
        df_processed['time_dt_ny'] = df_processed['time_dt_utc'].dt.tz_convert(ny_timezone)
        print("Timezone conversion to New York successful.")

        # 5. --- NEW: Identify and fill time gaps ---
        print("Identifying and filling time gaps...")
        if not df_processed.empty:
            df_processed.sort_values(by='time_dt_ny', inplace=True)
            
            gap_freq = f'{GAP_INTERVAL_MINUTES}T' # Pandas frequency string, e.g., '15T' for 15 minutes

            # Set time_dt_ny as index to reindex
            df_processed.set_index('time_dt_ny', inplace=True)

            if not df_processed.empty: # Check again after potential drops/sorts
                min_time = df_processed.index.min()
                max_time = df_processed.index.max()
                
                # Create the full time range based on min/max times in data
                full_range = pd.date_range(start=min_time, end=max_time, freq=gap_freq)
                
                # Reindex. Original data is kept. Missing time slots get NaN for all columns.
                df_reindexed = df_processed.reindex(full_range)

                # For columns 'open', 'high', 'low', 'close', fill NaN with "GAP"
                # These are the original column names before renaming to uppercase.
                ohlc_cols_original_case = ['open', 'high', 'low', 'close']
                for col_name in ohlc_cols_original_case:
                    if col_name in df_reindexed.columns:
                        df_reindexed[col_name].fillna("GAP", inplace=True)
                    else:
                        # If an OHLC column somehow wasn't there, create it and fill with GAP for safety
                        df_reindexed[col_name] = "GAP" 
                
                # Restore 'time_dt_ny' from index to a column
                df_reindexed.reset_index(inplace=True)
                df_processed = df_reindexed.rename(columns={'index': 'time_dt_ny'})
                
                # Note: 'time' and 'time_dt_utc' columns will be NaN for the GAP rows.
                # This is acceptable as they are not part of FINAL_EXCEL_ORDERED_COLUMNS.
                print("Time gaps filled.")
            else:
                print("DataFrame became empty before gap filling (e.g. after sorting). Skipping.")
        else:
            print("DataFrame is empty, skipping gap filling.")
        # --- End of Gap Filling ---

        # 6. Create 'Date' (DD/MM/YYYY) and 'Time' (hh:mm:ss) columns
        print("Formatting Date and Time columns...")
        df_processed['Date'] = df_processed['time_dt_ny'].dt.strftime('%d/%m/%Y')
        df_processed['Time'] = df_processed['time_dt_ny'].dt.strftime('%H:%M:%S')
        print("Date and Time columns formatted.")

        # 7. Prepare the DataFrame for Excel output
        # Rename OHLC columns to uppercase
        df_processed.rename(columns={
            'open': 'OPEN',
            'high': 'HIGH',
            'low': 'LOW',
            'close': 'CLOSE'
        }, inplace=True)
        
        # Select and order columns for the final Excel sheet
        output_df = df_processed[FINAL_EXCEL_ORDERED_COLUMNS].copy()
        print(f"Prepared data for Excel with columns: {', '.join(FINAL_EXCEL_ORDERED_COLUMNS)}")

        # 8. --- MODIFIED: Format price columns (OPEN, HIGH, LOW, CLOSE) ---
        price_cols_in_df = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
        print("Formatting price columns (to 3 decimal places, comma as separator)...")
        for col in price_cols_in_df:
            if col in output_df.columns:
                
                output_df[col] = output_df[col].astype(object) # Ensure column can hold mixed types

                def format_price_or_gap(value):
                    if isinstance(value, str) and value == "GAP":
                        return "GAP"
                    if isinstance(value, (int, float)): # If it's already a number
                        return f"{value:.3f}".replace('.', ',')
                    if pd.notnull(value): # If it's a string that might be a number or other type
                        try:
                            # Try to convert to float; str(value) handles non-string inputs
                            num_val = float(str(value)) 
                            return f"{num_val:.3f}".replace('.', ',')
                        except (ValueError, TypeError):
                            # If not "GAP" and not convertible to float, return as string
                            return str(value) 
                    return '' # For NaN/None values that are not "GAP"

                output_df[col] = output_df[col].apply(format_price_or_gap)
        print("Price columns formatted.")
        # --- End of Modified Price Formatting ---

        # 9. Write to Excel
        print(f"Writing data to Excel file: {excel_filepath} (Sheet: {OUTPUT_SHEET_NAME})...")
        output_df.to_excel(excel_filepath, sheet_name=OUTPUT_SHEET_NAME, index=False)
        print(f"Successfully created Excel file: {excel_filepath}")

    except FileNotFoundError:
        print(f"Error: The file {csv_filepath} was not found.")
        print("Please ensure 'price_data.csv' is in the same directory as the script, or provide the full path.")
    except KeyError as e:
        print(f"Error: A required column was not found in the CSV. Details: {e}")
        print(f"Please ensure your CSV has the columns: {', '.join(CSV_COLUMNS)}")
    except pytz.exceptions.UnknownTimeZoneError as e:
        print(f"Error: Timezone 'America/New_York' is unknown. Details: {e}")
        print("This might indicate an issue with the pytz library installation or an incorrect timezone name.")
    except Exception as e:
        print(f"An unexpected error occurred: {e.__class__.__name__} - {e}")
        import traceback
        print("Traceback:")
        traceback.print_exc()
        print("If the error is unclear, please provide the full error message for further assistance.")

if __name__ == "__main__":
    input_csv_file = CSV_FILENAME
    output_excel_file = CSV_FILENAME.rsplit('.', 1)[0] + '.xlsx' 
    
    convert_csv_to_excel_ny_time(input_csv_file, output_excel_file)