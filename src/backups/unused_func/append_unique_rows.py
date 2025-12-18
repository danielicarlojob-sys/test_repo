import pandas as pd
import os
from src.utils.log_file import LOG_FILE, log_message


def append_unique_rows(csv_path: str, new_data: pd.DataFrame):
    """
    Appends only unique rows from `new_data` to a CSV file at `csv_path`.
    - If the file does not exist, it creates it with the new data.
    - If it does exist, it appends only truly new rows (ignoring the index column for comparison).
    - Ensures column order matches the existing file.

    Parameters
    ----------
    - csv_path (str): Path to the CSV file.
    - new_data (pd.DataFrame): The new data to append.
    """

    # Check if the target CSV file already exists
    if os.path.exists(csv_path):
        # Load existing data from file, treating first column as index
        existing_data = pd.read_csv(csv_path, index_col=0)

        # Align column order of new_data to match existing_data
        new_data = new_data[existing_data.columns]

        # Combine existing and new data, ignore index for comparison
        combined = pd.concat([existing_data, new_data], ignore_index=True)
        # Convert 'reportdatetime' column to datetime format
        combined['reportdatetime'] = pd.to_datetime(combined['reportdatetime'], errors='coerce')
        # Drops rows where the datetime conversion failed (NaT = Not a Time)
        combined = combined[combined['reportdatetime'].notna()]
        # Sort by datetime
        combined.sort_values(by='reportdatetime',ascending=True,inplace=True)
        # Drop duplicates based on content (ignoring index column)
        deduplicated = combined.drop_duplicates()

        # Identify only the rows that are new
        new_unique = deduplicated.iloc[len(existing_data):]

        # Append new rows to file if any
        if not new_unique.empty:
            new_unique.to_csv(csv_path, mode='a', header=False, index=True)
        else:
            log_message("No new unique rows to append.")
    
    else:
        # File doesn't exist — create it with full new_data (with header)
        log_message(f"File '{csv_path}' does not exist. Creating new file.")
        new_data.to_csv(csv_path, index=True)
