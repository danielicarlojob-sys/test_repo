import csv
from datetime import datetime

def extract_latest_timestamp(csv_path, timestamp_column_name, output_txt='timestamp.txt'):
    """
    Extracts the latest timestamp from the specified column in a CSV file and writes it to a text file.
    
    Parameters:
    - csv_path: path to the input CSV file
    - timestamp_column_name: name of the column containing datetime strings
    - output_txt: output text file path (default 'timestamp.txt')
    """
    latest_timestamp = None  # Will hold the most recent datetime

    # Open the CSV file for reading
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)  # Use DictReader for named column access

        for row in reader:
            try:
                # Parse datetime from the column (format: 'YYYY-MM-DD HH:MM:SS')
                current = datetime.strptime(row[timestamp_column_name], '%Y-%m-%d %H:%M:%S')

                # Update latest if this timestamp is more recent
                if latest_timestamp is None or current > latest_timestamp:
                    latest_timestamp = current

            except (ValueError, KeyError) as e:
                # Skip invalid or missing timestamps
                continue

    if latest_timestamp is None:
        raise ValueError("No valid timestamps found in the column.")

    # Write the latest timestamp to timestamp.txt (overwrite if exists)
    with open(output_txt, 'w', encoding='utf-8') as f_out:
        f_out.write(latest_timestamp.strftime('%Y-%m-%d %H:%M:%S'))
