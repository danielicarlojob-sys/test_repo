import csv
import os
import tempfile

def normalize_row(row):
    """
    Normalize a row to detect duplicates:
    - Strip whitespace from each cell
    - Convert to lowercase
    - Join the cells with commas
    """
    return ','.join(cell.strip().lower() for cell in row)

def append_and_deduplicate_csv(csv_path, new_rows):
    """
    Appends new_rows to a CSV file with a header, and removes any duplicated data rows (excluding the header).
    
    Parameters:
    - csv_path: Path to the CSV file (string)
    - new_rows: List of lists, where each list is a row to append
    """
    
    # STEP 1: Append new rows to the existing CSV file
    with open(csv_path, 'a', newline='', encoding='utf-8') as f_append:
        writer = csv.writer(f_append)
        for row in new_rows:
            writer.writerow(row)  # Just append, no checks yet

    # STEP 2: Deduplicate entire file (excluding header)
    seen = set()  # To store normalized row strings

    # Create a temporary file to store deduplicated content
    tmp_fd, tmp_path = tempfile.mkstemp(text=True)
    os.close(tmp_fd)  # We’ll reopen it with csv.writer

    with open(csv_path, 'r', newline='', encoding='utf-8') as f_in, \
         open(tmp_path, 'w', newline='', encoding='utf-8') as f_out:

        reader = csv.reader(f_in)
        writer = csv.writer(f_out)

        try:
            header = next(reader)  # Read the first row as header
            writer.writerow(header)  # Always write header to new file
        except StopIteration:
            # File is empty — no header to copy
            return

        for row in reader:
            norm = normalize_row(row)
            if norm not in seen:
                writer.writerow(row)  # Write original row (not normalized)
                seen.add(norm)

    # STEP 3: Replace the original file with the deduplicated one
    os.replace(tmp_path, csv_path)
