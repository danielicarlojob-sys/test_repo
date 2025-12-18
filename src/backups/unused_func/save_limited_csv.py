import pandas as pd
import os
import tempfile

def append_unique_rows(csv_path, df, size_limit_bytes=100 * 1024 * 1024): # 100 MB default
    """
    Appends only unique rows from `df` to a CSV at `csv_path`.
    Ensures file stays under `size_limit_bytes` by removing oldest rows (by 'reportdatetime').
    """

    # If the file exists, merge with existing data
    if os.path.exists(csv_path):
        existing_data = pd.read_csv(csv_path, index_col=0)

        # Align columns
        df = df[existing_data.columns]

        # Combine old and new data
        combined = pd.concat([existing_data, df], ignore_index=True)

        # Ensure proper datetime parsing
        combined['reportdatetime'] = pd.to_datetime(combined['reportdatetime'], errors='coerce')
        combined = combined[combined['reportdatetime'].notna()]

        # Sort by reportdatetime ascending (oldest first)
        combined.sort_values(by='reportdatetime', ascending=True, inplace=True)

        # Remove exact duplicates
        combined = combined.drop_duplicates()

        # Write to a temporary file using mkstemp (Windows safe)
        fd, tmp_path = tempfile.mkstemp(suffix='.csv')
        os.close(fd) # Close file descriptor immediately
        try:
            combined.to_csv(tmp_path, index=True)
            if os.path.getsize(tmp_path) <= size_limit_bytes:
                combined.to_csv(csv_path, index=True)
                print(f"Saved {len(combined) - len(existing_data)} new unique rows to '{csv_path}'.")
            else:
                # Trim oldest rows to reduce size
                for i in range(len(combined)):
                    trimmed = combined.iloc[i:] # remove i oldest rows
                    fd_trim, test_path = tempfile.mkstemp(suffix='.csv')
                    os.close(fd_trim)
                    try:
                        trimmed.to_csv(test_path, index=True)
                        if os.path.getsize(test_path) <= size_limit_bytes:
                            trimmed.to_csv(csv_path, index=True)
                            print(f"Trimmed {i} oldest rows to fit size limit. Saved {len(trimmed)} rows to '{csv_path}'.")
                            return
                    finally:
                        os.remove(test_path)
                print("Unable to trim enough data to meet size limit. No changes saved.")
        finally:
            os.remove(tmp_path)

    else:
        # File does not exist yet — save full df
        print(f"File '{csv_path}' does not exist. Creating new file.")
        df.to_csv(csv_path, index=True)
