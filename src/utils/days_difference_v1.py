import pandas as pd
from src.utils.log_file import LOG_FILE, log_message, debug_info, f_lineno as line


def days_difference(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column 'days_since_prev' to the input DataFrame, representing the number
    of days between consecutive 'reportdatetime' entries within each group of
    (ESN, ACID, ENGPOS). The input 'reportdatetime' column must be in string format
    (yyyy-mm-dd HH:MM:SS), and will be converted to datetime.

    Parameters:
    -----------
    df : pd.DataFrame
        Input DataFrame containing the columns:
        - 'ESN': engine serial number (or equivalent identifier)
        - 'ACID': aircraft identifier
        - 'ENGPOS': engine position
        - 'reportdatetime': string timestamp in format yyyy-mm-dd HH:MM:SS

    Returns:
    --------
    pd.DataFrame
        The same DataFrame with an additional column:
        - 'days_since_prev': float, number of days since the previous reportdatetime
          within the same (ESN, ACID, ENGPOS) group. The first row in each group will be 0.
    """
    
    # Step 0: check if df is empty
    if df.empty:
        cols = list(df.columns)
        df = pd.DataFrame(columns=cols + ["days_since_prev"])

        return df
    # Step 1: split df between old and new data
    df_new = df[df['NEW_FLAG']==1].copy()
    df_old = df[df['NEW_FLAG']!=1].copy()
    
    # Step 2: Sort the DataFrame to ensure correct order of records within
    # each group
    df_new = df_new.sort_values(['ESN', 'ACID', 'ENGPOS', 'reportdatetime'])
    
    # Step 3: Calculate the time difference (in days) between rows in each
    # group
    df_new['days_since_prev'] = round(
        df_new.groupby(['ESN', 'ACID', 'ENGPOS'])['reportdatetime']
        .diff()  # difference with the previous timestamp in the group
        .dt.total_seconds() / (60 * 60 * 24),  # convert seconds to days
        1  # round to 1 decimal place
    )

    # Step 4: replace NaN (first row in each group) with 0
    df_new['days_since_prev'] = df_new['days_since_prev'].fillna(0)
    
    # Step 5: concatenate back old data to new
    df_out = pd.concat([df_old, df_new], ignore_index=True)
    

    return df_out
