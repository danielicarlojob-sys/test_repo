import pandas as pd


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

    # Step 1: Convert 'reportdatetime' from string to datetime
    df['reportdatetime'] = pd.to_datetime(df['reportdatetime'])

    # Step 2: Sort the DataFrame to ensure correct order of records within
    # each group
    df = df.sort_values(['ESN', 'ACID', 'ENGPOS', 'reportdatetime'])

    # Step 3: Calculate the time difference (in days) between rows in each
    # group
    df['days_since_prev'] = round(
        df.groupby(['ESN', 'ACID', 'ENGPOS'])['reportdatetime']
        .diff()  # difference with the previous timestamp in the group
        .dt.total_seconds() / (60 * 60 * 24),  # convert seconds to days
        1  # round to 1 decimal place
    )

    # Step 4: Optional – replace NaN (first row in each group) with 0
    df['days_since_prev'] = df['days_since_prev'].fillna(0)

    return df
