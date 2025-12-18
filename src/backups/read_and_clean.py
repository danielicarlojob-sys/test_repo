
import pandas as pd
from src.utils.enforce_string_dtype import enforce_string_dtype
from src.utils.log_file import LOG_FILE, log_message, debug_info


def read_and_clean_csv(csv_path: str, columns_type_check: bool = False) -> pd.DataFrame:
    """
    Reads a CSV file with specified data types, cleans invalid rows, and rounds float columns.

    - Parses 'reportdatetime' and 'datestored' as datetime
    - Drops rows where either datetime is invalid
    - Rounds all float columns except 'ESN', 'ACID', 'reportdatetime', and 'datestored' to 5 decimals

    Parameters
    ----------
    Args:
         - csv_path (str): path to CSV file
         - columns_type_check (bool) = False: option to check an log dtype of each column in the Dataframe

    Returns
    -------
         - df (pd.DataFrame): cleaned DataFrame
    """
    # Read CSV columns

    cols = pd.read_csv(csv_path,nrows=0).columns

    # Read CSV

    # Define known dtypes
    dtype_spec = {
        'ESN': 'int64',
        'operator': 'string',
        'equipmentid': 'Int64',
        'ACID': 'string',
        'ENGPOS': 'Int64',
        'DSCID': 'Int64',
        'days_since_prev': 'float',
        'NEW_FLAG': 'Int64',
        'SISTER_ESN': 'Int64',
        'FlagSV': 'Int64',
        'FlagSisChg': 'Int64'
    }
    #    df = pd.read_csv(csv_path, dtype=dtype_spec)
    df = pd.read_csv(csv_path, low_memory=False)

    # log_message(f"read_and_clean_csv - line 40 - (MAX, MIN) df['reportdatetime']:\n {(df['reportdatetime'].max(), df['reportdatetime'].min())}")
    # Set dtypes
    safe_map = {
        col: typ for col, typ in dtype_spec.items() if col in df.columns}
    df = df.astype(safe_map)
    # log_message(f"read_and_clean_csv - line 44 - (MAX, MIN) df['reportdatetime']:\n {(df['reportdatetime'].max(), df['reportdatetime'].min())}")
    # List of datetime columns to parse
    datetime_cols = ['reportdatetime', 'datestored']

    # Coerce bad datetimes to NaT (Not a Time)
    for col in datetime_cols:
        df[col] = pd.to_datetime(
            df[col],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce')
        "df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')"

        # df[col] = pd.to_datetime(df[col], errors='coerce')
    # log_message(f"read_and_clean_csv - line 52 - (MAX, MIN) df['reportdatetime']:\n {(df['reportdatetime'].max(), df['reportdatetime'].min())}")
    # Drop rows where any datetime column is invalid

    df = df.dropna(subset=datetime_cols)
    # log_message(f"read_and_clean_csv - line 56 - (MAX, MIN) df['reportdatetime']:\n {(df['reportdatetime'].max(), df['reportdatetime'].min())}")
    # log_message(f"pandas df read from: {csv_path}")
    if 'Unnamed: 0' in list(df.columns):
        df = df.drop(columns=['Unnamed: 0'])
    if 'Column1' in list(df.columns):
        df = df.drop(columns=['Column1'])

    # Exclude certain columns from rounding
    exclude_cols = {
        'ESN',
        'operator',
        'equipmentid',
        'ACID',
        'ENGPOS',
        'DSCID',
        'days_since_prev',
        'NEW_FLAG',
        'SISTER_ESN',
        'FlagSV',
        'FlagSisChg',
        *datetime_cols}

    # Identify float columns to round
    float_cols_to_round = [
        col for col in df.select_dtypes(include='float').columns
        if col not in exclude_cols
    ]

    # Round the appropriate float columns to 5 decimal places
    df.loc[:, float_cols_to_round] = df[float_cols_to_round].round(5)
    # log_message(f"        {debug_info()}  Number ESN from previous run\ndf['ESN'].unique():\n {len(list(df['ESN'].unique()))}")
    # Force dtype = string for VAR{i}_IDENTIFIER{lag}
    df = enforce_string_dtype(df)
    if columns_type_check == True:
        for i, col_name in enumerate(df.columns):
            col_name = df.columns[i]
            log_message(f"Column {i}: {col_name}")
            log_message(f"pandas dtype: {df[col_name].dtype}")
            log_message(f"Python types present:\n{df[col_name].apply(type).value_counts()}")
            log_message(f"##############")



    return df


def read_and_clean_csv2(csv_path: str) -> pd.DataFrame:
    """
    Reads a CSV file with specified data types, cleans invalid rows, and rounds float columns.

    - Parses 'reportdatetime' and 'datestored' as datetime
    - Drops rows where either datetime is invalid
    - Rounds all float columns except 'ESN', 'ACID', 'reportdatetime', and 'datestored' to 5 decimals

    Parameters
    ----------
    Args:
         - csv_path (str): path to CSV file

    Returns
    -------
         - df (pd.DataFrame): cleaned DataFrame
    """

    # Read CSV without automatic dtype enforcement
    df = pd.read_csv(csv_path, dtype=str)  # Read all columns as strings first

    # Strip spaces from all column names
    df.columns = df.columns.str.strip()

    # Strip spaces from all string columns (apply strip only to object dtype
    # columns)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Define known dtypes
    dtype_spec = {
        'ESN': 'Int64',
        'equipmentid': 'Int64',
        'ENGPOS': 'Int64',
        'DSCID': 'Int64',
        'days_since_prev': 'float',
        # 'NEW_FLAG': 'Int64',
        'SISTER_ESN': 'Int64',
        'FlagSV': 'Int64',
        'FlagSisChg': 'Int64'
    }
    # Set dtypes
    safe_map = {
        col: typ for col,
        typ in dtype_spec.items() if col in df.columns}
    log_message(f"        {debug_info()}  safe_map:\n {safe_map}")
    df = df.astype(safe_map)
    # List of datetime columns to parse
    datetime_cols = ['reportdatetime', 'datestored']

    # Coerce bad datetimes to NaT (Not a Time)
    for col in datetime_cols:
        df[col] = pd.to_datetime(
            df[col],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce')
        "df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')"

        # Drop rows where any datetime column is invalid

    df = df.dropna(subset=datetime_cols)
    # log_message(f"read_and_clean_csv - line 56 - (MAX, MIN) df['reportdatetime']:\n {(df['reportdatetime'].max(), df['reportdatetime'].min())}")
    # log_message(f"pandas df read from: {csv_path}")
    if 'Unnamed: 0' in list(df.columns):
        df = df.drop(columns=['Unnamed: 0'])
    if 'Column1' in list(df.columns):
        df = df.drop(columns=['Column1'])

    # Exclude certain columns from rounding
    exclude_cols = {
        'ESN',
        'operator',
        'equipmentid',
        'ACID',
        'ENGPOS',
        'DSCID',
        'days_since_prev',
        'NEW_FLAG',
        'SISTER_ESN',
        'FlagSV',
        'FlagSisChg',
        *datetime_cols}

    # Identify float columns to round
    float_cols_to_round = [
        col for col in df.select_dtypes(include='float').columns
        if col not in exclude_cols
    ]

    # Round the appropriate float columns to 5 decimal places
    df.loc[:, float_cols_to_round] = df[float_cols_to_round].round(5)
    return df


def read_and_clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reads a df with specified data types, cleans invalid rows, and rounds float columns.

    - Parses 'reportdatetime' and 'datestored' as datetime
    - Drops rows where either datetime is invalid
    - Rounds all float columns except 'ESN', 'ACID', 'reportdatetime', and 'datestored' to 5 decimals

    Parameters
    ----------
    Args:
         - df (pd.DataFrame): DataFrame to be filtered

    Returns
    -------
         - df (pd.DataFrame): cleaned DataFrame
    """
    """
    # Define known dtypes
    dtype_spec = {
        'ESN': 'Int64',
        'operator': 'string',
        'equipmentid': 'Int64',
        'ACID': 'string',
        'ENGPOS': 'Int64',
        'DSCID': 'Int64',
        'days_since_prev': 'float',
        'NEW_FLAG': 'Int64',
        'SISTER_ESN': 'Int64',
        'FlagSV': 'Int64',
        'FlagSisChg': 'Int64'
    }

    # Set dtypes
    # safe_map = {col: typ for col, typ in dtype_spec.items() if col in df.columns}
    # df = df.astype(safe_map)
    for col, dtype in dtype_spec.items():
        try:
            if dtype == 'Int64':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:
                df[col] = df[col].astype(dtype)

        except Exception as e:
            log_message(f"Failed to convert column '{col}' to {dtype}. Error: {e}")

    """
    # set ESN col as Int64
    df['ESN'] = df['ESN'].astype(int)

    # List of datetime columns to parse
    datetime_cols = ['reportdatetime', 'datestored']
    # Coerce bad datetimes to NaT (Not a Time)
    for col in datetime_cols:
        df[col] = pd.to_datetime(
            df[col],
            format='%Y-%m-%d %H:%M:%S',
            errors='coerce')

    # Drop rows where any datetime column is invalid
    df = df.dropna(subset=datetime_cols)

    # Exclude certain columns from rounding
    exclude_cols = {
        'ESN',
        'operator',
        'equipmentid',
        'ACID',
        'ENGPOS',
        'DSCID',
        'days_since_prev',
        'NEW_FLAG',
        'SISTER_ESN',
        'FlagSV',
        'FlagSisChg',
        *datetime_cols}

    # Identify float columns to round
    float_cols_to_round = [
        col for col in df.select_dtypes(include='float').columns
        if col not in exclude_cols
    ]

    # Round the appropriate float columns to 5 decimal places
    df.loc[:, float_cols_to_round] = df[float_cols_to_round].round(5)

    return df
