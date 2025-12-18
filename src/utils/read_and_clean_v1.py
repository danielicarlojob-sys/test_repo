
import pandas as pd
from pandas import DataFrame
from typing import List
from src.utils.log_file import LOG_FILE, log_message, debug_info

## UNUSED ##
def parse_mixed_datetime_columns_vectorized(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Efficiently parse multiple datetime columns in a DataFrame that may contain mixed formats.
    
    This vectorized function parses each column in three steps:
    1. Format "%Y-%m-%d %H:%M:%S" (with seconds)
    2. Format "%Y-%m-%d %H:%M" (without seconds)
    3. Fallback using pandas' `infer_datetime_format=True` for any remaining unparsable values
    
    Values that cannot be parsed are set to NaT.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing the datetime columns.
        cols (List[str]): List of column names in df to parse as datetime.
        
    Returns:
        pd.DataFrame: DataFrame with the specified columns converted to datetime64[ns].
    """
    # Copy to avoid modifying original if needed
    df_copy = df.copy()

    for col in cols:
        s = df_copy[col]
        
        # Step 1: parse full format with seconds
        dt = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
        
        # Step 2: parse format without seconds for remaining NaT
        mask = dt.isna()
        if mask.any():
            dt.loc[mask] = pd.to_datetime(s[mask], errors="coerce", format="%Y-%m-%d %H:%M")
        
        # Step 3: fallback with inferred format
        mask = dt.isna()
        if mask.any():
            dt.loc[mask] = pd.to_datetime(s[mask], errors="coerce", infer_datetime_format=True)
        
        # Assign the parsed series back
        df_copy[col] = dt
    
    return df_copy

# Example usage:
# df_clean = parse_mixed_datetime_columns_vectorized(df, ["reportdatetime", "datestored"])


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

    # Read CSV
    cols_list = pd.read_csv(csv_path,nrows=0).columns.tolist()
    
    cols_list_int64 = ['ESN', 'equipmentid', 'ENGPOS', 'DSCID', 'NEW_FLAG']
    cols_list_Int64 = ['SISTER_ESN', 'FlagSV', 'FlagSisChg','row_sum']# Nullable Int64 cols list
    cols_list_datetime = ['reportdatetime', 'datestored']
    cols_list_object = ['operator', 'ACID']
    identifiers_cols = [col for col in cols_list if 'IDENTIFIER' in col]
    cols_list_float = [ col for col in cols_list if col not in cols_list_int64 + cols_list_Int64 + cols_list_datetime + cols_list_object + identifiers_cols ]
    
    int64_dict = {col:'int64' for col in cols_list if col in cols_list_int64}
    Int64_dict = {col:'Int64' for col in cols_list if col in cols_list_Int64}
    string_dict = {col:'string' for col in cols_list if col in cols_list_object + identifiers_cols}
    float_dict = {col:'Float64' for col in cols_list if col in cols_list_float}

    # Define known dtypes
    dtype_spec = int64_dict |  Int64_dict |  string_dict | float_dict
    df = pd.read_csv(csv_path, dtype=dtype_spec, parse_dates=cols_list_datetime)
    # Keep the following line to force convertion to datetime, is good practice to sanitize data from CSV import 
    #df[cols_list_datetime] = df[cols_list_datetime].apply(pd.to_datetime, errors="ignore", format="%Y-%m-%d %H:%M:%S") 
    df[cols_list_datetime] = df[cols_list_datetime].apply(pd.to_datetime, format="%Y-%m-%d %H:%M:%S") 


    
    # Round the appropriate float columns to 5 decimal places
    df.loc[:, cols_list_float] = df[cols_list_float].round(5)
    # log_message(f"df (historic data) .shape:{df.shape}")

    if columns_type_check == True:
        with open("historical_df_cols.txt", "w") as f:
                f.write(df.dtypes.to_string())
        for i, col_name in enumerate(df.columns):
            col_name = df.columns[i]
            log_message(f"Column {i}: {col_name}")
            log_message(f"pandas dtype: {df[col_name].dtype}")
            log_message(f"Python types present:\n{df[col_name].apply(type).value_counts()}")
            log_message(f"##############")

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
