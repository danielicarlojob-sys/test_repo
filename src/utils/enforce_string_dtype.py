import pandas as pd
import re

def enforce_string_dtype(df: pd.DataFrame, pattern: str = r"VAR\d+_IDENTIFIER\d+") -> pd.DataFrame:
    """
    Ensures all columns matching the given regex pattern are of pandas 'string' dtype.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to modify.
    pattern : str, optional
        Regex pattern to match column names. Defaults to 'VAR\\d+_IDENTIFIER\\d+'.

    Returns:
    -------
    pd.DataFrame
        The modified DataFrame with enforced string dtypes.
    """
    for col in df.columns:
        if re.fullmatch(pattern, col):
            df[col] = df[col].astype("string")
    return df