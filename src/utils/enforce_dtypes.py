import pandas as pd

def enforce_dtypes(df: pd.DataFrame, dtypes: pd.Series) -> pd.DataFrame:
    """
    Ensures that a DataFrame conforms to the dtypes specified in a Pandas Series.

    Parameters:
    ----------
    df : pd.DataFrame
        The input DataFrame to be standardized.
    dtypes : pd.Series
        A Series mapping column names to expected dtypes (e.g., df.dtypes).

    Returns:
    -------
    pd.DataFrame
        A DataFrame with all expected columns present and cast to the correct dtype.
    """
    df = df.copy()

    for col, dtype in dtypes.items():
        if col not in df.columns:
            # Add missing column with correct dtype and NA values
            df[col] = pd.Series([pd.NA] * len(df), dtype=dtype)
        else:
            # Cast existing column to expected dtype
            df[col] = df[col].astype(dtype, errors='ignore')

    return df # Keeps all columns, including extras
