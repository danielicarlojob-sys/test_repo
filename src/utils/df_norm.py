import pandas as pd
import numpy as np


def add_norm_column(
        df: pd.DataFrame,
        cols: list,
        new_col="norm",
        ord=2
) -> pd.DataFrame:
    """
    Adds a new column to df with the row-wise norm of the selected columns.

    Parameters
    ----------
    df : pd.DataFrame
        The input dataframe.
    cols : list of str
        The subset of column names to compute the norm from.
    new_col : str, optional
        Name of the new column (default "norm").
    ord : int or float, optional
        Order of the norm (default 2 → Euclidean norm).
        ord=1 → Manhattan norm, ord=np.inf → max abs value, etc.

    Returns
    -------
    df : pd.DataFrame
        The dataframe with the added norm column.
        The new column values are floats rounded to 8 decimal places.
    """
    df[new_col] = np.round(
        np.linalg.norm(df[cols].values, axis=1, ord=ord), 8
    ).astype(float)
    return df
