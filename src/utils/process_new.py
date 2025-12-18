import functools
import pandas as pd


def process_only_new(func):
    """
    Decorator: apply function only to rows with NEW_FLAG == 1,
    then merge result back with old rows.
    """
    @functools.wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
        # Split
        df_new = df[df["NEW_FLAG"] == 1].copy()
        df_old = df[df["NEW_FLAG"] == 0].copy()

        # Process new data
        df_new_processed = func(df_new, *args, **kwargs)

        # Merge
        df_out = pd.concat([df_old, df_new_processed])
        df_out = df_out.sort_index()  # keep original order

        return df_out
    return wrapper
