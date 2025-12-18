import os
import pandas as pd
from src.utils.df_norm import add_norm_column
# from src.utils.log_file import LOG_FILE, log_message, debug_info


def lag_cols_indentifier(
    df: pd.DataFrame,
    mid_str: str = "__DEL_PC_MAV_NO_SV_STEPS_E2E_SHIFT",
    parameters: list = [
        'PS26',
        'T25',
        'P30',
        'T30',
        'TGT',
        'NL',
        'NI',
        'NH',
        'FF',
        'P160'],
        lag_list: list = [
            50,
            100,
            200,
        400]) -> pd.DataFrame:

    for lag in lag_list:
        cols = [par + mid_str + str(lag) for par in parameters]
        new_col = "OBSERVED_MAGNITUDE" + str(lag)
        add_norm_column(df, cols, new_col, ord=2)
        print(cols)
    return df
