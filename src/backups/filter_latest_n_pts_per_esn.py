import numpy as np
import pandas as pd
import os
from backups.read_and_clean import read_and_clean_df
from src.utils.import_data_filters import filter_parameters
from src.utils.days_difference import days_difference
from src.utils.log_file import LOG_FILE, log_message, debug_info
# to print current line number "print(inspect.currentframe().f_lineno)"
import inspect


def filter_latest_n_pts_per_esn(
    df: pd.DataFrame,
    flight_phase: str,
    timestamp_str_initial: str,
    DebugOption: int = 0
) -> pd.DataFrame:
    """
    For each unique ESN, keeps only the most recent n_pts rows based on 'reportdatetime'.
    Returns a single DataFrame with all such rows, sorted by 'reportdatetime' descending.

    Parameters
    ----------
    Args:
        - df (pd.DataFrame): Input dataframe.
        - flight_phase (str): input string containing flight phase.
        - timestamp_str_initial (str): timestamp.
        - DebugOption: int = 1, option to create ad save a copy of the data in CSV format.

    Returns
    -------
        - result_df (pd.DataFrame): DataFrame with filtered data

    """
    # Set df columns types
    result_df = read_and_clean_df(df)

    result_df = filter_parameters(result_df, flight_phase)
    result_df['TS25S__NOM_K'] = result_df['TS25S__NOM_K'] - 273.15
    result_df['TS30S__NOM_K'] = result_df['TS30S__NOM_K'] - 273.15
    result_df['TGTS__NOM_K'] = result_df['TGTS__NOM_K'] - 273.15
    result_df = days_difference(result_df)
    # Convert timestamp to datetime
    timestamp_dt = pd.to_datetime(
        timestamp_str_initial,
        format='%Y-%m-%d %H:%M:%S')

    # Update the NEW_FLAG column based on the condition
    result_df.loc[result_df['reportdatetime'] <= timestamp_dt, 'NEW_FLAG'] = 0
    result_df.loc[result_df['reportdatetime'] > timestamp_dt, 'NEW_FLAG'] = 1

    # Add remaining columns
    columns_to_add = [
        'PS26__DEL_PC',
        'T25__DEL_PC',
        'P30__DEL_PC',
        'T30__DEL_PC',
        'TGTU__DEL_PC',
        'NL__DEL_PC',
        'NI__DEL_PC',
        'NH__DEL_PC',
        'FF__DEL_PC',
        'P160__DEL_PC',
        'SISTER_ESN',  # int
        'PS26__DEL_PC_E2E',
        'T25__DEL_PC_E2E',
        'P30__DEL_PC_E2E',
        'T30__DEL_PC_E2E',
        'TGTU__DEL_PC_E2E',
        'NL__DEL_PC_E2E',
        'NI__DEL_PC_E2E',
        'NH__DEL_PC_E2E',
        'FF__DEL_PC_E2E',
        'P160__DEL_PC_E2E',
        'FlagSV',  # int
        'FlagSisChg',  # int
        'PS26__DEL_PC_E2E_MAV_NO_STEPS',
        'T25__DEL_PC_E2E_MAV_NO_STEPS',
        'P30__DEL_PC_E2E_MAV_NO_STEPS',
        'T30__DEL_PC_E2E_MAV_NO_STEPS',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS',
        'NL__DEL_PC_E2E_MAV_NO_STEPS',
        'NI__DEL_PC_E2E_MAV_NO_STEPS',
        'NH__DEL_PC_E2E_MAV_NO_STEPS',
        'FF__DEL_PC_E2E_MAV_NO_STEPS',
        'P160__DEL_PC_E2E_MAV_NO_STEPS',
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
        'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_400'
    ]
    for col in columns_to_add:
        if 'DEL' in col:
            result_df.loc[result_df['reportdatetime']
                          > timestamp_dt, col] = np.nan

        else:
            result_df.loc[result_df['reportdatetime'] > timestamp_dt, col] = 0

    result_df = result_df.sort_values(
        by='reportdatetime',
        ascending=False).reset_index(
        drop=True)
    # Saves function output to CSV file
    if DebugOption == 1:
        filename_filtered = f"FILTERED_{flight_phase}.csv"
        result_df.to_csv(filename_filtered)
        log_message(f"{debug_info()} - filtered data saved to: {filename_filtered}")
    return result_df
