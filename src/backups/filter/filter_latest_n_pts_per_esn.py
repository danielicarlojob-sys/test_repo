import numpy as np
import pandas as pd
from backups.read_and_clean import read_and_clean_df
from src.utils.import_data_filters import filter_parameters
from src.utils.days_difference import days_difference
from src.utils.log_file import log_message, debug_info

def filter_latest_n_pts_per_esn(
    df: pd.DataFrame,
    flight_phase: str,
    timestamp_str_initial: str,
    DebugOption: int = 0
) -> pd.DataFrame:
    """
    Filters and transforms engine sensor data for a given flight phase and timestamp.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    flight_phase : str
        Flight phase to filter by.
    timestamp_str_initial : str
        Reference timestamp for flagging and filtering.
    DebugOption : int, optional
        If set to 1, saves the filtered DataFrame to CSV.

    Returns
    -------
    pd.DataFrame
        Transformed and filtered DataFrame.
    """

    # Early exit for empty input
    if df.empty:
        return df.copy()

    # Clean and filter input
    result_df = read_and_clean_df(df.copy())
    result_df = filter_parameters(result_df, flight_phase)

    # Convert temperatures from Kelvin to Celsius
    for col in ["TS25S__NOM_K", "TS30S__NOM_K", "TGTS__NOM_K"]:
        if col in result_df.columns:
            result_df[col] = result_df[col] - 273.15

    # Apply date-based logic
    result_df = days_difference(result_df)
    timestamp_dt = pd.to_datetime(timestamp_str_initial, format='%Y-%m-%d %H:%M:%S')

    result_df["NEW_FLAG"] = np.where(
        result_df["reportdatetime"] > timestamp_dt, 1, 0
    )

    # Define columns to add
    columns_to_add = [
        # DEL_PC columns
        *[f"{prefix}__DEL_PC" for prefix in ["PS26", "T25", "P30", "T30", "TGTU", "NL", "NI", "NH", "FF", "P160"]],
        "SISTER_ESN", "FlagSV", "FlagSisChg",
        # E2E variants
        *[f"{prefix}__DEL_PC_E2E" for prefix in ["PS26", "T25", "P30", "T30", "TGTU", "NL", "NI", "NH", "FF", "P160"]],
        # MAV_NO_STEPS
        *[f"{prefix}__DEL_PC_E2E_MAV_NO_STEPS" for prefix in ["PS26", "T25", "P30", "T30", "TGTU", "NL", "NI", "NH", "FF", "P160"]],
        # LAG variants
        *[f"{prefix}__DEL_PC_E2E_MAV_NO_STEPS_LAG_{lag}" for lag in [50, 100, 200, 400]
          for prefix in ["PS26", "T25", "P30", "T30", "TGTU", "NL", "NI", "NH", "FF", "P160"]]
    ]

    # Initialize and assign values
    for col in columns_to_add:
        if 'DEL' in col:
            result_df[col] = np.nan
        else:
            result_df[col] = 0

        # Apply conditional assignment only for rows after timestamp
        mask = result_df["reportdatetime"] > timestamp_dt
        if 'DEL' in col:
            result_df.loc[~mask, col] = np.nan
        else:
            result_df.loc[~mask, col] = 0

    # Sort by datetime
    result_df = result_df.sort_values(by="reportdatetime", ascending=False).reset_index(drop=True)

    # Optional debug output
    if DebugOption == 1:
        filename_filtered = f"FILTERED_{flight_phase}.csv"
        result_df.to_csv(filename_filtered)
        log_message(f"{debug_info()} - filtered data saved to: {filename_filtered}")

    return result_df
