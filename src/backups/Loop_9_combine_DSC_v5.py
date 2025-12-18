import os
import asyncio
import pandas as pd
import numpy as np

from src.utils.print_time_now import print_time_now
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message



def merge_flight_phases(
    df_takeoff: pd.DataFrame,
    df_climb: pd.DataFrame,
    df_cruise: pd.DataFrame
) -> pd.DataFrame:

    """
    Merge take-off, climb and cruise DataFrames into a single DataFrame
    with aligned timestamps and row_sum values.

    This function does the following:
      1. Sorts each phase-specific DataFrame by the grouping keys and timestamp.
      2. Renames the timestamp and row_sum columns to carry a phase suffix.
      3. For each take-off record, finds the *next* climb record whose
         reportdatetime is strictly greater than the take-off time.
      4. Then finds the *next* cruise record strictly after the selected climb time.
      5. Enforces that the total span from take-off to cruise is at most 1 hour.
         If the cruise falls outside that window, both climb and cruise
         timestamps (and their row_sum values) are zeroed out (NaT / NaN).
      6. Returns only the keys, the three timestamps, and the three row_sum columns.

    Google-style docstring:

    Args:
        df_takeoff (pd.DataFrame): 
            DataFrame containing take-off observations. Must have columns
            ['ESN', 'operator', 'ACID', 'ENGPOS', 'DSCID', 'reportdatetime', 'row_sum']
            where DSCID == 53.
        df_climb (pd.DataFrame):
            Climb observations with the same schema (DSCID == 54).
        df_cruise (pd.DataFrame):
            Cruise observations with the same schema (DSCID == 52).

    Returns:
        pd.DataFrame:
            A new DataFrame with columns
            ['ESN', 'operator', 'ACID', 'ENGPOS',
             'reportdatetime_take-off', 'reportdatetime_climb',
             'reportdatetime_cruise',
             'row_sum_take-off', 'row_sum_climb', 'row_sum_cruise'].
            Each row corresponds to one take-off record and its paired
            climb/cruise within one hour.  Missing or out-of-window
            matches are represented as NaT (for datetimes) or NaN
            (for row_sum).
    """


    # 1. Define the grouping keys and the final output column order
    keys = ['ESN', 'operator', 'ACID', 'ENGPOS']
    final_cols = [
        *keys,
        'reportdatetime_take-off', 'reportdatetime_climb', 'reportdatetime_cruise',
        'row_sum_take-off', 'row_sum_climb', 'row_sum_cruise'
    ]

    # 2. Prepare each DF: sort and rename columns
    sort_cols = keys + ['reportdatetime']

    df_t = (
        df_takeoff
        .sort_values(sort_cols)
        .reset_index(drop=True)
        .rename(columns={
            'reportdatetime': 'reportdatetime_take-off',
            'row_sum': 'row_sum_take-off'
        })
    )

    df_k = (
        df_climb
        .sort_values(sort_cols)
        .reset_index(drop=True)
        .rename(columns={
            'reportdatetime': 'reportdatetime_climb',
            'row_sum': 'row_sum_climb'
        })
    )

    df_r = (
        df_cruise
        .sort_values(sort_cols)
        .reset_index(drop=True)
        .rename(columns={
            'reportdatetime': 'reportdatetime_cruise',
            'row_sum': 'row_sum_cruise'
        })
    )

    # 3. Find the *next* climb timestamp > take-off
    df_t['__merge_key_climb'] = df_t['reportdatetime_take-off'] + pd.Timedelta(microseconds=1)
    df_t = df_t.sort_values(keys + ['__merge_key_climb']) # 🔑 ensure sorted

    df_k = df_k.sort_values(keys + ['reportdatetime_climb']) # 🔑 ensure sorted

    merged = pd.merge_asof(
        left=df_t,
        right=df_k,
        left_on='__merge_key_climb',
        right_on='reportdatetime_climb',
        by=keys,
        direction='forward',
        allow_exact_matches=True
    ).drop(columns='__merge_key_climb')

    # 4. Find the *next* cruise timestamp > climb
    merged['__merge_key_cruise'] = merged['reportdatetime_climb'] + pd.Timedelta(microseconds=1)
    merged = merged.sort_values(keys + ['__merge_key_cruise']) # 🔑 ensure sorted

    df_r = df_r.sort_values(keys + ['reportdatetime_cruise']) # 🔑 ensure sorted

    merged = pd.merge_asof(
        left=merged,
        right=df_r,
        left_on='__merge_key_cruise',
        right_on='reportdatetime_cruise',
        by=keys,
        direction='forward',
        allow_exact_matches=True
    ).drop(columns='__merge_key_cruise')

    # 5. Enforce the 1-hour window from take-off to cruise
    span = merged['reportdatetime_cruise'] - merged['reportdatetime_take-off']
    too_long = span > pd.Timedelta(hours=1)

    for dt_col, sum_col in [
        ('reportdatetime_climb', 'row_sum_climb'),
        ('reportdatetime_cruise', 'row_sum_cruise')
    ]:
        merged.loc[too_long, dt_col] = pd.NaT
        merged.loc[too_long, sum_col] = np.nan

    # 6. If timestamps are NaT, force row_sum to NaN
    merged.loc[merged['reportdatetime_climb'].isna(), 'row_sum_climb'] = np.nan
    merged.loc[merged['reportdatetime_cruise'].isna(), 'row_sum_cruise'] = np.nan

    # 7. Return final sliced DataFrame
    result = merged[final_cols].copy()

    return result



def Loop_9_combine_DSC(
        data_dict: dict,
        Lim_dict: dict = {  'EtaThresh': [0, 0.2, 0.4, 0.6, 0.8, 1.0],
                            'RelErrThresh': [0.3],
                            'lim': 0.07,
                            'nEtaThresh': 6,
                            'nRelErrThresh': 1,
                            'num': 3},
        save_csv: bool = True
                            ) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Combines and processes rows in a DataFrame based on a threshold condition.

    This function separates the input DataFrame into two groups based on the 'NEW_FLAG' column,
    computes a row-wise count of values in columns containing 'FRACTION_GT' that exceed a specified
    threshold, and merges the processed data back together. Optionally, the result can be saved to CSV.

    Parameters
    -----
         - data_dict: (dict): Input dictionary containing DataFrames for each flight phase performance metrics and flags.
         - Lim_dict (dict, optional): Dictionary containing threshold parameters. Must include:
            - 'lim' (float): Threshold value for comparison.
            - 'num' (int): Unused in this function but retained for compatibility.
            - Other keys like 'EtaThresh', 'RelErrThresh', etc., are accepted but not used directly.
         - save_csv (bool, optional): If True, saves the resulting DataFrame to a CSV file. Defaults to True.

    Returns:
    -------
         - data_dict: (dict): Output dictionary containing updated DataFrames
         - df_combined (pd.DataFrame): A new DataFrame with updated rows, sorted by 'reportdatetime', and including a
         'row_sum' column that counts how many 'FRACTION_GT' values exceed the threshold for each flight phase.
    """

    # extract data from Lim_dict
    Limit = Lim_dict['lim']
    Num = Lim_dict['num']
    flight_phases = data_dict.keys()

    for flight_phase in tqdm(flight_phases, desc=" LOOP 9 ", unit="Flight Phase"):

        df = data_dict[flight_phase].copy()

        # Split into two groups:
        df_new = df[df['NEW_FLAG'] == 1]
        df_old = df[df['NEW_FLAG'] == 0]

        # define the Dataframe columns subset and extract them
        cols = list(df_new.columns)
        cols_subset = [col for col in cols if "FRACTION_GT" in col]

        # Sum times values in cols_subest > Limit row-wise, skipping NaNs
        df_new_copy = df_new.copy()
        df_new_copy['row_sum'] = (df_new_copy[cols_subset] >= Limit).sum(axis=1, skipna=True)

        # Merge updated new rows with old rows and restore original row order
        df_final = pd.concat([df_old, df_new_copy]).sort_values(
                by='reportdatetime',
                ascending=False).reset_index(
                drop=True)

        data_dict[flight_phase] = df_final

        # Optionally save results to CSV
        func_name = Loop_9_combine_DSC.__name__
        if save_csv:
            path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"{func_name}_{flight_phase}_whole.csv")
            df_final.to_csv(path_temp, index=False)
            log_message(f"File saved to: {path_temp}")

    #####################################################################################
    #####################################################################################

    # 1) Fully build + sort the combined frame
    cols = ['ESN','operator','ACID','ENGPOS','DSCID','reportdatetime','row_sum']
    dict_temp ={}
    for fp in data_dict.keys():
        dict_temp[fp] = data_dict[fp][cols]
    try:
        df_merged = merge_flight_phases(df_takeoff = dict_temp['take-off'],
                                        df_climb = dict_temp['climb'],
                                        df_cruise = dict_temp['cruise']) 
    
        # (Optional) save out
        if save_csv:
            path_out = os.path.join(os.getcwd(), "Fleetstore_Data",
                                    f"{Loop_9_combine_DSC.__name__}_merged_output.csv")
            df_merged.to_csv(path_out, index=False)
            log_message(f"File saved to: {path_out}")
    except Exception as e:
        log_message(f"Could not run {merge_flight_phases.__name__}: {e}")
    return data_dict, df_merged

# ==============================
# Script entry point for testing
# ==============================

if __name__ == "__main__":
    from src.utils.load_data import load_temp_data as ltd
    from src.utils.Initialise_Algorithm_Settings_engine_type_specific import (
        Initialise_Algorithm_Settings_engine_type_specific,
        Xrates_dic_vector_norm,
    )
    from src.utils.async_main import main as async_main

    root = os.getcwd()
    data_folder = os.path.join(root, "Fleetstore_Data")
    lim_dict, Xrates_loaded = Initialise_Algorithm_Settings_engine_type_specific()
    Xrates_loaded = Xrates_dic_vector_norm(Xrates_loaded)
    #print(f">>> data_folder:{os.path.normpath(data_folder)}")
    #flight_phases = ["cruise","climb","take-off"]
    data_dict = ltd("LOOP_8", data_folder)

    try:
        # LOOP 9 - combine DSC
        log_message(
            f"Start LOOP 9 - combine DSC at {str(print_time_now())}")

        """
        # async flight phase specific function
        data_dict = asyncio.run(async_main(
                                            data_dict = data_dict,
                                            Fleetstore_data_dir=data_folder,
                                            process_function = Loop_9_combine_DSC,
                                            Lim_dict = lim_dict))
        """

        data_dict, df_combined = Loop_9_combine_DSC(data_dict=data_dict, Lim_dict=lim_dict)
        log_message(
            f"Completed LOOP 9 - combine DSC at {str(print_time_now())}")
    except Exception as e:
        log_message(f"Could not run LOOP 9 - combine DSC: {e}")
