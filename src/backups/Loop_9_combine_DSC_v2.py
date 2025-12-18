import os
import asyncio
import pandas as pd
import numpy as np

from src.utils.print_time_now import print_time_now
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message

def Loop_9_grouping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes engine performance reports and assigns a reference timestamp (`report_ref`)
    to groups of rows that satisfy specific criteria.

    Criteria for grouping rows:
    1. Rows must share the same ESN, operator, ACID, and ENGPOS.
    2. The set of DSCIDs within the group must match one of the valid sets:
       - [53, 54, 52]
       - [53, 52]
       - [53, 54]
       - [54, 52]
    3. The maximum reportdatetime minus the minimum reportdatetime within the group
       must be less than 1 hour.
    4. The `report_ref` is populated with the earliest `reportdatetime` in the group.

    Args:
        df (pd.DataFrame): Input DataFrame with columns
            ['ESN','operator','ACID','ENGPOS','DSCID','reportdatetime','row_sum']

    Returns:
        pd.DataFrame: Copy of the original DataFrame with a new column 'report_ref'.
    """

    # Make a copy of df to avoid modifying the original DataFrame
    df = df.copy()

    # Ensure reportdatetime is in datetime format
    df["reportdatetime"] = pd.to_datetime(df["reportdatetime"])

    # Define valid DSCID sets (convert to sets for easier comparison)
    valid_sets = [
        [53, 54, 52],
        [53, 52],
        [53, 54],
        [54, 52],
    ]

    # Initialize the new column with NaT (not-a-time, for datetimes)
    df["report_ref"] = pd.NaT

    # Group by the keys of interest
    grouped = df.groupby(["ESN", "operator", "ACID", "ENGPOS"], group_keys=False)

    # Iterate through each group
    for _, group in grouped:
        # Sort group by datetime to ensure chronological order
        group_sorted = group.sort_values("reportdatetime")

        # Check DSCID set of this group
        dscid_set = set(group_sorted["DSCID"].unique())
        # dscid_set = group_sorted["DSCID"].unique()

        print(f"---> group:{group}")
        print(f"---> dscid_set:{dscid_set}")
        # If DSCID set is one of the valid sets
        if any(dscid_set == s for s in valid_sets):
            # Compute time span
            time_span = group_sorted["reportdatetime"].max() - group_sorted["reportdatetime"].min()

            # Check if span < 1 hour
            if time_span < pd.Timedelta(hours=1):
                # Earliest timestamp in group
                ref_time = group_sorted["reportdatetime"].min()

                # Assign earliest timestamp to the 'report_ref' column
                df.loc[group_sorted.index, "report_ref"] = ref_time

    return df


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
    df_combined = (
        pd.concat([df[cols] for df in data_dict.values()], ignore_index=True)
          .sort_values('reportdatetime', ascending=True)
    )
    df_final = Loop_9_grouping(df_combined)
    # 8) (Optional) save out
    if save_csv:
        path_out = os.path.join(os.getcwd(), "Fleetstore_Data",
                                f"{Loop_9_combine_DSC.__name__}_combined_output.csv")
        df_final.to_csv(path_out, index=False)
        log_message(f"File saved to: {path_out}")

    return data_dict, df_final

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
