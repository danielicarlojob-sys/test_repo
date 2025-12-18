import os
import asyncio
import pandas as pd
import numpy as np

from src.utils.print_time_now import print_time_now
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message

def Loop_9(df: pd.DataFrame) -> pd.DataFrame:
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
        {53, 54, 52},
        {53, 52},
        {53, 54},
        {54, 52},
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

