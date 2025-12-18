import os
import pandas as pd
from tqdm import tqdm
from src.utils.enforce_dtypes import enforce_dtypes
from src.utils.log_file import log_message
from src.utils.load_data import load_temp_data as ltd

def Loop_3_flag_sv_and_eng_change(
        df: pd.DataFrame,
        flight_phase: str = None,
        DebugOption: int = 1,
        min_sv_dur: float = 40) -> pd.DataFrame:
    """
    Identify and flag shop visit (SV) events and sister engine changes in a time-series DataFrame.

    This function processes engine data to mark two types of events:
    - Shop Visit (`FlagSV`): Flag set to 1 if a new report is the first for that ESN,
      or if the time gap from the previous report exceeds `min_sv_dur`.
    - Sister Engine Change (`FlagSisChg`): Flag set to 1 if the reported `SISTER_ESN` value
      has changed from the previous row for the same ESN.

    The function operates only on rows where `NEW_FLAG == 1`, meaning only newly added records
    are considered for flagging.

    Parameters
    ----------
     - df : pd.DataFrame
        Input DataFrame. Must contain the following columns:
            - 'ESN'           : Engine serial number identifier.
            - 'NEW_FLAG'      : Binary flag (1 = new data row to process).
            - 'REPORTDATENUM' : Timestamp (or numerical date) of the report.
            - 'SISTER_ESN'    : ID of the sister engine installed.
            - 'days_since_prev' : Precomputed duration since the previous report for the same ESN.
     - flight_phase: str = None, string referring to the current flight phase.
     - DebugOption : int, optional (default = 1), switch to create a copy of the output data in csv format.
     - min_sv_dur : float, optional (default=40)
        Minimum duration (in the same units as 'REPORTDATENUM', usually days) required between
        two reports to consider them part of separate shop visits.

    Returns
    -------
     - df: pd.DataFrame
        A copy of the original DataFrame with two additional columns:
                - 'FlagSV'      : 1 if the row marks the start of a new shop visit, else 0.
                - 'FlagSisChg'  : 1 if the sister ESN changed since the previous row, else 0.
    """
    
    # Define function's dysplay name
    Loop_3_flag_sv_and_eng_change.display_name = "LOOP 3 - Shop Visit SV and Engine change"    
    
    # Create df copy to avoid warnings
    df_temp = df.copy()

    # Preventive sort old to new data and reset df index
    df_temp = df_temp.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)
    
    # Split the dataframe between old and new data
    df_new = df_temp[df_temp['NEW_FLAG']==1].copy()
    df_old = df_temp[df_temp['NEW_FLAG']==0]
    dtypes_list = df_old.dtypes


    df_new['FlagSV'] = 0  # Set up new column for SV shop visit
    df_new['FlagSisChg'] = 0  # Set up new column for engine change
    # Identify ESNs with new data
    esns_with_new = df_new.loc[df_new['NEW_FLAG'] == 1, 'ESN'].unique()

    for esn in tqdm(
            esns_with_new,
            desc=f"        LOOP 3 {flight_phase} progress",
            unit="ESN"):

        df_esn = df_new[df_new['ESN'] == esn].copy()
        indices = df_esn.index.tolist()

        for idx_pos, i in enumerate(indices):

            if idx_pos == 0:
                # First row for this ESN
                df_new.at[i, 'FlagSV'] = 1
                df_new.at[i, 'FlagSisChg'] = 0
            else:
                prev_idx = indices[idx_pos - 1]
                # Check if time gap is too large
                if df_new.at[i, 'days_since_prev'] > min_sv_dur:
                    df_new.at[i, 'FlagSV'] = 1
                else:
                    df_new.at[i, 'FlagSV'] = 0
                # Check for sister ESN change
                if df_new.at[i, 'SISTER_ESN'] == df_new.at[prev_idx, 'SISTER_ESN']:
                    df_new.at[i, 'FlagSisChg'] = 0
                else:
                    df_new.at[i, 'FlagSisChg'] = 1
    
    df_new = enforce_dtypes(df_new, dtypes_list)
    
    # Concatenate back old and new data
    df_conc = pd.concat([df_old, df_new], ignore_index=True)

    # Remove duplicates
    df_conc = df_conc.sort_values(
        by='reportdatetime',
        ascending=True).drop_duplicates(keep='last')
    
    # Saves function output to CSV file
    if DebugOption == 1:
        # Get current dir and Fleetstore_Data dir
        current_dir = os.getcwd()
        FleetStore_dir = os.path.join(current_dir, "Fleetstore_Data")
        if not os.path.exists(FleetStore_dir):
            os.makedirs(FleetStore_dir)
        # Save a temporary CSV file for debugging or traceability
        path_temp = os.path.join(FleetStore_dir, f"LOOP_3_{flight_phase}.csv")
        log_message(f"        File saved to: {path_temp}")
        df_conc.to_csv(path_temp)
    return df_conc


# Manually run LOOP_3
if __name__ == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_2"
    try:
        data_dict = ltd(LOOP_str)
        flight_phase = 'cruise'
        df = data_dict[flight_phase]

        df = Loop_3_flag_sv_and_eng_change(df, flight_phase, DebugOption=1)
        log_message("Loop 3 completed!")

    except Exception as e:

        log_message(f"        error fetching data: {e}")
