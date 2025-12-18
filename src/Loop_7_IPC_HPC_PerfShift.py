import os
import pandas as pd
import numpy as np
from src.utils.log_file import log_message


def Loop_7_IPC_HPC_PerfShift(
        df: pd.DataFrame,
        lag_list: list[int] = [50, 100, 200, 400],
        save_csv: bool = True,
        flight_phase: str = "default",

) -> pd.DataFrame:
    """
    Loop_7 - Assigns IPC and HPC performance damage shifts based on
    best-fit variable identifiers.

    For each lag window (e.g., 50, 100, 200, 400), the function checks whether
    the IPC_DAMAGE_SHIFT{lag} and HPC_DAMAGE_SHIFT{lag} columns exist. If not,
    they are created and filled with NaN. Then, for rows flagged as new
    (NEW_FLAG == 1), the function assigns the appropriate VARx_SHIFT value to
    IPC_DAMAGE_SHIFT or HPC_DAMAGE_SHIFT depending on the identifier value.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing:
            - NEW_FLAG column (0 = old data, 1 = new data)
            - For each lag in lag_list:
                * VAR1_SHIFT{lag}, VAR2_SHIFT{lag}, VAR3_SHIFT{lag}
                * VAR1_IDENTIFIER{lag}, VAR2_IDENTIFIER{lag}, VAR3_IDENTIFIER{lag}
                * IPC_DAMAGE_SHIFT{lag}, HPC_DAMAGE_SHIFT{lag} (optional)
    lag_list : list of int, default [50, 100, 200, 400]
        List of lag values (suffixes) to process.
    save_csv : bool, default True
        If True, saves the updated DataFrame to CSV.
    flight_phase : str, default "default"
        Used in the output filename if save_csv is True.

    Returns
    -------
    pd.DataFrame
        The updated DataFrame with IPC_DAMAGE_SHIFT and HPC_DAMAGE_SHIFT
        filled in for rows where NEW_FLAG == 1.
    """
    # Define function's dysplay name
    Loop_7_IPC_HPC_PerfShift.display_name = "LOOP 7 - IPC HPC Performance Shift"    

    # Make a copy so the original DataFrame is not modified in place
    df = df.copy()

    # Preventive sort old to new data and reset df index
    df = df.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)


    # Split into two groups:
    df_new = df[df['NEW_FLAG'] == 1].copy()
    df_old = df[df['NEW_FLAG'] == 0]

    for Lag in lag_list:
        ipc_col = f'IPC_DAMAGE_SHIFT{Lag}'
        hpc_col = f'HPC_DAMAGE_SHIFT{Lag}'
        # Ensure IPC and HPC columns exist
        for col in [ipc_col, hpc_col]:
            if col not in df_new.columns:
                df_new[col] = np.nan

        # Row-wise assignment using .loc and masks
        for i in range(1, 4):
            id_col = f'VAR{i}_IDENTIFIER{Lag}'
            shift_col = f'VAR{i}_SHIFT{Lag}'

            # Assign IPC damage where identifier == "IPC ETA"
            ###### NB I have negated the efficiency change so that more +ve is always ######
            ###### more likely to be damage                                           ######
            df_new.loc[df_new[id_col] == "IPC ETA",
                       ipc_col] = -df_new.loc[df_new[id_col] == "IPC ETA", shift_col]

            # Assign HPC damage where identifier == "HPC ETA"
            df_new.loc[df_new[id_col] == "HPC ETA",
                       hpc_col] = -df_new.loc[df_new[id_col] == "HPC ETA", shift_col]

    # Merge updated new rows with old rows and restore original row order
    df_final = pd.concat([df_old, df_new]).sort_index()
    # Remove duplicates
    df_final = df_final.sort_values(
        by='reportdatetime',
        ascending=True).drop_duplicates(keep='last')

    # Optionally save results to CSV
    if save_csv:
        
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"LOOP_7_{flight_phase}_efficiency_negated.csv")
        df_final.to_csv(path_temp, index=False)
        log_message(f"File saved to: {path_temp}")
        
    return df_final


# ==============================
# Script entry point for testing
# ==============================

if __name__ == "__main__":
    import asyncio
    from src.utils.print_time_now import print_time_now
    from src.utils.load_data import load_temp_data as ltd
    from src.utils.async_main import main as async_main
    
    root = os.getcwd()
    data_folder = os.path.join(root, "Fleetstore_Data")
    data_dict = ltd("LOOP_6", data_folder)
    func = Loop_7_IPC_HPC_PerfShift
    try:
        # LOOP 7 - IPC HPC Performance Shift
        log_message(
            f"{func.__name__} at {str(print_time_now())}")
        data_dict = asyncio.run(async_main(
                                            data_dict = data_dict, 
                                            Fleetstore_data_dir=data_folder, 
                                            process_function = func))
        log_message(
            f"{func.__name__} at {str(print_time_now())}")
    except Exception as e:
        log_message(f"Could not execute {func.__name__}: {e}")
