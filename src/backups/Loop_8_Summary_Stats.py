import os
import pandas as pd
import numpy as np
from tqdm import tqdm

from src.utils.log_file import log_message

def Loop_8_Summary_Stats(
        df: pd.DataFrame,
        lag_list: list[int] = [50, 100, 200, 400],
        save_csv: bool = True,
        flight_phase: str = "default",
        Lim_dict: dict = {  'EtaThresh': [0, 0.2, 0.4, 0.6, 0.8, 1.0],
                            'RelErrThresh': [0.3],
                            'lim': 0.07,
                            'nEtaThresh': 6,
                            'nRelErrThresh': 1,
                            'num': 3},
) -> pd.DataFrame:
    """
    Loop_8 - Compute summary statistics for IPC and HPC damage shifts.

    For each lag window, the function calculates:
        - Max value of IPC and HPC shifts
        - Mean value of IPC and HPC shifts
        - Fraction of points above given thresholds

    Only new rows (NEW_FLAG == 1) are processed. Old rows are preserved and
    merged back at the end.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing IPC_DAMAGE_SHIFT{lag} and HPC_DAMAGE_SHIFT{lag}.
    lag_list : list of int, default [50, 100, 200, 400]
        List of lag values (suffixes) to process.
    save_csv : bool, default True
        If True, saves the updated DataFrame to CSV.
    flight_phase : str, default "default"
        Used in the output filename if save_csv is True.
    threshold_list : list of float, default [0.5, 1.0]
        Thresholds for calculating exceedance fractions.

    Returns
    -------
    pd.DataFrame
        DataFrame with additional summary statistic columns.
    """

    # Make a copy so we don’t modify the original DataFrame in place
    df = df.copy()
    
    # Split into two groups:
    df_new = df[df['NEW_FLAG'] == 1].copy()
    df_old = df[df['NEW_FLAG'] == 0]
    """    
    log_message(f"---> df.shape - {flight_phase}:{df.shape}")
    log_message(f"---> df_new.shape - {flight_phase}:{df_new.shape}")
    log_message(f"---> df_old.shape - {flight_phase}:{df_old.shape}")
    """

    esn_with_new_data = df_new["ESN"].unique()
    threshold_list = Lim_dict['EtaThresh']
    loop_8_list_merged = []
    print(f"{flight_phase} - start esn for loop")
    for esn in tqdm(esn_with_new_data, desc=f" LOOP 8 {flight_phase} ", unit="esn"):
        df_esn_temp = df_new[df_new["ESN"] == esn].copy()
        # Loop over each lag window (e.g. 50, 100, 200, 400 flights)
        for Lag in lag_list:
            # Loop over both components: IPC and HPC
            for comp in ["IPC", "HPC"]:
                # Build the name of the input column, e.g. "IPC_DAMAGE_SHIFT50"
                shift_col = f"{comp}_DAMAGE_SHIFT{Lag}"
                
                # Add lag/component if the column doees not exist
                if shift_col not in df_esn_temp.columns:
                    df_esn_temp[shift_col] = np.nan
                    # Skip this lag/component if the column does not exist
                    #continue

                # Define output column names for max and mean
                max_col = f"{comp}_MAX{Lag}"
                mean_col = f"{comp}_MEAN{Lag}"

                # Rolling maximum over the last "Lag" flights
                # Example: for Lag=50, at row i, it looks back at the last 50 rows of df_new[shift_col] and takes the max
                df_esn_temp[max_col] = df_esn_temp[shift_col].rolling(Lag, min_periods=1).max()

                # Rolling mean (average) over the last "Lag" flights
                df_esn_temp[mean_col] = df_esn_temp[shift_col].rolling(Lag, min_periods=1).mean()


                # For each threshold value, compute the fraction of points above threshold
                for thr in threshold_list:
                    # Build output column name, e.g. "IPC_FRACTION_GT_0.5_50"
                    frac_col = f"{comp}_FRACTION_GT_{thr}_{Lag}"

                    # rolling(Lag) → gives a moving window of size "Lag"
                    # .apply(...) → lets us define our own function to compute on each window
                    # lambda x: np.mean(x > thr) → custom function:
                    #   - "x" is a numpy array containing the window values
                    #   - "x > thr" produces a boolean array (True/False)
                    #   - np.mean(...) converts True/False to 1/0 and averages them
                    #   - Result = fraction of points above threshold
                    df_esn_temp[frac_col] = (
                        df_esn_temp[shift_col].rolling(Lag, min_periods=1)
                        .apply(lambda x: np.mean(x > thr), raw=True)
                    )
        loop_8_list_merged.append(df_esn_temp)

    # Merge the esn specific processed dataframe
    df_new = pd.concat(loop_8_list_merged, ignore_index=True)
    
    # Merge updated new rows with old rows and restore original row order
    df_final = pd.concat([df_old, df_new]).sort_values(
            by='reportdatetime',
            ascending=False).reset_index(
            drop=True)
    """    
    log_message(f"---> df_new_post_merge.shape - {flight_phase}:{df_new.shape}")
    log_message(f"---> df_final.shape - {flight_phase}:{df_final.shape}")
    log_message(f"---> is df.shape == df_final.shape - {flight_phase}:{df.shape[0] == df_final.shape[0]}")
    """

    # Optionally save results to CSV
    if save_csv:
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"LOOP_8_{flight_phase}.csv")
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
    from src.utils.Initialise_Algorithm_Settings_engine_type_specific import (
        Initialise_Algorithm_Settings_engine_type_specific,
        Xrates_dic_vector_norm,
    )
    from src.utils.async_main import main as async_main
    root = os.getcwd()
    data_folder = os.path.join(root, "Fleetstore_Data")
    lim_dict, Xrates_loaded = Initialise_Algorithm_Settings_engine_type_specific()
    data_dict = ltd("LOOP_7", data_folder)
    


    try:
        # LOOP 8 - Summary Stats
        log_message(
            f"Start LOOP 8 - Summary Stats {str(print_time_now())}")
        data_dict = asyncio.run(async_main(
                                            data_dict = data_dict, 
                                            Fleetstore_data_dir=data_folder, 
                                            process_function = Loop_8_Summary_Stats,
                                            Lim_dict = lim_dict))
        log_message(
            f"Completed LOOP 8 - Summary Stats at {str(print_time_now())}")
    except Exception as e:
        log_message(f"Could not execute LOOP 8 - Summary Stats {e}")