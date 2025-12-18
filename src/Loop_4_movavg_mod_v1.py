import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from src.utils.log_file import log_message, debug_info, f_lineno
from src.utils.load_data import load_temp_data as ltd

def min_adjusted_value(index_list: list[int], other_integer: int, window_length: int = 21) -> int:
    """
    Compute the maximum adjusted value from an index list after subtracting a window length, 
    or return a fallback value if no valid adjustment is possible.

    This function takes a list of integer indices, subtracts a fixed `window_length` 
    from each value, and filters out any results that are negative. 
    It then returns the maximum of the adjusted values. 
    If the input list is empty, the function returns `other_integer`. 
    If no non-negative adjusted values exist (i.e., all adjusted values are negative), 
    the function returns 0.

    Args:
         - `index_list` (list[int]): 
            A list of integer indices to adjust. Can be empty.
         - `other_integer` (int): 
            A fallback integer returned if `index_list` is empty.
         - `window_length` (int, optional): 
            The amount to subtract from each element of `index_list`. 
            Defaults to 21.

    Returns:
         - `int`: 
            - `other_integer` if `index_list` is empty.  
            - `0` if all adjusted values are negative (i.e., `window_length` is 
              greater than every value in `index_list`).  
            - The maximum non-negative adjusted value otherwise.
    """
    if not index_list:
        # if list is empty return other integer
        output_integer = other_integer
    else:
        # Subtract window length from values in index_list and keep non-negative results
        adjusted_list = [x - window_length for x in index_list if x - window_length >= 0]

        if adjusted_list == []:
            # window_length > max(index_list), but since index_list is not empty return 0
            output_integer = 0
        else:
            output_integer = max(adjusted_list)

    return output_integer



def Loop_4_movavg(
        df: pd.DataFrame,
        flight_phase: str = None,
        WindowSemiWidth: int = 10,
        DebugOption: int = 1) -> pd.DataFrame:
    """
    Applies a robust moving average (using trimmed mean) to selected columns of a DataFrame,
    grouped by ESN where NEW_FLAG == 1, and handles discontinuities and missing data.

    Parameters
    ----------
     - `df` : pd.DataFrame
        Input DataFrame containing columns for ESN, NEW_FLAG, FlagSV, FlagSisChg, and the required delta columns.
     - `flight_phase`: str = None, string referring to the current flight phase.
     - `WindowSemiWidth` : int, optional
        Semi-width of the moving window (default is 10). The full window size is 2 * WindowSemiWidth + 1.
     - `DebugOption` : int, optional (default = 1), switch to create a copy of the output data in csv format.

    Returns
    -------
     - `df`: pd.DataFrame
        The input DataFrame with additional columns containing the robust moving averages.
    """
    # Define function's dysplay name
    Loop_4_movavg.display_name = "LOOP 4 - Moving average"    
    
    # Define window size
    win_size = 2 * WindowSemiWidth + 1
    
    # Define delta columns to process
    delta_cols = [
        'PS26__DEL_PC',
        'T25__DEL_PC',
        'P30__DEL_PC',
        'T30__DEL_PC',
        'TGTU__DEL_PC',
        'NL__DEL_PC',
        'NI__DEL_PC',
        'NH__DEL_PC',
        'FF__DEL_PC',
        'P160__DEL_PC']

    # Define corresponding E2E and output columns
    E2E_cols = [col + '_E2E' for col in delta_cols]
    E2E_MAV_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Ensure required columns exist
    for col in E2E_cols + E2E_MAV_cols:
        if col not in df.columns:
            df[col] = np.nan
    
    # Empty df check
    if df.empty:
        return df
    
    # Preventive sort old to new data and reset df index
    df = df.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)
    
    # Split df between old and new data
    df_old = df[df['NEW_FLAG'] == 0].copy()
    df_new = df[df['NEW_FLAG'] == 1].copy()

    # Filter ESNs with NEW_FLAG == 1
    esns = df_new['ESN'].unique()
    # .reset_index(drop=True)
    esn_temp_container = []

    for esn in tqdm(esns, desc=f" LOOP 4 {flight_phase} progress", unit="ESN"):

        # Filter rows for current ESN
        esn_mask = df['ESN'] == esn
        df_esn = df[esn_mask].sort_values(by='reportdatetime', ascending=True).reset_index(drop=True).copy()
        df_esn_new = df_esn[df_esn['NEW_FLAG']==1].copy()
        df_esn_new_idx_list = df_esn_new.index.to_list()
        df_esn_old = df_esn[df_esn['NEW_FLAG']==0].copy()
        df_esn_old_idx_list = df_esn_old.index.to_list()

        min_idx_df_new = min(df_esn_new_idx_list)
        idx_start = min_adjusted_value(
            index_list = df_esn_old_idx_list, 
            other_integer = min_idx_df_new, 
            window_length = win_size)
        df_temp = df_esn.iloc[idx_start:].copy()
        # print("df_temp:\n",df_temp[["reportdatetime","FlagSV", "FlagSisChg", "PS26__DEL_PC_E2E", "PS26__DEL_PC_E2E_MAV_NO_STEPS"]])
        # Rolling mean (average) over the last `win_size` flights
        mask = (df_temp['FlagSV'] == 0) & (df_temp['FlagSisChg'] == 0)
        df_temp[E2E_MAV_cols] = (
            df_temp[E2E_cols]
            .where(mask)                     # mask first
            .rolling(win_size, min_periods=win_size)
            .mean()
            .round(5)
        )
        
        ### df_temp = df_temp[df_temp['NEW_FLAG']==1].sort_values(
        ### by='reportdatetime', ascending=False).drop_duplicates(keep='last').copy()
        df_temp = df_temp[df_temp['NEW_FLAG']==1]
        esn_temp_container.append(df_temp)
        #############
    # print(debug_info())
    df_out_for_loop = pd.concat(esn_temp_container, ignore_index=True)
    # print(debug_info())
    if not df_old.empty:
        df_out = pd.concat([df_old, df_out_for_loop], ignore_index=True)
    else:
        df_out = df_out_for_loop


    # Remove duplicates
    df_out = df_out.sort_values(
        by='reportdatetime',
        ascending=True).drop_duplicates(keep='last').reset_index(drop=True)

    if DebugOption == 1:
        # Save a temporary CSV file for debugging or traceability
        current_dir = os.getcwd()
        fleetore_dir = os.path.join(current_dir, "Fleetstore_Data")
        path_temp = os.path.join(fleetore_dir, f"LOOP_4_{flight_phase}_mod_v1.csv")
        log_message(f" File saved to: {path_temp}")
        df_out.to_csv(path_temp)

    return df_out


if __name__ == "__main__":
    load_data = 1
    if load_data == 1:
        current_dir = os.getcwd()
        log_message(f"        current dir: {current_dir}")
        LOOP_str = "LOOP_3"

        try:
            data_dict = ltd(LOOP_str)
            df = data_dict['cruise']
            df = Loop_4_movavg(df, 'cruise')
            log_message("Loop 4 completed!")
            #path_temp = os.path.join(current_dir, "loop4_temp.csv")
            #log_message(f"file will be saved to : {path_temp}")
            #df.to_csv(path_temp)
            
        except Exception as e:

            log_message(f"        error fetching data: {e}")
