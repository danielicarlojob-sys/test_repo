import os
import pandas as pd
import numpy as np
from src.utils.print_time_now import print_time_now
import asyncio

from src.utils.log_file import log_message

def Loop_9_combine_DSC(
        df:pd.DataFrame, 
        flight_phase: str = "default",
        Lim_dict: dict = {  'EtaThresh': [0, 0.2, 0.4, 0.6, 0.8, 1.0],
                            'RelErrThresh': [0.3],
                            'lim': 0.07,
                            'nEtaThresh': 6,
                            'nRelErrThresh': 1,
                            'num': 3},
        save_csv: bool = True
                            ) -> pd.DataFrame:
    """
    Combines and processes rows in a DataFrame based on a threshold condition.

    This function separates the input DataFrame into two groups based on the 'NEW_FLAG' column,
    computes a row-wise count of values in columns containing 'FRACTION_GT' that exceed a specified
    threshold, and merges the processed data back together. Optionally, the result can be saved to CSV.

    Parameters
    -----
         - df (pd.DataFrame): Input DataFrame containing performance metrics and flags.
         - flight_phase (str, optional): Label to assign to the 'flight_phase' column and to name the output file. Defaults to "default".
         - Lim_dict (dict, optional): Dictionary containing threshold parameters. Must include:
            - 'lim' (float): Threshold value for comparison.
            - 'num' (int): Unused in this function but retained for compatibility.
            - Other keys like 'EtaThresh', 'RelErrThresh', etc., are accepted but not used directly.
         - save_csv (bool, optional): If True, saves the resulting DataFrame to a CSV file. Defaults to True.

    Returns:
    -------
         - df_final (pd.DataFrame): A new DataFrame with updated rows, sorted by 'reportdatetime', and including a
         'row_sum' column that counts how many 'FRACTION_GT' values exceed the threshold.
    """
    # extract data from Lim_dict
    Limit = Lim_dict['lim']
    Num = Lim_dict['num']
    
    # Split into two groups:
    df_new = df[df['NEW_FLAG'] == 1].copy()
    df_old = df[df['NEW_FLAG'] == 0]
    
    # define the Dataframe columns subset and extract them
    cols = list(df_new.columns)
    cols_subset = [col for col in cols if "FRACTION_GT" in col]
        
    # Sum times values in cols_subest > Limit row-wise, skipping NaNs 
    df_new['row_sum'] = (df_new[cols_subset] >= Limit).sum(axis=1, skipna=True)
    

    # Merge updated new rows with old rows and restore original row order
    df_final = pd.concat([df_old, df_new]).sort_values(
            by='reportdatetime',
            ascending=False).reset_index(
            drop=True)
    df_final['flight_phase'] = flight_phase
    
    # Optionally save results to CSV
    if save_csv:
        func_name = Loop_9_combine_DSC.__name__
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"{func_name}_{flight_phase}.csv")
        df_final.to_csv(path_temp, index=False)
        log_message(f"File saved to: {path_temp}")
    
    return df_final


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
        data_dict = asyncio.run(async_main(
                                            data_dict = data_dict, 
                                            Fleetstore_data_dir=data_folder, 
                                            process_function = Loop_9_combine_DSC,
                                            Lim_dict = lim_dict))
        log_message(
            f"Completed LOOP 9 - combine DSC at {str(print_time_now())}")
    except Exception as e:
        log_message(f"Could not run LOOP 9 - combine DSC: {e}")
      
        
