import os
import asyncio
import pandas as pd
import numpy as np

from src.utils.print_time_now import print_time_now
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message, f_lineno as line
from src.utils.merge_flight_phases_v1 import merge_flight_phases, merged_data_evaluation




def Loop_9_combine_DSC(
        data_dict: dict,
        Lim_dict: dict = {  'EtaThresh': [0.2, 0.4, 0.6, 0.8, 1.0],
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
    # Define function's dysplay name
    Loop_9_combine_DSC.display_name = "LOOP 9 - Combine DSC"  
    # extract data from Lim_dict
    Limit = Lim_dict['lim']
    Num = Lim_dict['num']
    flight_phases = data_dict.keys()

    for flight_phase in tqdm(flight_phases, desc=" LOOP 9 ", unit="Flight Phase"):

        df = data_dict[flight_phase].copy()
        
        # Preventive sort old to new data and reset df index
        df = df.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)

        # Split into two groups:
        df_new = df[df['NEW_FLAG'] == 1].copy()
        df_old = df[df['NEW_FLAG'] == 0]
        if not df_new.empty:
            # define the Dataframe columns subset and extract them
            cols = list(df_new.columns)
            # columns containing the substring "FRACTION_GT"
            cols_subset = [col for col in cols if "FRACTION_GT" in col]
            df_new_copy = df_new.copy()
            # Sum times values in cols_subest > Limit row-wise, skipping NaNs
            df_new_copy['row_sum'] = (df_new_copy[cols_subset] >= Limit).sum(axis=1, skipna=True)

            # Merge updated new rows with old rows and restore original row order
            df_final = pd.concat([df_old, df_new_copy]).sort_values(
                    by='reportdatetime',
                    ascending=True).reset_index(
                    drop=True)

            data_dict[flight_phase] = df_final
        else:
            df_final = df_old
            data_dict[flight_phase] = df_final

        # Remove duplicates
        df_final = df_final.sort_values(
            by='reportdatetime',
            ascending=True).drop_duplicates(keep='last')

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

     # save guard for function return in case merge_flight_phases fails
    df_merged = pd.DataFrame()
    
    try:
        df_merged = merge_flight_phases(df_takeoff = dict_temp['take-off'],
                                        df_climb = dict_temp['climb'],
                                        df_cruise = dict_temp['cruise'])
        df_merged, df_DN = merged_data_evaluation(df_merged, Num) 

        # Remove duplicates
        df_merged = df_merged.sort_values(
            by='reportdatetime_takeoff',
            ascending=True).drop_duplicates(keep='last')
    
        # (Optional) save out
        if save_csv:
            path_out_df_merged = os.path.join(os.getcwd(), "Fleetstore_Data",
                                    f"{Loop_9_combine_DSC.__name__}_merged_output.csv")
            df_merged.to_csv(path_out_df_merged, index=False)

            path_out_df_DN = os.path.join(os.getcwd(), "Fleetstore_Data",
                                    f"{Loop_9_combine_DSC.__name__}_DN_output.csv")
            df_DN.to_csv(path_out_df_DN, index=False)
            
            log_message(f"File saved to: {os.path.join(os.getcwd(), 'Fleetstore_Data')}")
    except Exception as e:
        log_message(f"Could not run {merge_flight_phases.__name__}: {e}")
    return data_dict, df_merged

# ==============================
# Script entry point for testing
# ==============================

if __name__ == "__main__":
    func = Loop_9_combine_DSC
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
            f"Start {func.__name__} at {str(print_time_now())}")

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
            f"Completed  {func.__name__} at {str(print_time_now())}")
        ##########################################################################
        # Data output save
        ##########################################################################
        for flight_phase in data_dict.keys():
            path_output_data = os.path.join(data_folder, f"data_output_{flight_phase}.csv")
            data_dict[flight_phase].to_csv(path_output_data, index=False)
            log_message(f"{flight_phase.capitalize()} data saved to: {path_output_data}")
    except Exception as e:
        log_message(f"Could not run  {func.__name__}: {e}")
