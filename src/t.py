import os
import pandas as pd
import numpy as np
from src.utils.print_time_now import print_time_now
from tqdm import tqdm
from typing import Dict, Tuple
from src.utils.log_file import log_message, f_lineno as line

"""
                    # Filter df_esn_temp to include only rows with signature = f"{comp} ETA"
                    xrate_identifier_columns = [f"VAR1_IDENTIFIER{Lag}",f"VAR2_IDENTIFIER{Lag}",f"VAR3_IDENTIFIER{Lag}"]
                    df_esn_temp_comp = df_esn_temp[df_esn_temp == ]
"""

def merge_flight_phases(df_takeoff: pd.DataFrame,
                        df_climb: pd.DataFrame,
                        df_cruise: pd.DataFrame) -> pd.DataFrame:

    df_takeoff = df_takeoff.copy()
    df_climb = df_climb.copy()
    df_cruise = df_cruise.copy()
    
    # Group by the keys of interest
    keys_to_group = ["ESN", "operator", "ACID", "ENGPOS"]
    grouped_takeoff = df_takeoff.groupby(keys_to_group, group_keys=False)
    grouped_climb = df_climb.groupby(keys_to_group, group_keys=False)
    grouped_cruise = df_cruise.groupby(keys_to_group, group_keys=False)

    # Initialize empty Dataframe

    indexes_tko = df_takeoff.index
    columns_merged = keys_to_group + ["reportdatetime_takeoff",
    "reportdatetime_climb",
    "reportdatetime_cruise",
    "row_sum_takeoff",
    "row_sum_climb",
    "row_sum_cruise"]
    df_merged = pd.DataFrame(index=list(indexes_tko), columns=columns_merged)
    i = 0
    for key, group_tko in grouped_takeoff:
        # key is a tuple, e.g. (12345, "Lufthansa", "D-AIXA", 1)
        # group_tko is the DataFrame for that group
        # Sort group by datetime to ensure chronological order
        group_tko = group_tko.copy()
        group_tko = group_tko.sort_values("reportdatetime").copy()

        # Check if groups with the same key are present is Climb and Cruise
        # If so extract said groups and sorte them by date
        if key in grouped_climb.groups:
            group_climb = grouped_climb.get_group(key).sort_values("reportdatetime").copy()
            
        if key in grouped_cruise.groups:

            group_cruise = grouped_cruise.get_group(key).sort_values("reportdatetime").copy()

        # Loop through each row in group_tko
        for idx, rdt in enumerate(group_tko['reportdatetime']):
            takeoff_timestamp = rdt
            idx_tko_temp= group_tko.index.tolist()
           
            # Assing key group values to i-th row in df_merged along with take-off's timestamp and take-off's row_sum
            
            df_merged.loc[i,keys_to_group] = group_tko.loc[idx_tko_temp[idx], keys_to_group]
            takeoff_cols = ["reportdatetime_takeoff","row_sum_takeoff"]
            temp = group_tko.loc[idx_tko_temp[idx], ["reportdatetime","row_sum"]]
            df_merged.loc[i,takeoff_cols] = temp.values


            if group_climb:
                log_message(f"line {line()} ---> group_climb:{group_climb}")

                climb_timestamp_temp = (group_climb.loc[group_climb["reportdatetime"] > takeoff_timestamp,
                                                        "reportdatetime"].min())
                log_message(f"line {line()} ---> climb_timestamp_temp:{climb_timestamp_temp}")
                if (climb_timestamp_temp - takeoff_timestamp) < pd.Timedelta(hours=1):
                    
                    climb_timestamp = climb_timestamp_temp
                    matching_idx = group_climb.loc[group_climb['reportdatetime'] == climb_timestamp].index.tolist()
                    df_merged.loc[i,["reportdatetime_climb","row_sum_climb"]] = group_climb.loc[matching_idx, ["reportdatetime","row_sum"]]
            
            if group_cruise:
                cruise_timestamp_temp = group_cruise[group_cruise["reportdatetime"]>takeoff_timestamp].min()
                if (cruise_timestamp_temp - takeoff_timestamp) < pd.Timedelta(hours=1):
                    
                    cruise_timestamp = cruise_timestamp_temp
                    matching_idx = group_cruise.loc[group_cruise['reportdatetime'] == cruise_timestamp].index.tolist()
                    df_merged.loc[i,["reportdatetime_cruise","row_sum_cruise"]] = group_cruise.loc[matching_idx, ["reportdatetime","row_sum"]]
 
            i += 1
    return df_merged


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

     # save guard for function return in case merge_flight_phases fails
    df_merged = pd.DataFrame()
    
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

Lim_dict: dict = {  'EtaThresh': [0, 0.2, 0.4, 0.6, 0.8, 1.0],'RelErrThresh': [0.3],'lim': 0.07,'nEtaThresh': 6,'nRelErrThresh': 1,'num': 3}
