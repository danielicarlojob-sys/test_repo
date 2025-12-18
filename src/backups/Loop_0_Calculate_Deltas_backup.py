import os
import pandas as pd
from src.utils.log_file import LOG_FILE, log_message
from src.utils.load_data import load_temp_data, load_csv_to_df



def Loop_0_delta_calc(df: pd.DataFrame, flight_phase: str = None, DebugOption: int = 1, n_pts: int = 1250, data_str: str = 'data_output_') -> pd.DataFrame:
    """
    Function computes the percentage deltas of actual engine parameters from their nominal
    (baseline) values. It also filters the dataset to return the latest `n_pts` points per ESN,
    removing duplicates.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing the following columns:
        - 'ESN', 'reportdatetime', 'ACID', 'ENGPOS'
        - Sensor readings (e.g., 'P25__PSI', 'T25__DEGC', etc.)
        - Corresponding nominal values (e.g., 'PS26S__NOM_PSI', 'TS25S__NOM_K', etc.)
        - Other metadata: 'datestored', 'operator', 'equipmentid', 'DSCID'

    n_pts : int, optional (default=1250)
        Maximum number of most recent data points to keep for each ESN.

    data_str : str, optional (default='data_')
        string of text common to the csv files from previous run.

    Returns
    -------
    df_merged : pd.DataFrame
        The merged and filtered DataFrame, containing original and delta columns.
  
    """
    # Compute deltas
    df_out = df.copy()
    """    
    df_out = pd.DataFrame()
    df_out['ESN'] = df['ESN']
    df_out['reportdatetime'] = df['reportdatetime']
    df_out['ACID'] = df['ACID']
    df_out['ENGPOS'] = df['ENGPOS']
    """

    df_out['PS26__DEL_PC'] = round((df['P25__PSI'] - df['PS26S__NOM_PSI']) * 100 / df['PS26S__NOM_PSI'], 5)
    df_out['T25__DEL_PC'] = round((df['T25__DEGC'] - df['TS25S__NOM_K']) * 100 / df['TS25S__NOM_K'], 5)
    df_out['P30__DEL_PC'] = round((df['P30__PSI'] - df['PS30S__NOM_PSI']) * 100 / df['PS30S__NOM_PSI'], 5)
    df_out['T30__DEL_PC'] = round((df['T30__DEGC'] - df['TS30S__NOM_K']) * 100 / df['TS30S__NOM_K'], 5)
    df_out['TGTU__DEL_PC'] = round((df['TGTU_A__DEGC'] - df['TGTS__NOM_K']) * 100 / df['TGTS__NOM_K'], 5)
    df_out['NL__DEL_PC'] = round((df['NL__PC'] - df['NL__NOM_PC']) * 100 / df['NL__NOM_PC'], 5)
    df_out['NI__DEL_PC'] = round((df['NI__PC'] - df['NI__NOM_PC']) * 100 / df['NI__NOM_PC'], 5)
    df_out['NH__DEL_PC'] = round((df['NH__PC'] - df['NH__NOM_PC']) * 100 / df['NH__NOM_PC'], 5)
    df_out['FF__DEL_PC'] = round((df['FF__LBHR'] - df['FF__NOM_LBHR']) * 100 / df['FF__NOM_LBHR'], 5)
    df_out['P160__DEL_PC'] = round((df['PS160__PSI'] - df['P135S__NOM_PSI']) * 100 / df['P135S__NOM_PSI'], 5)

    # Get current dir and Fleetstore_Data dir
    current_dir = os.getcwd()

    FleetStore_dir = os.path.join(current_dir,"Fleetstore_Data")
  

    # Get previous DataFrame data from CSV file in Fleetstore_Data dir

    CSV_str = [file for file in os.listdir(FleetStore_dir) if data_str.lower() in file.lower() and flight_phase.lower() in file.lower()]
    if CSV_str:
        # If CSV data from previos run is found load it and Merge it with current output form SQL query
        df_previous = load_csv_to_df(CSV_str[0])
        
        # Ensure both Dataframes have 'reportdatetime' in datetime format
        df_previous['reportdatetime'] = pd.to_datetime(df_previous['reportdatetime'], format='%Y-%m-%d %H:%M:%S')
        df_out['reportdatetime'] = pd.to_datetime(df_out['reportdatetime'], format='%Y-%m-%d %H:%M:%S')
        
        # Concatenete computed deltas with previous run data
        concateneted_df = pd.concat([df_previous, df_out], ignore_index=True)
        concateneted_df = concateneted_df.drop_duplicates(subset=['ESN', 'reportdatetime', 'ACID', 'ENGPOS'])
        concateneted_df = concateneted_df.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        
    else:
        concateneted_df = df_out
        
    try:
        # Drop exact duplicates on key columns
        columns_to_check_for_duplicates = [
            'reportdatetime', 'datestored', 'operator', 'equipmentid', 'ACID', 'ENGPOS', 'DSCID',
            'P25__PSI', 'T25__DEGC', 'P30__PSI', 'T30__DEGC', 'TGTU_A__DEGC',
            'NL__PC', 'NI__PC', 'NH__PC', 'FF__LBHR', 'PS160__PSI'
        ]

        filtered_list_merged = []

        for esn_value in concateneted_df['ESN'].unique():
            esn_df = concateneted_df[concateneted_df['ESN'] == esn_value]
            esn_df = esn_df.drop_duplicates(subset=columns_to_check_for_duplicates)
            esn_df_sorted = esn_df.sort_values(by='reportdatetime', ascending=False)
            top_n = esn_df_sorted.head(n_pts) # keeps onpy top n_pts from esn_df_sorted
            filtered_list_merged.append(top_n) # appeneds top_n DataFrame to filtered_list_merged

        # Final output
        df_merged = pd.concat(filtered_list_merged, ignore_index=True)
        df_merged = df_merged.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)

    except Exception as e:
        
        log_message(f"error merging data: {e}")

    # Saves function output to CSV file 
    if DebugOption == 1:
        # Save a temporary CSV file for debugging or traceability
        path_temp = os.path.join(FleetStore_dir, f"LOOP_0_{flight_phase}.csv")
        log_message(f"        File saved to: {path_temp}")
        df_merged.to_csv(path_temp)

    return df_merged

import asyncio
from backups.async_main import main as async_main
from backups.data_queries import *
from src.utils.print_time_now import print_time_now
from src.utils.log_file import LOG_FILE, log_message



if __name__  == "__main__":

    root_dir = os.getcwd()
    Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')
try:
    data_dict = fetch_all_flight_phase_data(root_dir)
    log_message("Manual run - fetch_all_flight_phase_data completed!")

    flight_phase = 'cruise'
    #log_message(f"Manual run - line 132 - (data_dict.items():{data_dict.items()}")
    #log_message(f"Manual run - line 133 - flight_phase: {flight_phase}")
    # LOOP 0 - DELTA CALCULATION
    #log_message("Manual run - line135 - Start data processing")
    log_message(f"Manual run - line136  - Start LOOP 0  - DELTA CALCULATION")
    #log_message(f"Manual run - line137  - (data_dict[{flight_phase}]:{data_dict[flight_phase]}")
    for fp in list(data_dict.keys()):
        df_temp = Loop_0_delta_calc(data_dict[fp], fp)
    # asyncio.run(async_main(data_dict, Fleetstore_data_dir,Loop_0_delta_calc))

    log_message(f"Manual run - Completed LOOP 0 - DELTA CALCULATION")

except Exception as e:
    
    log_message(f"Manual run - line 146 - error fetching data: {e}")
