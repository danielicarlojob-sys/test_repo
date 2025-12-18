import os
import pandas as pd
from src.utils.log_file import LOG_FILE, log_message,debug_info 
from src.utils.load_data import load_temp_data, load_csv_to_df

def df_merger(
    df: pd.DataFrame, 
    flight_phase: str = None,
    n_pts: int = 1250, 
    DebugOption: int = 1, 
    data_str: str = 'data_output_'
    ) -> pd.DataFrame:
    """
    Merge historical data stored in CSV format (filename is indicated by CSV_str), with result from current data query

    Parameters
    ----------
    Args:
         - df: pd.DataFrame, DataFrame containing data from curren SQL query
         - flight_phase: str = None, indicated flight phase 
         - n_pts : int, optional (default=1250) Maximum number of most recent data points to keep for each ESN.
         - DebugOption: int = 1, option to create ad save a copy of the data in CSV format 
         - data_str: str = 'data_output_', substring used to identify the historical data from previous run

    Return
    ------
         - df: pd.DataFrame, DataFrame containing merged data 
    """
    df_out = df.copy()
    # Get current dir and Fleetstore_Data dir
    current_dir = os.getcwd()
    FleetStore_dir = os.path.join(current_dir,"Fleetstore_Data")
  
    # Get previous DataFrame data from CSV file in Fleetstore_Data dir

    CSV_str = [file for file in os.listdir(FleetStore_dir) if data_str.lower() in file.lower() and flight_phase.lower() in file.lower()]
    if CSV_str:
        # If CSV data from previos run is found load it and Merge it with current output form SQL query
        df_previous = load_csv_to_df(CSV_str[0])
        
        # Ensure both Dataframes have 'reportdatetime' in datetime format
        df_previous['reportdatetime'] = pd.to_datetime(df_previous['reportdatetime'], format='%Y-%m-%d %H:%M:%S').dt.floor('s')
        df_out['reportdatetime'] = pd.to_datetime(df_out['reportdatetime'], format='%Y-%m-%d %H:%M:%S').dt.floor('s')
        
        # Concatenete computed deltas with previous run data
        concateneted_df = pd.concat([df_previous, df_out], ignore_index=True)
        concateneted_df = concateneted_df.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        
    else:
        concateneted_df = df_out

    try:
        # Drop exact duplicates on key columns
        columns_to_check_for_NaN_or_duplicates = [
            'ESN', 'reportdatetime', 'operator', 'equipmentid', 'ACID', 'ENGPOS', 
            'P25__PSI', 'T25__DEGC', 'P30__PSI', 'T30__DEGC', 'TGTU_A__DEGC', 'NL__PC', 'NI__PC', 'NH__PC', 'FF__LBHR', 'PS160__PSI',
            'PS26S__NOM_PSI', 'TS25S__NOM_K', 'PS30S__NOM_PSI', 'TS30S__NOM_K', 'TGTS__NOM_K', 'NL__NOM_PC', 'NI__NOM_PC', 'NH__NOM_PC', 'FF__NOM_LBHR', 'P135S__NOM_PSI'
        ]

        # Drop rows where any value in columns_to_check_for_NaN_or_duplicates column is invalid
        concateneted_df = concateneted_df.dropna(subset=columns_to_check_for_NaN_or_duplicates).reset_index(drop=True)
        concateneted_df = concateneted_df.drop_duplicates(subset=columns_to_check_for_NaN_or_duplicates).reset_index(drop=True)


        filtered_list_merged = []
        ESNs_unique_list = concateneted_df['ESN'].unique()

        for esn_value in ESNs_unique_list:
            esn_df = concateneted_df[concateneted_df['ESN'] == esn_value]
            esn_df_sorted = esn_df.sort_values(by='reportdatetime', ascending=False)
            top_n = esn_df_sorted.head(n_pts) # keeps onpy top n_pts from esn_df_sorted
            filtered_list_merged.append(top_n) # appeneds top_n DataFrame to filtered_list_merged

        # Final output
        df_merged = pd.concat(filtered_list_merged, ignore_index=True)
        df_merged = df_merged.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        
        # Saves function output to CSV file 
        if DebugOption == 1:
            # Save a temporary CSV file for debugging or traceability
            path_temp = os.path.join(FleetStore_dir, f"Merged_data_{flight_phase}.csv")
            log_message(f"        Merged data saved to: {path_temp}")
            df_merged.to_csv(path_temp)
        
        return df_merged

    except Exception as e:
        
        log_message(f"error merging data: {e}")