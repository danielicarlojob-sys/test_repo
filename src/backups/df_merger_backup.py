import os
import pandas as pd
from src.utils.log_file import LOG_FILE, log_message
from src.utils.load_data import load_temp_data, load_csv_to_df

def df_merger(
    df: pd.DataFrame, 
    flight_phase: str = None,
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
         - concateneted_df: pd.DataFrame, DataFrame containing merged data 
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
        df_previous['reportdatetime'] = pd.to_datetime(df_previous['reportdatetime'], format='%Y-%m-%d %H:%M:%S')
        df_out['reportdatetime'] = pd.to_datetime(df_out['reportdatetime'], format='%Y-%m-%d %H:%M:%S')
        
        # Concatenete computed deltas with previous run data
        concateneted_df = pd.concat([df_previous, df_out], ignore_index=True)
        concateneted_df = concateneted_df.drop_duplicates(subset=['ESN', 'reportdatetime', 'ACID', 'ENGPOS'])
        concateneted_df = concateneted_df.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        
    else:
        concateneted_df = df_out

        return concateneted_df