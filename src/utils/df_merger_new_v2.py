import os
import pandas as pd
from src.utils.log_file import LOG_FILE, log_message, debug_info, f_lineno as line
from src.utils.read_and_clean_v1 import read_and_clean_csv


def df_merger_new(
    df: pd.DataFrame,
    flight_phase: str = None,
    n_pts: int = 650,
    DebugOption: int = 0,
    data_str: str = 'data_output_'
) -> pd.DataFrame:
    """
    Merge historical data stored in CSV format (filename is indicated by CSV_str),
    with result from current data query.

    Parameters
    ----------
    Args:
         - df: pd.DataFrame, DataFrame containing data from current SQL query
         - flight_phase: str = None, indicated flight phase
         - n_pts : int, optional (default = 650) Maximum number of most recent
           unique reportdatetime points to keep for each ACID+ESN+ENGPOS combination.
         - DebugOption: int = 1, option to create and save a copy of the data in CSV format
         - data_str: str = 'data_output_', substring used to identify the historical
           data from previous run

    Return
    ------
         - df: pd.DataFrame, DataFrame containing merged and trimmed data
    """
    # Create a copy of the input df to prevent warnings
    df_out = df.copy()

    # Set the proper columns type for each one in df_out (query output)
    # ATTENTION - changed "object" to "string" it might be problematic
    query_columns = df_out.columns
    
    cols_list_int64 = ['ESN', 'equipmentid', 'ENGPOS', 'DSCID']
    cols_list_datetime = ['reportdatetime', 'datestored']
    cols_list_object = ['operator', 'ACID']
    cols_list_float = [col for col in query_columns if col not in cols_list_int64+cols_list_datetime+cols_list_object ]
    
    df_out[cols_list_int64] = df_out[cols_list_int64].astype('int64')
    df_out[cols_list_datetime] = df_out[cols_list_datetime].astype('datetime64[ns]')
    df_out[cols_list_object] = df_out[cols_list_object].astype('string')
    df_out[cols_list_float] = df_out[cols_list_float].astype('Float64')
    
    # Round the appropriate float columns to 5 decimal places
    df_out[cols_list_float] = df_out[cols_list_float].round(5)
    # Search for flight phase specific csv data from previous run
    current_dir = os.getcwd()
    FleetStore_dir = os.path.join(current_dir, "Fleetstore_Data")
    FleetStore_files_list = os.listdir(FleetStore_dir)
    CSV_str = [
        file for file in FleetStore_files_list
        if data_str.lower() in file.lower()
        and flight_phase.lower() in file.lower()
    ]
    # If flight phase specific csv data from previous run is found loads nad merge data with df_out
    if CSV_str:
        log_message(f"{debug_info()}  previous {flight_phase} data file found")

        file_path = os.path.join(FleetStore_dir, CSV_str[0])
        df_previous = read_and_clean_csv(file_path)
        concatenated_df = pd.concat([df_previous, df_out], ignore_index=True)
        
        
    else:
        log_message(f"{debug_info()}  NO previous data file found")
        concatenated_df = df_out
        
    try:
        # Remove rows with NaNs in query_columns
        concatenated_df = concatenated_df.dropna(
            subset=query_columns)

        # Here is important to use keep='first', this will keep the first row 
        # with matching values for the cols subset, meaning the row from df_previous
        concatenated_df = concatenated_df.sort_values(
            by='reportdatetime', ascending=False)

        
        # SUBROUTINE to concatenate top n_pts by ESN
        # Keep only the top n_pts rows per ESN
        df_merged = (
            concatenated_df
            .sort_values(by="reportdatetime", ascending=False)
            .groupby("ESN", group_keys=False)
            .head(n_pts)
            .reset_index(drop=True)
        )

        # Concatenate once at the end
        df_merged = df_merged.sort_values(  by='reportdatetime',
                                            ascending=False).reset_index(drop=True)
        
        if DebugOption == 1:
            path_temp = os.path.join(
                FleetStore_dir, f"Merged_data_{flight_phase}.csv")
            log_message(f"{debug_info()}  Merged data saved to: {path_temp}")
            df_merged.to_csv(path_temp, index=False)

        return df_merged
    except Exception as e:
        log_message(f"error merging data: {e}")
        return df
