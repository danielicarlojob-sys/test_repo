import os
import pandas as pd
from src.utils.log_file import LOG_FILE, log_message, debug_info, f_lineno as line
from backups.read_and_clean import read_and_clean_csv


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
    df_out = df.copy()
    cols_list_int64 = ['ESN', 'equipmentid', 'ENGPOS', 'DSCID']
    cols_list_datetime = ['reportdatetime', 'datestored']
    cols_list_object = ['operator', 'ACID']
    cols_list_float = [col for col in df_out.columns if col not in cols_list_int64+cols_list_datetime+cols_list_object ]
    df_out['ESN'] = df_out['ESN'].astype('int64')
    current_dir = os.getcwd()
    FleetStore_dir = os.path.join(current_dir, "Fleetstore_Data")
    FleetStore_files_list = os.listdir(FleetStore_dir)
    CSV_str = [
        file for file in FleetStore_files_list
        if data_str.lower() in file.lower()
        and flight_phase.lower() in file.lower()
    ]
    if CSV_str:
        log_message(f"{debug_info()}  previous {flight_phase} data file found")

        file_path = os.path.join(FleetStore_dir, CSV_str[0])
        df_previous = read_and_clean_csv(file_path)
        concatenated_df = pd.concat([df_previous, df_out], ignore_index=True)

        log_message(f"{debug_info()}  previous {flight_phase} data concatened with new data")

        
    else:
        log_message(f"{debug_info()}  NO previous data file found")
        concatenated_df = df_out
        
    try:
        columns_to_check_for_NaN = [
            'ESN',
            'reportdatetime',
            'operator',
            'equipmentid',
            'ACID',
            'ENGPOS',
            'P25__PSI',
            'T25__DEGC',
            'P30__PSI',
            'T30__DEGC',
            'TGTU_A__DEGC',
            'NL__PC',
            'NI__PC',
            'NH__PC',
            'FF__LBHR',
            'PS160__PSI',
            'PS26S__NOM_PSI',
            'TS25S__NOM_K',
            'PS30S__NOM_PSI',
            'TS30S__NOM_K',
            'TGTS__NOM_K',
            'NL__NOM_PC',
            'NI__NOM_PC',
            'NH__NOM_PC',
            'FF__NOM_LBHR',
            'P135S__NOM_PSI',
            'ALT__FT',
            'MN1',
            'P20__PSI',
            'T20__DEGC']
        concatenated_df = concatenated_df.dropna(
            subset=columns_to_check_for_NaN)
        concatenated_df = concatenated_df.sort_values(
            by='reportdatetime', ascending=False)
        concatenated_df = concatenated_df.drop_duplicates(
            subset=['ACID', 'ESN', 'ENGPOS', 'reportdatetime'], keep='first'
        )
        
        # SUBROUTINE to concatenate top n_pts by ESN
        col = list(concatenated_df.columns)
        df_merged = pd.DataFrame(columns=col)
        filtered_list_merged = []
        unique_ESNs = concatenated_df['ESN'].unique()
        rows = 0

        # Loop
        for esn in unique_ESNs:
            df_temp = concatenated_df[concatenated_df['ESN'] == esn].copy()
            df_temp = df_temp.sort_values(
                by='reportdatetime',
                ascending=False).reset_index(
                drop=True)
            top_n = df_temp.head(n_pts)
            rows += top_n.shape[0]
            if len(top_n) > n_pts:
                log_message(f"{debug_info()}  top_n.shape:\n {top_n.shape}")
            filtered_list_merged.append(top_n)

        # Concatenate once at the end
        df_merged = pd.concat(filtered_list_merged, ignore_index=True)
        df_merged = df_merged.sort_values(
            by='reportdatetime',
            ascending=False).reset_index(
            drop=True)
        
        if DebugOption == 1:
            path_temp = os.path.join(
                FleetStore_dir, f"Merged_data_{flight_phase}.csv")
            log_message(f"{debug_info()}  Merged data saved to: {path_temp}")
            df_merged.to_csv(path_temp, index=False)

        return df_merged
    except Exception as e:
        log_message(f"error merging data: {e}")
        return df
