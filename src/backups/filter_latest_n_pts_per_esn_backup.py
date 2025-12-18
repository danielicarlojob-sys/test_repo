import numpy as np
import pandas as pd
import os
from backups.read_and_clean import read_and_clean_csv, read_and_clean_df
from src.utils.import_data_filters import drop_nans, filter_parameters
from src.utils.days_difference import days_difference
from src.utils.log_file import LOG_FILE, log_message




def filter_latest_n_pts_per_esn(
    df: pd.DataFrame, 
    csv_path: str, 
    flight_phase: str, 
    timestamp_str_initial: str, 
    n_pts: int = 1250,
    DebugOption: int = 1
    ) -> pd.DataFrame:
    """
    For each unique ESN, keeps only the most recent n_pts rows based on 'reportdatetime'.
    Returns a single DataFrame with all such rows, sorted by 'reportdatetime' descending.
    
    Parameters
    ----------
    Args:
        - df (pd.DataFrame): Input dataframe.
        - csv_path (str): input string containing path to CSV file.
        - flight_phase (str): input string containing flight phase.
        - timestamp_str_initial (str): timestamp.
        - n_pts (int): datapoint's cutoff (new to old), any point older that n_pts will be removed.
        - DebugOption: int = 1, option to create ad save a copy of the data in CSV format.
    
    Returns
    -------
        - result_df (pd.DataFrame): DataFrame with filtered data

    """
    # Set df columns types 
    df = read_and_clean_df(df)
    log_message(f"filter_latest_n_pts_per_esn - line 41 - df.shape:\n current SQL run DataFrame shape {df.shape}")
    # If the file exists, merge with existing data
    if os.path.exists(csv_path):
        # existing_data = pd.read_csv(csv_path, index_col=0)
        existing_data = read_and_clean_csv(csv_path)
        log_message(f"filter_latest_n_pts_per_esn - line 46 - existing_data.shape:\n historical DataFrame shape {existing_data.shape}")

        # Align columns
        columns = ['ESN', 'reportdatetime', 'datestored', 'operator', 'equipmentid',
       'ACID', 'ENGPOS', 'DSCID', 'P25__PSI', 'T25__DEGC', 'P30__PSI',
       'T30__DEGC', 'TGTU_A__DEGC', 'NL__PC', 'NI__PC', 'NH__PC', 'FF__LBHR',
       'PS160__PSI', 'PS26S__NOM_PSI', 'TS25S__NOM_K', 'PS30S__NOM_PSI',
       'TS30S__NOM_K', 'TGTS__NOM_K', 'NL__NOM_PC', 'NI__NOM_PC', 'NH__NOM_PC',
       'FF__NOM_LBHR', 'P135S__NOM_PSI', 'ALT__FT', 'MN1', 'P20__PSI',
       'T20__DEGC']
        df = df[columns]
        log_message(f"filter_latest_n_pts_per_esn - line 57 - df.shape:\n POST COLUMNS SORT current SQL run DataFrame shape {df.shape}")        # Combine old and new data
        combined = pd.concat([existing_data, df], ignore_index=True)
        log_message(f"filter_latest_n_pts_per_esn - line 59 - combined.shape:\n combined DataFrame shape {df.shape}")

        # Remove exact duplicates
        columns_to_check_for_duplicates = [
            'reportdatetime',
            'datestored',
            'operator',
            'equipmentid',
            'ACID',
            'ENGPOS',
            'DSCID',
            'P25__PSI',
            'T25__DEGC',
            'P30__PSI',
            'T30__DEGC',
            'TGTU_A__DEGC',
            'NL__PC',
            'NI__PC',
            'NH__PC',
            'FF__LBHR',
            'PS160__PSI'
        ]

        combined = combined.drop_duplicates(subset=columns_to_check_for_duplicates)

        # Container for the filtered rows
        filtered_list = []

        # Loop over each unique ESN
        for esn_value in combined['ESN'].unique():
            esn_df = combined[combined['ESN'] == esn_value]
            esn_df = esn_df.drop_duplicates(subset=columns_to_check_for_duplicates)
            esn_df_sorted = esn_df.sort_values(by='reportdatetime', ascending=False) # Most recent first
            top_n = esn_df_sorted.head(n_pts)
            filtered_list.append(top_n)

        # Combine all filtered subsets into one DataFrame
        result_df = pd.concat(filtered_list, ignore_index=True)

        # Sort the final result by reportdatetime (you can switch ascending to True if needed)
        result_df = result_df.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        result_df = drop_nans(result_df)
        result_df = filter_parameters(result_df, flight_phase)   
        result_df['TS25S__NOM_K'] = result_df['TS25S__NOM_K']-273.15
        result_df['TS30S__NOM_K'] = result_df['TS30S__NOM_K']-273.15
        result_df['TGTS__NOM_K'] = result_df['TGTS__NOM_K']-273.15
        result_df = days_difference(result_df) 

        # Convert timestamp to datetime
        timestamp_dt = pd.to_datetime(timestamp_str_initial, format='%Y-%m-%d %H:%M:%S')

        # Create the NEW_FLAG column based on the condition
        result_df["NEW_FLAG"] = (result_df["reportdatetime"] >= timestamp_dt).astype(int)

        if DebugOption == 1:
            result_df.to_csv(csv_path)
            log_message(f"        filtered data saved to: {csv_path}")
        return result_df
    else:
        # File does not exist yet — save full df
        log_message(f"File '{csv_path}' does not exist. Creating new file.")

        df['reportdatetime'] = pd.to_datetime(df['reportdatetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Container for the filtered rows
        filtered_list = []

        # Loop over each unique ESN
        for esn_value in df['ESN'].unique():
            esn_df = df[df['ESN'] == esn_value]
            esn_df = esn_df.drop_duplicates()
            esn_df_sorted = esn_df.sort_values(by='reportdatetime', ascending=False) # Most recent first
            top_n = esn_df_sorted.head(n_pts)
            filtered_list.append(top_n)

        # Combine all filtered subsets into one DataFrame
        result_df = pd.concat(filtered_list, ignore_index=True)

        # Sort the final result by reportdatetime (you can switch ascending to True if needed)
        result_df = result_df.sort_values(by='reportdatetime', ascending=False).reset_index(drop=True)
        result_df = drop_nans(result_df)
        result_df = filter_parameters(result_df, flight_phase)
        result_df['TS25S__NOM_K'] = result_df['TS25S__NOM_K']-273.15
        result_df['TS30S__NOM_K'] = result_df['TS30S__NOM_K']-273.15
        result_df['TGTS__NOM_K'] = result_df['TGTS__NOM_K']-273.15
        result_df = days_difference(result_df)
        
        # Convert timestamp to datetime
        timestamp_dt = pd.to_datetime(timestamp_str_initial, format='%Y-%m-%d %H:%M:%S')

        # Create the NEW_FLAG column based on the condition
        result_df["NEW_FLAG"] = (result_df["reportdatetime"] >= timestamp_dt).astype(int)

        # Add remaining columns
        columns_to_add = [
            'PS26__DEL_PC',
            'T25__DEL_PC',
            'P30__DEL_PC',
            'T30__DEL_PC',
            'TGTU__DEL_PC',
            'NL__DEL_PC',
            'NI__DEL_PC',
            'NH__DEL_PC',
            'FF__DEL_PC',
            'P160__DEL_PC',
            'SISTER_ESN', #int
            'PS26__DEL_PC_E2E',
            'T25__DEL_PC_E2E',
            'P30__DEL_PC_E2E',
            'T30__DEL_PC_E2E',
            'TGTU__DEL_PC_E2E',
            'NL__DEL_PC_E2E',
            'NI__DEL_PC_E2E',
            'NH__DEL_PC_E2E',
            'FF__DEL_PC_E2E',
            'P160__DEL_PC_E2E',
            'FlagSV', #int
            'FlagSisChg', #int
            'PS26__DEL_PC_E2E_MAV_NO_STEPS',
            'T25__DEL_PC_E2E_MAV_NO_STEPS',
            'P30__DEL_PC_E2E_MAV_NO_STEPS',
            'T30__DEL_PC_E2E_MAV_NO_STEPS',
            'TGTU__DEL_PC_E2E_MAV_NO_STEPS',
            'NL__DEL_PC_E2E_MAV_NO_STEPS',
            'NI__DEL_PC_E2E_MAV_NO_STEPS',
            'NH__DEL_PC_E2E_MAV_NO_STEPS',
            'FF__DEL_PC_E2E_MAV_NO_STEPS',
            'P160__DEL_PC_E2E_MAV_NO_STEPS',
            'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_50',
            'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_100',
            'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_200',
            'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_400',
            'P160__DEL_PC_E2E_MAV_NO_STEPS_LAG_400'
            ] 
        for col in columns_to_add:
            if 'DEL' in col:
                result_df[col]= np.nan
            else:
                result_df[col]= 0
        0# Saves function output to CSV file 
        if DebugOption == 1:
            result_df.to_csv(csv_path)
            log_message(f"        filtered data saved to: {csv_path}")
        return result_df
