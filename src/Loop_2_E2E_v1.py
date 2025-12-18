from src.utils.load_data import load_temp_data as ltd
import pandas as pd
import os
from src.utils.enforce_dtypes import enforce_dtypes
from src.utils.log_file import log_message
from tqdm import tqdm

def Loop_2_E2E(
        df: pd.DataFrame,
        flight_phase: str = None,
        DebugOption: int = 1) -> pd.DataFrame:
    """
    For each (ACID, reportdatetime) combination in the input DataFrame that has
    exactly two engines (ESNs) reporting, calculate the engine-to-engine (E2E)
    delta for selected parameters.

    Parameters:
    ----------
     - df : pd.DataFrame
        Input DataFrame containing columns 'ACID', 'reportdatetime', 'ESN', and
        a set of parameter columns suffixed with '__DEL_PC'.
     - flight_phase: str = None, string referring to the current flight phase.
     - DebugOption : int, optional (default = 1), switch to create a copy of the output data in csv format.


    Returns:
    -------
     - pd.DataFrame
        A new DataFrame containing all original data plus:
            - 'SISTER_ESN': the ESN of the "sister" engine
            - '[PARAM]_E2E': the difference between the engine's value and its sister's
    """
    # Define function's dysplay name
    Loop_2_E2E.display_name = "LOOP 2  - E2E CALCULATION"
    
    # Create df copy to avoid warnings
    df_temp = df.copy()
    
    # Preventive sort old to new data and reset df index
    df_temp = df_temp.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)

    # Split the dataframe between old and new data
    df_new = df_temp[df_temp['NEW_FLAG']==1].copy()
    df_old = df_temp[df_temp['NEW_FLAG']==0]
    dtypes_list = df_old.dtypes
    # List of parameter columns to calculate E2E deltas for
    input_columns = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC',
        'TGTU__DEL_PC', 'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC',
        'FF__DEL_PC', 'P160__DEL_PC'
    ]

    grouped = df_new.groupby(['ACID', 'reportdatetime'])

    # Filter: keep only ACID+reportdatetime pairs with exactly 2 unique ESNs
    valid_groups = grouped.filter(lambda x: x['ESN'].nunique() == 2)

    # Group by ACID and reportdatetime
    valid_groups_by_pair = valid_groups.groupby(['ACID', 'reportdatetime'])

    # Initialize a list to store each processed row's results
    result_rows = []

    # Iterate over each valid group of (ACID, reportdatetime)
    for (acid, rdt), group in tqdm(valid_groups_by_pair,
                                   desc=f"        LOOP 2 {flight_phase} progress", unit="group"):

        # Sort the group by ESN so that engine order is consistent
        group_sorted = group.sort_values('ESN').reset_index(drop=True)

        # Assign engine 1 and engine 2
        eng1 = group_sorted.iloc[0]
        eng2 = group_sorted.iloc[1]

        # For each engine in the pair
        for i, row in group_sorted.iterrows():
            # Assign the "sister" engine: if this is eng1, sister is eng2 and
            # vice versa
            sister = eng2 if i == 0 else eng1

            # Convert the row to a dictionary for output
            data = row.to_dict()

            # Add the ESN of the sister engine
            data['SISTER_ESN'] = int(sister['ESN'])

            # Calculate E2E deltas for each input parameter
            for param in input_columns:
                try:
                    delta = float(row[param]) - float(sister[param])
                except (ValueError, KeyError):
                    # Assign NaN if conversion or lookup fails
                    delta = float('nan')
                data[param + '_E2E'] = round(delta, 5)

            # Add the enriched row to the result list
            result_rows.append(data)

    # Create a DataFrame from all result rows
    df_out = pd.DataFrame(result_rows)
    df_out = enforce_dtypes(df_out, dtypes_list)

    # Concatenate back old and new data(df_out)
    df_conc = pd.concat([df_old, df_out], ignore_index=True)

    # Remove duplicates
    if not df_conc.empty:
        df_conc = df_conc.sort_values(
            by='reportdatetime',
            ascending=True).drop_duplicates(keep='last')

    # Saves function output to CSV file
    if DebugOption == 1:
        dtypes_txt = False
        if dtypes_txt == True:
            with open(f'Loop_2_{flight_phase}_output_df_dtypes.txt', 'w') as f:
                f.write(str(df_conc.dtypes.to_string()))
            with open(f'Loop_2_{flight_phase}_input_df_dtypes.txt', 'w') as f:
                f.write(str(df_temp.dtypes.to_string()))
        # Get current dir and Fleetstore_Data dir
        current_dir = os.getcwd()
        FleetStore_dir = os.path.join(current_dir, "Fleetstore_Data")
        if not os.path.exists(FleetStore_dir):
            os.makedirs(FleetStore_dir)
        # Save a temporary CSV file for debugging or traceability
        path_temp = os.path.join(FleetStore_dir, f"LOOP_2_{flight_phase}.csv")
        log_message(f"        File saved to: {path_temp}")
        df_conc.to_csv(path_temp)
    return df_conc


# Manually run LOOP_2
if __name__ == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "data_"
    try:
        data_dict = ltd(LOOP_str)
        flight_phase = 'cruise'
        df = data_dict[flight_phase]
        log_message(f"data from {LOOP_str} loaded!")

        df = Loop_2_E2E(df, flight_phase='cruise', DebugOption=1)
        log_message("Loop 2 completed!")

    except Exception as e:

        log_message(f"        error fetching data: {e}")
