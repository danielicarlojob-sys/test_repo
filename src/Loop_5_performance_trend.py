import os
import pandas as pd
import numpy as np
from src.utils.log_file import log_message
from tqdm import tqdm  # For showing a progress bar in loops

# Custom data loading function (not used in this function but likely
# available for debug or future use)

# Define the main function to process performance trends over different
# lag intervals


def Loop5_performance_trend(
    df: pd.DataFrame,
    flight_phase: str = None,
    Lag: list = [50, 100, 200, 400],
    DebugOption: int = 1
) -> pd.DataFrame:
    """
    Implements Loop 5: calculates changes in E2E deltas over lagged windows
    for each ESN from the E2E_MAV_NO_STEPS columns.

    Parameters
    ----------
    Args:
        - df (pd.DataFrame): Input dataframe with ESN and E2E_MAV_NO_STEPS columns.
        - flight_phase (str): input string containing flight phase
        - Lag (list): list of windows datapoints
        - DebugOption (int): switch to create CSV output

    Returns
    -------
        - pd.DataFrame: Modified dataframe with new LAG columns added.
    """
    # Define function's dysplay name
    Loop5_performance_trend.display_name = "LOOP 5 - changes in E2E deltas over lagged windows"
    # Define the list of columns for which lag deltas will be calculated
    e2e_mav21_cols = [
        'PS26__DEL_PC_E2E_MAV_NO_STEPS',
        'T25__DEL_PC_E2E_MAV_NO_STEPS',
        'P30__DEL_PC_E2E_MAV_NO_STEPS',
        'T30__DEL_PC_E2E_MAV_NO_STEPS',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS',
        'NL__DEL_PC_E2E_MAV_NO_STEPS',
        'NI__DEL_PC_E2E_MAV_NO_STEPS',
        'NH__DEL_PC_E2E_MAV_NO_STEPS',
        'FF__DEL_PC_E2E_MAV_NO_STEPS',
        'P160__DEL_PC_E2E_MAV_NO_STEPS'
    ]

    # Make a working copy of the input DataFrame to avoid mutating the original
    df_out = df.copy()

    # Preventive sort old to new data and reset df index
    df_out = df_out.sort_values(by='reportdatetime', ascending=True).reset_index(drop=True)

    # Get the list of ESNs (engine serial numbers) that have NEW_FLAG = 1
    # These are the engines for which the lag delta calculation should be
    # applied
    esns_with_new_data = df_out[df_out["NEW_FLAG"] == 1]["ESN"].unique()

    # Loop through each ESN with new data, showing progress using tqdm
    for esn in tqdm(
            esns_with_new_data,
            desc=f" LOOP 5 {flight_phase} progress",
            unit="ESN"):

        # Filter the DataFrame to only include rows for this ESN
        df_esn = df_out[df_out["ESN"] == esn]

        # Get the index positions of the rows for this ESN
        idx_esn = df_esn.index

        # Loop through each lag window (e.g., 50, 100, 200, 400 points)
        for lag in Lag:

            # Loop through each point in the ESN's data
            for i in range(len(idx_esn)):
                idx_now = idx_esn[i]  # Index of the current row

                # Skip if there isn't enough history to calculate the lag
                if i - lag < 0:
                    continue

                # Index of the row 'lag' steps before
                idx_prev = idx_esn[i - lag]

                # For each of the E2E MAV columns, calculate the difference
                # over the lag
                for col in e2e_mav21_cols:
                    # Define the name for the new lagged column
                    col_lag = f"{col}_LAG_{lag}"

                    # If the lagged column doesn't already exist, initialize it
                    # with NaN values
                    if col_lag not in df_out.columns:
                        df_out[col_lag] = np.nan

                    # Get the current and previous values for the column
                    val_now = df_out.at[idx_now, col]
                    val_prev = df_out.at[idx_prev, col]

                    # If both values are valid (not NaN), compute the
                    # difference and store it
                    if not pd.isna(val_now) and not pd.isna(val_prev):
                        df_out.at[idx_now, col_lag] = round(val_now - val_prev,5)

    # Remove duplicates
    df_out = df_out.sort_values(
        by='reportdatetime',
        ascending=True).drop_duplicates(keep='last')

    # If debugging is enabled, save the resulting DataFrame to a temporary CSV
    # file
    if DebugOption == 1:
        # Get the current working directory
        current_dir = os.getcwd()

        # Construct the path to a subdirectory for saving the file
        fleetore_dir = os.path.join(current_dir, "Fleetstore_Data")

        # Construct the full file path, including flight phase name
        path_temp = os.path.join(fleetore_dir, f"LOOP_5_{flight_phase}.csv")

        # Print the file path to the console
        log_message(f" File saved to: {path_temp}")

        # Write the modified DataFrame to CSV
        df_out.to_csv(path_temp)

    # Return the DataFrame with new lagged delta columns
    return df_out


"""
from src.utils.print_time_now import print_time_now
# Manually run LOOP_5
if __name__  == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_5"
    try:
        data_dict = ltd(LOOP_str)
        # LOOP 5 - changes in E2E deltas over lagged windows
        log_message(f"    Start LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")

        for flight_phase in data_dict.keys():
            log_message(f"        Start {flight_phase} cycle at {str(print_time_now())}")
            df_Loop5 = loop5_performance_trend(data_dict[flight_phase], flight_phase)
            data_dict[flight_phase] = df_Loop5
            log_message(f"        {flight_phase} cycle completed at {str(print_time_now())}")

        log_message(f"    Completed LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")


        LOG_FILE = os.path.join(os.getcwd(), "COLUMNS_Loop5_log.txt")
        with open(LOG_FILE, "a", encoding="utf-8") as f:
             for flight_phase in data_dict.keys():
                [f.write(f"({flight_phase}, {idx}, {i})\n") for  idx, i in enumerate(data_dict[flight_phase].columns)]



        #[log_message(f"flight_phase: {flight_phase}\n DataFrame columns: {list(data_dict[flight_phase].columns)} ") for flight_phase in flight_phases]
        # df = loop5_performance_trend(df,flight_phase)
        #log_message("Loop 5 completed!")
            # Define the 4 moving average window sizes
        lags = [50, 100, 200, 400]

        # Define the base parameter names to look for in real_df and Xrates
        param_names = ['PS26', 'T25', 'P30', 'T30', 'TGTU', 'NL', 'NI', 'NH', 'FF', 'P160']
            # Process each lag window (50, 100, 200, 400)
        lag_cols = []
        for l, lag in enumerate(lags):
            # Create the list of column names for this lag window
            lag_cols.append([f"{param}__DEL_PC_E2E_MAV_NO_STEPS_LAG_{lag}" for param in param_names])

        #[log_message(f"lag_col:\n {lag_cols[i]}\n") for i,lag_col in enumerate(lag_cols)]
        for lag_col in lag_cols:
            #all(col in data_dict[flight_phase].columns for col in lag_col)
            [log_message(f"is lag_col = {lag_col}\n in data_dict[{flight_phase}]?\n {all(col in data_dict[flight_phase].columns for col in lag_col)}\n") for flight_phase in data_dict.keys()]


    except Exception as e:

        log_message(f"        error fetching data: {e}")

"""
