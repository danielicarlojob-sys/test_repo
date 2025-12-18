import pandas as pd
import numpy as np
import os
import warnings
from sklearn.covariance import MinCovDet
from tqdm import tqdm
from src.utils.load_data import load_temp_data as ltd

def loop_4_robcov_backup(df: pd.DataFrame, flight_phase: str = None, WindowSemiWidth: int = 10, DebugOption: int = 1) -> pd.DataFrame:
    """
    Applies a robust moving average (using Minimum Covariance Determinant) to selected columns of a DataFrame,
    grouped by ESN where NEW_FLAG == 1, and handles discontinuities and missing data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing columns for ESN, NEW_FLAG, FlagSV, FlagSisChg, and the required delta columns.
    WindowSemiWidth : int, optional
        Semi-width of the moving window (default is 10). The full window size is 2 * WindowSemiWidth + 1.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with additional columns containing the robust moving averages.
    """
    # Define delta columns to process
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]

    # Define corresponding E2E and output columns
    E2E_cols = [col + '_E2E' for col in delta_cols]
    E2E_MAV_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Ensure required columns exist
    for col in E2E_cols + E2E_MAV_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Filter ESNs with NEW_FLAG == 1
    esns = df[df['NEW_FLAG'] == 1]['ESN'].unique()

    for esn in tqdm(esns, desc=f"        LOOP 4 {flight_phase} progress", unit="ESN"):
        """
        print(f"        LOOP 4 {flight_phase}: {round(100 * idx / len(esns), 1)}% complete")
        """

        # Filter rows for current ESN
        esn_mask = df['ESN'] == esn
        df_esn = df[esn_mask].copy()
        df_esn_indices = df_esn.index

        # Identify discontinuity points (SV or SisChg)
        discon_idx = set(df_esn[df_esn['FlagSV'] == 1].index) | set(df_esn[df_esn['FlagSisChg'] == 1].index)

        # Extract values for E2E columns
        values = df_esn[E2E_cols].values
        win_size = 2 * WindowSemiWidth + 1

        # Initialize result DataFrame with float64 dtype to avoid warnings
        result = pd.DataFrame(index=df_esn.index, columns=E2E_MAV_cols, dtype=np.float64)

        # Loop through each row (by index position, not label)
        for i, idx_row in enumerate(df_esn.index):
            window_start = max(0, i - win_size + 1)
            window_end = i + 1
            window_indices = df_esn.index[window_start:window_end]

            # Skip if any discontinuity in window
            if any(w in discon_idx for w in window_indices):
                result.loc[idx_row] = np.nan
                continue

            # Get the window data
            window_data = values[window_start:window_end, :]

            # Skip if any NaN in window
            if np.isnan(window_data).any():
                result.loc[idx_row] = np.nan
                continue

            try:
                # Suppress covariance rank warnings during robust fit
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    # calculates the robust moving average over window_data
                    mcd = MinCovDet(support_fraction=0.75).fit(window_data) # HERE MinCovDet CALCULATED THE ROBUST MEAN FOR THE MATRIX AS A WHOLE!!!

                # Round to 5 decimals and cast to float64
                robust_mean = np.round(mcd.location_, 5).astype(np.float64)
                result.loc[idx_row] = robust_mean

            except ValueError:
                result.loc[idx_row] = np.nan

        # Assign result back to original DataFrame with correct dtype
        for col_idx, col in enumerate(delta_cols):
            out_col = col + '_E2E_MAV_NO_STEPS'
            df.loc[df_esn_indices, out_col] = result.iloc[:, col_idx].values.astype(np.float64)
    if DebugOption == 1:
        # Save a temporary CSV file for debugging or traceability
        current_dir = os.getcwd()
        fleetore_dir = os.path.join(current_dir,"Fleetstore_Data")
        path_temp = os.path.join(fleetore_dir, f"LOOP_4_{flight_phase}.csv")
        print(f"        File saved to: {path_temp}")
        df.to_csv(path_temp)

    return df





if __name__  == "__main__":
    current_dir = os.getcwd()
    print(f"        current dir: {current_dir}")
    LOOP_str = "LOOP_3"
        # Step 4: Define the columns to use for the calculations
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]
    robust_cols = [col + '_E2E' for col in delta_cols]
    cumulative_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]
    # print("cumulative_cols: \n",cumulative_cols)
    try:
        data_dict = ltd(LOOP_str)
        df = data_dict['cruise']
        """
        df = loop_4_robcov(df,'cruise')
        print("Loop 4 completed!") 
        path_temp = os.path.join(current_dir, "loop4_temp.csv")
        print(f"file will be saved to : {path_temp}")
        df.to_csv(path_temp)
        print("df[cumulative_cols]: \n",df[cumulative_cols])
        """
        # yield df
    except Exception as e:
        
        print(f"        error fetching data: {e}")
