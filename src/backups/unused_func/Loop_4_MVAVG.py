import os
import pandas as pd
import numpy as np
from sklearn.covariance import MinCovDet
from src.load_data import load_temp_data as ltd

def loop_4(df: pd.DataFrame, WindowSemiWidth: int = 10) -> pd.DataFrame:
    """
    Performs robust moving average and cumulative delta calculations on engine time-series data,
    mimicking the logic of the original MATLAB Loop 4 script.

    Args:
        df (pd.DataFrame): The input dataframe with engine monitoring data. Must include specific columns
                           such as NEW_FLAG, ESN, FlagSV, FlagSisChg, and 10 engine delta parameters.
        WindowSemiWidth (int, optional): Half-width of the moving window. Default is 10 (=> 20-pt window).

    Returns:
        pd.DataFrame: The modified dataframe with additional columns for robust means and cumulative deltas.
    """

    df = df.copy()

    # Step 1: Identify rows where a discontinuity (e.g., shop visit or sister engine change) occurred
    IndDiscon = df.index[(df['FlagSV'] == 1) | (df['FlagSisChg'] == 1)].tolist()

    # Step 2: Get unique ESNs where NEW_FLAG == 1
    ESNs_with_new_data = df.loc[df['NEW_FLAG'] == 1, 'ESN'].unique()
    NumESNwithNewData = len(ESNs_with_new_data)

    # Step 3: Build dictionaries to store row indices for each ESN
    ESNmin = {} # First occurrence of ESN
    ESNmax = {} # Last occurrence of ESN
    PtFirstNew = {} # First point with NEW_FLAG == 1 for that ESN

    for esn in ESNs_with_new_data:
        esn_mask = df['ESN'] == esn
        new_mask = esn_mask & (df['NEW_FLAG'] == 1)
        ESNmin[esn] = df[esn_mask].index.min()
        ESNmax[esn] = df[esn_mask].index.max()
        PtFirstNew[esn] = df[new_mask].index.min()

    # Step 4: Define the columns to use for the calculations
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]
    robust_cols = [col + '_E2E' for col in delta_cols]
    cumulative_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Step 5: Initialize output columns if missing
    for col in robust_cols + cumulative_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Step 6: Main loop over ESNs
    for esn_idx, esn in enumerate(ESNs_with_new_data, start=1):
        print(f"LOOP 4 : {round(100 * esn_idx / NumESNwithNewData, 1)}% loop completion")

        # Inner loop: from first new data point to last point for this ESN
        for i in range(PtFirstNew[esn], ESNmax[esn] + 1):

            # 1. Calculate the left edge of the window
            WindowMinPoss = max(i - 2 * WindowSemiWidth, ESNmin[esn])

            # 2. If there are any discontinuities between WindowMinPoss and i, start after last one
            IndDisconBefore = [idx for idx in IndDiscon if WindowMinPoss <= idx <= i]
            WindowMin = IndDisconBefore[-1] if IndDisconBefore else WindowMinPoss

            # 3. If the window is wide enough, compute robust mean
            if i - WindowMin >= 2 * WindowSemiWidth - 1e-7:
                for col_base, col_robust in zip(delta_cols, robust_cols):
                    y = df.loc[WindowMin:i, col_base].to_numpy().reshape(-1, 1)

                    # Skip if any NaNs
                    if not np.isnan(y).any():
                        # Fast-MCD robust covariance estimation (mean is stored)
                        mcd = MinCovDet(support_fraction=0.75).fit(y)
                        df.at[i, col_robust] = mcd.location_[0]
            else:
                # If the window is too narrow, store NaN
                df.loc[i, robust_cols] = np.nan

            # 4. Cumulative delta tracking
            if i == ESNmin[esn]:
                # First data point — start from zero
                df.loc[i, cumulative_cols] = 0
            elif df.loc[i, robust_cols].isna().any() or df.loc[i - 1, robust_cols].isna().any():
                # Missing data — carry forward previous cumulative value
                df.loc[i, cumulative_cols] = df.loc[i - 1, cumulative_cols]
            else:
                # Accumulate the delta between current and previous robust mean
                delta = df.loc[i, robust_cols].values - df.loc[i - 1, robust_cols].values
                df.loc[i, cumulative_cols] = df.loc[i - 1, cumulative_cols].values + delta

    return df


import pandas as pd
import numpy as np
from sklearn.covariance import MinCovDet

def loop_4_simplified(df: pd.DataFrame, WindowSemiWidth: int = 10) -> pd.DataFrame:
    """
    Applies a simple moving average and cumulative delta tracking on selected engine deltas,
    while accounting for discontinuities (shop visits or engine changes).

    Parameters:
        df (pd.DataFrame): Input DataFrame with engine monitoring data.
        WindowSemiWidth (int): Half-width of the moving average window (default is 10).

    Returns:
        pd.DataFrame: DataFrame with additional mean-based and cumulative tracking columns.
    """
    df = df.copy()

    # Define the engine delta columns
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]
    mean_cols = [col + '_E2E' for col in delta_cols]
    cumu_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Ensure output columns exist
    for col in mean_cols + cumu_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Identify discontinuity rows
    discontinuities = set(df.index[(df['FlagSV'] == 1) | (df['FlagSisChg'] == 1)])

    # Get ESNs with new data
    esns = df[df['NEW_FLAG'] == 1]['ESN'].unique()
    # Process each ESN
    for idx, esn in enumerate(esns, 1): # index idx start at 1 not 0 while esn is the list of engine with new data
    
        print(f"LOOP 4: {round(100 * idx / len(esns), 1)}% complete")

        esn_df = df[df['ESN'] == esn]
        esn_indices = esn_df.index
        first_idx = esn_df.index.min()
        last_idx = esn_df.index.max()
        first_new_idx = df[(df['ESN'] == esn) & (df['NEW_FLAG'] == 1)].index.min()

        for i in range(first_new_idx, last_idx + 1):
            window_start = max(i - 2 * WindowSemiWidth, first_idx)
            # print(f"window_start: {window_start}")

            # Check for discontinuities and reset window start if needed
            discon_in_window = [d for d in discontinuities if window_start <= d <= i]
            # print(f"discon_in_window: {discon_in_window}")

            if discon_in_window:
                window_start = discon_in_window[-1] # last item in discon_in_window

            # Calculate mean over window if large enough
            # print(f" i - window_start >= 2 * WindowSemiWidth: {i - window_start >= 2 * WindowSemiWidth}")
            if i - window_start >= 2 * WindowSemiWidth:
                for base_col, mean_col in zip(delta_cols, mean_cols):
                    series = df.loc[window_start:i, base_col]
                    # print(f"not series.isna().any() : {not series.isna().any()}")
                    if not series.isna().any():
                        # print(f"base_col: {base_col}, series.mean(): {series.mean()}")
                        df.at[i, mean_col] = series.mean()
            else:
                df.loc[i, mean_cols] = np.nan

            # Cumulative step delta tracking
            if i == first_idx:
                df.loc[i, cumu_cols] = 0
            elif df.loc[i, mean_cols].isna().any() or df.loc[i - 1, mean_cols].isna().any():
                df.loc[i, cumu_cols] = df.loc[i - 1, cumu_cols]
            else:
                df.loc[i, cumu_cols] = df.loc[i - 1, cumu_cols] + (
                    df.loc[i, mean_cols] - df.loc[i - 1, mean_cols]
                )

    return df






if __name__  == "__main__":
    current_dir = os.getcwd()
    print(f"current dir: {current_dir}")
    LOOP_str = "LOOP_3"
        # Step 4: Define the columns to use for the calculations
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]
    robust_cols = [col + '_E2E' for col in delta_cols]
    cumulative_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]
    print("cumulative_cols: \n",cumulative_cols)
    try:
        data_dict = ltd(LOOP_str)
        df = data_dict['cruise']
        df = loop_4_simplified(df)
        print("Loop 4 completed!") 
        path_temp = os.path.join(current_dir, "loop4_temp.csv")
        print(f"file will be saved to : {path_temp}")
        df.to_csv(path_temp)
        print("df[cumulative_cols]: \n",df[cumulative_cols])
        # yield df
    except Exception as e:
        
        print(f"error fetching data: {e}")
