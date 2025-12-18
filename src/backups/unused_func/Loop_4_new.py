import pandas as pd
import numpy as np
import os
from src.load_data import load_temp_data as ltd

def loop_4_mean(df: pd.DataFrame, WindowSemiWidth: int = 10) -> pd.DataFrame:
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]

    E2E_cols = [col + '_E2E' for col in delta_cols]
    E2E_MAV_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Ensure all target columns exist
    for col in E2E_cols + E2E_MAV_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Identify ESNs with new data
    esns = df[df['NEW_FLAG'] == 1]['ESN'].unique()

    for idx, esn in enumerate(esns, 1):
        print(f"LOOP 4: {round(100 * idx / len(esns), 1)}% complete")

        # Filter rows for the current ESN
        esn_mask = df['ESN'] == esn
        df_esn = df[esn_mask].copy() # Make a copy to work safely
        df_esn_indices = df_esn.index

        # Find discontinuities in this ESN
        disc_mask = (df_esn['FlagSV'] == 1) | (df_esn['FlagSisChg'] == 1)
        disc_indices = df_esn.index[disc_mask].tolist()

        # Add start and end for segmentation
        segments = []
        prev_idx = df_esn.index[0]

        for d in disc_indices:
            segments.append((prev_idx, d)) # segment BEFORE discontinuity
            prev_idx = d # restart at discontinuity

        # Add last segment
        segments.append((prev_idx, df_esn.index[-1] + 1)) # end-exclusive

        # For each signal column
        for delta_col in delta_cols:
            e2e_col = delta_col + '_E2E'
            mav_col = delta_col + '_E2E_MAV_NO_STEPS'

            # Initialize empty array to store result
            result = pd.Series(index=df_esn.index, dtype='float64')

            # Process each segment
            for start_idx, end_idx in segments:
                segment = df_esn.loc[start_idx:end_idx - 1, e2e_col]

                # Compute rolling mean
                rolling = segment.rolling(
                    window=2 * WindowSemiWidth + 1,
                    min_periods=2 * WindowSemiWidth + 1
                ).mean()

                # Store result
                result.loc[start_idx:end_idx - 1] = rolling

            # Assign back to full dataframe
            df.loc[df_esn_indices, mav_col] = result

    # Save output
    current_dir = os.getcwd()
    path_temp = os.path.join(current_dir, "loop4_temp.csv")
    print(f"File saved to: {path_temp}")
    df.to_csv(path_temp)

    return df

import pandas as pd
import numpy as np
import os
from sklearn.covariance import MinCovDet
from src.load_data import load_temp_data as ltd

def loop_4_robucov(df: pd.DataFrame, WindowSemiWidth: int = 10) -> pd.DataFrame:
    delta_cols = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC', 'TGTU__DEL_PC',
        'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC', 'FF__DEL_PC', 'P160__DEL_PC'
    ]

    E2E_cols = [col + '_E2E' for col in delta_cols]
    E2E_MAV_cols = [col + '_E2E_MAV_NO_STEPS' for col in delta_cols]

    # Ensure all required columns exist
    for col in E2E_cols + E2E_MAV_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Only process ESNs with NEW_FLAG == 1
    esns = df[df['NEW_FLAG'] == 1]['ESN'].unique()

    for idx, esn in enumerate(esns, 1):
        print(f"LOOP 4: {round(100 * idx / len(esns), 1)}% complete")

        # Extract data for the current ESN
        esn_mask = df['ESN'] == esn
        df_esn = df[esn_mask].copy()
        df_esn_indices = df_esn.index

        # Identify discontinuity indices for this ESN
        discon_idx = set(df_esn[df_esn['FlagSV'] == 1].index) | set(df_esn[df_esn['FlagSisChg'] == 1].index)

        # Prepare values as 2D array
        values = df_esn[E2E_cols].values
        win_size = 2 * WindowSemiWidth + 1

        # Store results here
        result = pd.DataFrame(index=df_esn.index, columns=E2E_MAV_cols)

        for i, idx_row in enumerate(df_esn.index):
            window_start = max(0, i - win_size + 1)
            window_end = i + 1
            window_indices = df_esn.index[window_start:window_end]

            # Skip if discontinuity is in window
            if any(w in discon_idx for w in window_indices):
                result.loc[idx_row] = np.nan
                continue

            window_data = values[window_start:window_end, :]

            # Skip if any NaNs in the window
            if np.isnan(window_data).any():
                result.loc[idx_row] = np.nan
                continue

            try:
                mcd = MinCovDet(support_fraction=0.75).fit(window_data)
                robust_mean = mcd.location_
                result.loc[idx_row] = robust_mean
            except ValueError:
                result.loc[idx_row] = np.nan

        # Assign results to original df
        for col_idx, col in enumerate(delta_cols):
            out_col = col + '_E2E_MAV_NO_STEPS'
            df.loc[df_esn_indices, out_col] = result.iloc[:, col_idx].values

    # Save CSV for traceability
    current_dir = os.getcwd()
    path_temp = os.path.join(current_dir, "loop4_temp.csv")
    print(f"File saved to: {path_temp}")
    df.to_csv(path_temp)

    return df
