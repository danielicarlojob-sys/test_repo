import pandas as pd
import numpy as np
from sklearn.covariance import MinCovDet


def calculate_robust_e2e_moving_average_df(
    df: pd.DataFrame,
    ESNmin: np.ndarray,
    ESNmax: np.ndarray,
    PtFirstNew: np.ndarray,
    WindowSemiWidth: int,
    NumESNwithNewData: int,
    e2e_delta_cols: list,
    robust_avg_cols: list,
    cumu_sum_cols: list,
    flag_sv_col: str = 'FlagSV',
    flag_sischg_col: str = 'FlagSisChg'
) -> pd.DataFrame:
    """
    Compute robust moving averages and cumulative sum of E2E deltas using pandas DataFrame.

    Parameters:
    - df: Input DataFrame.
    - ESNmin, ESNmax, PtFirstNew: Arrays of indices per engine.
    - WindowSemiWidth: Half-width of the moving window.
    - NumESNwithNewData: Number of engine segments to process.
    - e2e_delta_cols: List of column names with E2E deltas (length 10).
    - robust_avg_cols: Columns to store robust means.
    - cumu_sum_cols: Columns to store cumulative MAV without steps.
    - flag_sv_col, flag_sischg_col: Columns indicating discontinuities.

    Returns:
    - DataFrame with updated columns.
    """
    # Step 1: Get indices where discontinuity flags are set
    ind_discon = df.index[
        (df[flag_sv_col] == 1) | (df[flag_sischg_col] == 1)
    ].to_numpy()

    for count in range(NumESNwithNewData):
        print(
            f"LOOP 4 : {round(100 * (count + 1) / NumESNwithNewData, 1)}% loop completion")

        for i in range(PtFirstNew[count], ESNmax[count] + 1):
            window_min_possible = max(i - 2 * WindowSemiWidth, ESNmin[count])

            # Step 2: Check for discontinuity within window
            discon_in_window = ind_discon[
                (ind_discon >= window_min_possible) & (ind_discon <= i)
            ]
            window_min = discon_in_window[-1] if len(
                discon_in_window) > 0 else window_min_possible

            # Step 3: Compute robust average if window is large enough
            if i - window_min >= 2 * WindowSemiWidth - 1e-7:
                for p, col in enumerate(e2e_delta_cols):
                    y = df.loc[window_min:i, col].to_numpy()
                    if not np.isnan(y).any():
                        mcd = MinCovDet(support_fraction=0.75).fit(
                            y.reshape(-1, 1))
                        df.at[i, robust_avg_cols[p]] = mcd.location_[0]
            else:
                df.loc[i, robust_avg_cols] = np.nan

            # Step 4: Compute cumulative sum of robust averages
            if i == ESNmin[count]:
                df.loc[i, cumu_sum_cols] = 0
            elif df.loc[i, robust_avg_cols].isna().any() or df.loc[i - 1, robust_avg_cols].isna().any():
                df.loc[i, cumu_sum_cols] = df.loc[i - 1, cumu_sum_cols]
            else:
                df.loc[i, cumu_sum_cols] = (
                    df.loc[i - 1, cumu_sum_cols] +
                    df.loc[i, robust_avg_cols] - df.loc[i - 1, robust_avg_cols]
                )

    return df
