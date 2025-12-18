import numpy as np
import pandas as pd
from itertools import combinations
from src.utils.log_file import LOG_FILE, log_message


def Loop_6(
    df: pd.DataFrame, 
    Xrates: pd.DataFrame, 
    ind_new: list,  
    flight_phase: str = None, 
    DebugOption: int = 1
    ) -> pd.DataFrame:
    """
    Fits each performance shift vector (from df) to a combination of known signature vectors (from Xrates),
    across multiple data windows. For each window, it attempts 1-, 2-, and 3-signature fits and keeps the best one.

    Parameters:
    Args:
         - df: DataFrame containing engine health data including performance shift vectors.
         - Xrates: DataFrame with signature vectors (rows) and their vector norms in 'Vector_Norm'.
         - ind_new: Indices of df to process (new events or rows of interest).

    Returns:
         - Updated df with additional columns for fit results.
    """
    real_df = df.copy()
    # extract rows with new data
    real_new_df = real_df[real_df['NEW_FLAG']==1]
    # extract list of unique ESN with new data
    unique_esn = list(real_new_df['ESN'].unique())
    # Define the 4 moving average window sizes
    lags = [50, 100, 200, 400]

    # Define the base parameter names to look for in real_df and Xrates
    param_names = ['P26', 'T26', 'P30', 'T30', 'TGT', 'NL', 'NI', 'NH', 'WFE']
    # extract the Xrates for the specific flight phase
    Xrates_fp = Xrates[flight_phase]
    # Extract signature vectors E from Xrates using the defined parameters
    E = np.array(Xrates_fp.iloc[:,:9])
    normE = Xrates_fp['Vector_Norm'].values  # Vector norms for scaling contribution magnitudes
    sizeE = E.shape[0]  # Number of signatures available

    nNew = len(ind_new)  # Number of data points to fit

    # Initialize output columns in real_df
    for l in range(4):  # One set of results per lag (50, 100, 200, 400)
        for col_offset in range(13):  # 13 fit outputs per lag
            col_name = f'fit_{l}_param_{col_offset}'
            if col_name not in real_df.columns:
                real_df[col_name] = np.nan

    # Process each lag window (50, 100, 200, 400)
    for l, lag in enumerate(lags):
        # Create the list of column names for this lag window
        lag_cols = [f"{param}__DEL_PC_E2E_MAV_NO_STEPS_LAG_{lag}" for param in param_names] # I THINK THESE COLUMNS HAVE ALREADY BEEN ADDED TO df

        # Extract actual shift vectors for this lag
        actual_shift_vectors = real_df.loc[ind_new, lag_cols].values  # shape: (nNew, 10)

        # Try fitting 1, 2, and 3 signature vectors
        for k in range(1, 4):
            for idxs in combinations(range(sizeE), k):  # all k-combinations of signatures
                F = E[list(idxs), :]  # matrix of selected signatures (shape: k x 10)

                try:
                    # Solve least-squares fit: F.T * coeffs = actual.T => coeffs = F.T⁻¹ * actual.T
                    var_shift = np.linalg.lstsq(F.T, actual_shift_vectors.T, rcond=None)[0].T  # shape: (nNew, k)
                except np.linalg.LinAlgError:
                    continue  # skip singular matrix

                for i, idx in enumerate(ind_new):
                    actual = actual_shift_vectors[i]
                    actual_norm = np.linalg.norm(actual)

                    if actual_norm == 0 or np.isnan(actual_norm):
                        continue  # Skip degenerate input

                    reconstruction = np.dot(var_shift[i], F)  # reconstructed vector using selected signatures
                    error_vec = actual - reconstruction
                    error_norm = np.linalg.norm(error_vec)
                    rel_error = error_norm / actual_norm  # relative error of the fit

                    var_mags = np.abs(var_shift[i] * normE[list(idxs)])  # scaled contribution magnitudes
                    total_mag = np.sum(var_mags)

                    prev_rel_error = real_df.loc[idx, f'fit_{l}_param_3']

                    # Accept new fit if:
                    # - first good one (and rel_error < 30% and magnitude reasonable), or
                    # - better than previous and not exceeding error threshold
                    if (
                        (np.isnan(prev_rel_error) and rel_error < 0.3 and total_mag < 5 * actual_norm) or
                        (not np.isnan(prev_rel_error) and rel_error < prev_rel_error and total_mag < 5 * actual_norm and
                         (k == 1 or not np.isnan(real_df.loc[idx, f'fit_{l}_param_{k}'])))
                    ):
                        # Store coefficients
                        for j in range(k):
                            real_df.loc[idx, f'fit_{l}_param_{j}'] = var_shift[i][j]

                        # Store relative error
                        real_df.loc[idx, f'fit_{l}_param_3'] = rel_error

                        # Store indices of selected signatures
                        for j in range(k):
                            real_df.loc[idx, f'fit_{l}_param_{4 + j}'] = idxs[j]

                        # Store scaled magnitudes of contributions
                        for j in range(k):
                            real_df.loc[idx, f'fit_{l}_param_{9 + j}'] = var_mags[j]

                        # Store absolute error norm
                        real_df.loc[idx, f'fit_{l}_param_12'] = error_norm

    return real_df
