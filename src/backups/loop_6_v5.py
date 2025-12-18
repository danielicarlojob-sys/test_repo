import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
from src.utils.log_file import log_message


def loop_6_fit_signatures(
    df: pd.DataFrame,
    flight_phase: str,
    Xrates: dict,
    lag_list: list = [50, 100, 200, 400],
    DebugOption: int = 1
) -> pd.DataFrame:
    """
    Fit observed magnitude shifts against signature vectors for up to 3 signatures using least squares.

    This function processes rows flagged as "new" (NEW_FLAG == 1) and attempts to explain observed
    magnitude shift vectors as linear combinations of up to three signature vectors from the
    provided Xrates dictionary. It skips rows with NaNs or zero-norm vectors and appends fit results
    and error metrics to the DataFrame.

    Parameters
    ----------
     - df : pd.DataFrame
        Input DataFrame containing observed shift vectors and a 'NEW_FLAG' column.
     - Xrates : dict
        Dictionary mapping flight phases (e.g., 'Cruise') to signature matrices.
        Each matrix is a DataFrame with signature vectors and a final 'norm' column.
     - flight_phase : str
        Flight phase key used to select the appropriate signature matrix from Xrates.
     - lag_list : list of int, optional
        List of lag values to process. Default is [50, 100, 200, 400].
     - DebugOption (int): switch to create CSV output.

    Returns
    -------
     - df_out : pd.DataFrame
        A copy of the input DataFrame with additional columns for each lag:
            - VAR{i}_SHIFT{lag}: Coefficient of the i-th signature
            - VAR{i}_MAGNITUDE{lag}: Magnitude contribution of the i-th signature
            - VAR{i}_IDENTIFIER{lag}: Identifier of the i-th signature used
            - ERROR_REL{lag}: Relative fitting error
            - ERROR_MAGNITUDE{lag}: Absolute fitting error
            - OBS_MAGNITUDE{lag}: Norm of the observed vector
    """

    df = df.copy()
    df_cols = df.columns

    # ----------------------------
    # Preallocate result columns
    # ----------------------------
    cols_to_check = []
    for lag in lag_list:
        # per-signature outputs
        for i in range(1, 4):
            cols_to_check.append(f"VAR{i}_SHIFT{lag}")
            cols_to_check.append(f"VAR{i}_MAGNITUDE{lag}")
            cols_to_check.append(f"VAR{i}_IDENTIFIER{lag}")
        # errors + observed magnitude
        cols_to_check.append(f"ERROR_REL{lag}")
        cols_to_check.append(f"ERROR_MAGNITUDE{lag}")
        cols_to_check.append(f"OBS_MAGNITUDE{lag}")  # ensure this exists too

    for col in cols_to_check:
        if col not in df_cols:
            # identifiers should be object dtype; others can be float
            if col.endswith("IDENTIFIER50") or col.endswith("IDENTIFIER100") or col.endswith("IDENTIFIER200") or col.endswith("IDENTIFIER400"):
                df[col] = pd.Series(dtype=object)
            else:
                df[col] = np.nan

    # Ensure identifier columns are object dtype (in case they already existed)
    for lag in lag_list:
        for i in range(1, 4):
            ident_col = f"VAR{i}_IDENTIFIER{lag}"
            if ident_col in df:
                df[ident_col] = df[ident_col].astype(object)

    # ----------------------------
    # Split df into old/new
    # ----------------------------
    df_old = df[df["NEW_FLAG"] != 1].copy()
    df_new = df[df["NEW_FLAG"] == 1].copy()
    if df_new.empty:
        return df.copy()

    # ----------------------------
    # Prepare Xrates for flight phase
    # ----------------------------
    Xrates_fp = Xrates[flight_phase.capitalize()]
    # Ensure index is strings for safe assignment later
    if not all(isinstance(idx, str) for idx in Xrates_fp.index):
        Xrates_fp.index = [f"Xrate_{i + 1}" for i in range(len(Xrates_fp))]

    # ----------------------------
    # Measured (observed) columns pattern
    # ----------------------------
    obs_mag_cols = [
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_',
        'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_'
    ]

    # ----------------------------
    # Main loop: rows × lags
    # ----------------------------
    for idx in tqdm(df_new.index, desc=f" LOOP 6 {flight_phase} progress", unit="rows"):
        for lag in lag_list:
            shift_columns_lag = [col + str(lag) for col in obs_mag_cols]

            # Gather observed vector for this row/lag
            # (If any required column is missing, this will raise; assuming input is consistent)
            obs_vec = df_new.loc[idx, shift_columns_lag].to_numpy(dtype=float)

            # --- Optimization: skip if any NaN ---
            if np.isnan(obs_vec).any():
                # leave outputs as NaN and skip fitting
                continue

            # Compute magnitude (norm)
            obs_mag = np.linalg.norm(obs_vec)

            # Store observed magnitude for this row/lag
            df_new.at[idx, f"OBS_MAGNITUDE{lag}"] = obs_mag

            # Skip fitting if zero (nothing to explain)
            if obs_mag == 0.0:
                continue

            best_fit = None
            best_err = np.inf

            # Try 1–3 signatures
            n_xrates = Xrates_fp.shape[0]
            for n_sig in (1, 2, 3):
                if n_sig > n_xrates:
                    break  # no combos possible
                for combo in itertools.combinations(range(n_xrates), n_sig):
                    # F has shape (n_features, n_sig)
                    F = Xrates_fp.iloc[list(combo), :-1].to_numpy(dtype=float).T
                    norms = Xrates_fp.iloc[list(combo), -1].to_numpy(dtype=float)

                    # Solve least squares: F * coeffs ≈ obs_vec
                    coeffs, *_ = np.linalg.lstsq(F, obs_vec, rcond=None)
                    fit_vec = F @ coeffs

                    err = np.linalg.norm(obs_vec - fit_vec) / obs_mag
                    magnitudes = np.abs(coeffs * norms)

                    # Keep best fit within thresholds
                    if err < 0.3 and np.sum(magnitudes) < 5 * obs_mag and err < best_err:
                        best_fit = (coeffs, err, magnitudes, combo, fit_vec)
                        best_err = err

            # Write back results if a valid fit was found
            if best_fit:
                coeffs, err, magnitudes, combo, fit_vec = best_fit
                for i in range(3):
                    df_new.at[idx, f"VAR{i + 1}_SHIFT{lag}"] = coeffs[i] if i < len(coeffs) else np.nan
                    df_new.at[idx, f"VAR{i + 1}_MAGNITUDE{lag}"] = magnitudes[i] if i < len(magnitudes) else np.nan
                    df_new.at[idx, f"VAR{i + 1}_IDENTIFIER{lag}"] = (
                        Xrates_fp.index[combo[i]] if i < len(combo) else np.nan
                    )
                df_new.at[idx, f"ERROR_REL{lag}"] = err
                df_new.at[idx, f"ERROR_MAGNITUDE{lag}"] = np.linalg.norm(obs_vec - fit_vec)

    # Recombine and return
    df_out = pd.concat([df_old, df_new]).sort_index()
    
    # If debugging is enabled, save the resulting DataFrame to a temporary CSV
    
    if DebugOption == 1:
        # Get the current working directory
        current_dir = os.getcwd()

        # Construct the path to a subdirectory for saving the file
        fleetore_dir = os.path.join(current_dir, "Fleetstore_Data")

        # Construct the full file path, including flight phase name
        path_temp = os.path.join(fleetore_dir, f"LOOP_6_{flight_phase}.csv")

        # Print the file path to the console
        log_message(f" File saved to: {path_temp}")

        # Write the modified DataFrame to CSV
        df_out.to_csv(path_temp)
    return df_out



# ---------------------------
# 🔹 Example setup
# ---------------------------
if __name__ == "__main__":
    manual_test = 0
    load_csv_data = 1
    if manual_test == 1:
        # Parameters
        parameters = [
            'PS26',
            'T25',
            'P30',
            'T30',
            'TGT',
            'NL',
            'NI',
            'NH',
            'FF',
            'P160']

        # Build Xrates (5 signatures + norms) as numpy arrays
        signatures = [
            np.array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),  # Xrate_1
            np.array([0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),  # Xrate_2
            np.array([0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),  # Xrate_3
            np.array([0, 0, 0, 1, 1, 0, 0, 0, 0, 0]),  # Xrate_4
            np.array([0, 0, 0, 0, 0, 1, 1, 0, 0, 0]),  # Xrate_5
        ]

        # Build Xrates DataFrame with norms
        Xrates = pd.DataFrame(
            [np.append(s, np.linalg.norm(s)) for s in signatures],
            columns=parameters + ["norm"]
        )
        Xrates.index = [f"Xrate_{i + 1}" for i in range(len(Xrates))]

        # Example observed data (linear combo of 3 signatures)
        obs_vector = (
            0.5 * signatures[0] +
            1.2 * signatures[3] +
            -0.8 * signatures[4]
        )
        obs_norm = np.linalg.norm(obs_vector)

        df = pd.DataFrame([dict(zip(parameters, obs_vector))
                        | {"OBS_MAGNITUDE50": obs_norm}])

        # Run fit
        df_out = loop_6_fit_signatures(df, Xrates, lag_list=[50])

        print("\nInput Observed Vector:\n", df[parameters].values)
        print("\nFit Results:\n", df_out.filter(regex="VAR|ERROR").head())
    if load_csv_data == 1:
        import os
        from src.utils.log_file import log_message
        from src.utils.load_data import load_temp_data as ltd
        from src.utils.Initialise_Algorithm_Settings_engine_type_specific import Initialise_Algorithm_Settings_engine_type_specific, Xrates_dic_vector_norm

        lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()

        Xrates = Xrates_dic_vector_norm(Xrates)
        Xrates['Cruise'].to_csv('Xrates_temp_Cruise.csv')
        print(f">>> Xrates.keys:{Xrates.keys()}")
        current_dir = os.getcwd()
        log_message(f"        current dir: {current_dir}")
        LOOP_str = "LOOP_5"
        try:
            data_dict = ltd(LOOP_str)
            df = data_dict['cruise']
            df = loop_6_fit_signatures(df, Xrates, 'cruise')
            log_message("Loop 6 completed!")
            path_temp = os.path.join(current_dir, "loop6_temp.csv")
            log_message(f"file will be saved to : {path_temp}")
            df.to_csv(path_temp)
            
        except Exception as e:

            log_message(f"        error fetching data: {e}")