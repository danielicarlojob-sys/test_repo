"""
Loop 6: Fit Signatures to Flight Phase Data with Optional Parallelism
======================================================================

This module contains a refactored version of the `loop_6_fit_signatures` function
with optional multi-core parallel execution, designed to process flight data
for different phases and compute fits to precomputed signature matrices.

Features:
- Optional parallel execution using ProcessPoolExecutor
- Safe handling of dtypes to prevent pandas FutureWarnings
- Google-style docstrings and line-by-line comments
- Optional max_workers parameter to control parallelism
"""

import asyncio
import os
from datetime import datetime as dt

from src.utils.log_file import log_message
from src.utils.print_time_now import print_time_now

import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
from concurrent.futures import ProcessPoolExecutor

# --- Global variables for worker processes ---
_signature_combos = None
_obs_mag_cols = None
_lag_list = None
_df_new = None

def _init_worker(signature_combos, obs_mag_cols, lag_list, df_new):
    """Initializer to set global variables in each worker process."""
    global _signature_combos, _obs_mag_cols, _lag_list, _df_new
    _signature_combos = signature_combos
    _obs_mag_cols = obs_mag_cols
    _lag_list = lag_list
    _df_new = df_new

def process_row(idx):
    """Worker function to process a single row index."""
    row = _df_new.loc[idx].copy()  # Copy the row to avoid modifying the shared DataFrame
    for lag in _lag_list:
        # Construct the lagged column names
        shift_columns_lag = [col + str(lag) for col in _obs_mag_cols]
        try:
            # Extract observation vector for the current lag
            obs_vec = row[shift_columns_lag].to_numpy(dtype=float)
        except KeyError:
            # Skip if any column is missing
            continue
        if np.isnan(obs_vec).any():
            # Skip row if it contains NaNs
            continue

        obs_mag = np.linalg.norm(obs_vec)  # Compute magnitude of observation vector
        row[f"OBS_MAGNITUDE{lag}"] = obs_mag
        if obs_mag == 0.0:
            continue  # Skip zero vectors

        best_fit = None
        best_err = np.inf

        # Loop through all signature combinations
        for F, norms, ids in _signature_combos:
            coeffs, *_ = np.linalg.lstsq(F, obs_vec, rcond=None)  # Solve linear least squares
            fit_vec = F @ coeffs  # Compute fitted vector
            err = np.linalg.norm(obs_vec - fit_vec) / obs_mag  # Relative error
            magnitudes = np.abs(coeffs * norms)  # Magnitudes for each coefficient

            # Check if this combination meets thresholds
            if err < 0.3 and np.sum(magnitudes) < 5 * obs_mag and err < best_err:
                best_fit = (coeffs, err, magnitudes, ids, fit_vec)
                best_err = err

        if best_fit:
            coeffs, err, magnitudes, ids, fit_vec = best_fit
            for i in range(3):
                row[f"VAR{i + 1}_SHIFT{lag}"] = coeffs[i] if i < len(coeffs) else np.nan
                row[f"VAR{i + 1}_MAGNITUDE{lag}"] = magnitudes[i] if i < len(magnitudes) else np.nan
                row[f"VAR{i + 1}_IDENTIFIER{lag}"] = ids[i] if i < len(ids) else np.nan
            row[f"ERROR_REL{lag}"] = err
            row[f"ERROR_MAGNITUDE{lag}"] = np.linalg.norm(obs_vec - fit_vec)
    return idx, row

def loop_6_fit_signatures(
    df: pd.DataFrame,
    flight_phase: str,
    Xrates: dict,
    lag_list: list = [50, 100, 200, 400],
    DebugOption: int = 1,
    use_parallel: bool = True,
    max_workers: int = None
) -> pd.DataFrame:
    """
    Fit observed magnitude shifts against signature vectors for up to 3 signatures using least squares.

    This function processes rows flagged as "new" (NEW_FLAG == 1) and attempts to explain observed
    magnitude shift vectors as linear combinations of up to three signature vectors from the
    provided Xrates dictionary. It skips rows with NaNs or zero-norm vectors and appends fit results
    and error metrics to the DataFrame.

    Parameters
    -----
        - df (pd.DataFrame): Flight data containing new rows to process (NEW_FLAG == 1)
        - flight_phase (str): Phase of flight ('cruise', 'climb', 'take-off')
        - Xrates (dict): Dictionary of Xrates DataFrames for each flight phase
        - lag_list (list, optional): List of lags to compute. Defaults to [50,100,200,400].
        - DebugOption (int, optional): If 1, saves output CSV. Defaults to 1.
        - use_parallel (bool, optional): If True, uses multi-core parallel processing.
        - max_workers (int, optional): Maximum number of processes for parallel execution.

    Returns:
    --------
        - df_out (pd.DataFrame): with computed VAR_* columns, error metrics, and observation magnitudes.
    """
    df = df.copy()  # Copy input to avoid in-place modifications

    # Preallocate columns for all lags
    for lag in lag_list:
        for i in range(1, 4):
            df[f"VAR{i}_SHIFT{lag}"] = np.nan
            df[f"VAR{i}_MAGNITUDE{lag}"] = np.nan
            df[f"VAR{i}_IDENTIFIER{lag}"] = pd.Series([np.nan]*len(df), dtype=object)
        df[f"ERROR_REL{lag}"] = np.nan
        df[f"ERROR_MAGNITUDE{lag}"] = np.nan
        df[f"OBS_MAGNITUDE{lag}"] = np.nan

    # Split into new and old rows
    df_old = df[df["NEW_FLAG"] != 1].copy()
    df_new = df[df["NEW_FLAG"] == 1].copy()
    if df_new.empty:
        return df.copy()

    # Prepare Xrates for this flight phase
    Xrates_fp = Xrates[flight_phase.capitalize()]
    if not all(isinstance(idx, str) for idx in Xrates_fp.index):
        Xrates_fp.index = [f"Xrate_{i+1}" for i in range(len(Xrates_fp))]

    signature_matrix = Xrates_fp.iloc[:, :-1].to_numpy(dtype=float)
    signature_norms = Xrates_fp.iloc[:, -1].to_numpy(dtype=float)
    signature_ids = list(Xrates_fp.index)

    # Precompute signature combinations
    signature_combos = []
    for n_sig in (1, 2, 3):
        for combo in itertools.combinations(range(len(signature_ids)), n_sig):
            F = signature_matrix[list(combo)].T
            norms = signature_norms[list(combo)]
            ids = [signature_ids[i] for i in combo]
            signature_combos.append((F, norms, ids))

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
    # Parallel or sequential execution
    # if use_parallel:
    if __name__ == "__main__" and use_parallel:

        with ProcessPoolExecutor(
            initializer=_init_worker,
            initargs=(signature_combos, obs_mag_cols, lag_list, df_new),
            max_workers=max_workers
        ) as executor:
            results = list(
                tqdm(
                    executor.map(process_row, df_new.index),
                    total=len(df_new),
                    desc=f" LOOP 6 {flight_phase} parallel",
                    unit="row"
                )
            )

        # Merge results
        for idx, updated_row in results:
            for col in updated_row.index:
                df_new.at[idx, col] = updated_row[col]
    else:
        # Sequential processing
        for idx in tqdm(df_new.index, desc=f" LOOP 6 {flight_phase} sequential", unit="row"):
            _, updated_row = process_row(idx)
            for col in updated_row.index:
                df_new.at[idx, col] = updated_row[col]

    # Concatenate old and new rows
    df_out = pd.concat([df_old, df_new]).sort_index()

    # Save CSV if DebugOption enabled
    if DebugOption == 1:
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"LOOP_6_{flight_phase}_v10.csv")
        log_message(f"File saved to: {path_temp}")
        df_out.to_csv(path_temp, index=False)

    return df_out

async def process_phase_loop_6_async(flight_phase, df_phase, Xrates_loaded):
    loop_6_time_start = print_time_now()

    df_out = await asyncio.to_thread(
        loop_6_fit_signatures,
        df_phase,
        flight_phase,
        Xrates_loaded,
        DebugOption=1,
        use_parallel=True,
        max_workers=None
    )

    loop_6_time_end = print_time_now()
    start_dt = dt.strptime(loop_6_time_start, "%H:%M:%S %d-%m-%y")
    end_dt = dt.strptime(loop_6_time_end, "%H:%M:%S %d-%m-%y")
    loop_6_time_duration = str(end_dt - start_dt)

    log_message(f"Loop 6 fitting completed - flight phase: {flight_phase}")
    log_message(f"Loop 6 - flight phase: {flight_phase}'s elapsed time {loop_6_time_duration}")

    return flight_phase, df_out


async def loop_6_main_async(data_dict, Xrates_loaded):
    """Run all phases in parallel and return updated dict."""
    tasks = []
    for flight_phase, df_phase in data_dict.items():
        tasks.append(process_phase_loop_6_async(flight_phase, df_phase, Xrates_loaded))

    results = await asyncio.gather(*tasks)
    return {phase: df_out for phase, df_out in results}

# ==============================
# Script entry point for testing
# ==============================

if __name__ == "__main__":
    from src.utils.load_data import load_temp_data as ltd
    from src.utils.Initialise_Algorithm_Settings_engine_type_specific import (
        Initialise_Algorithm_Settings_engine_type_specific,
        Xrates_dic_vector_norm,
    )

    root = os.getcwd()
    data_folder = os.path.join(root, "Fleetstore_Data")
    lim_dict, Xrates_loaded = Initialise_Algorithm_Settings_engine_type_specific()
    Xrates_loaded = Xrates_dic_vector_norm(Xrates_loaded)
    #print(f">>> data_folder:{os.path.normpath(data_folder)}")
    #flight_phases = ["cruise","climb","take-off"]
    data_dict = ltd("LOOP_5", data_folder)
    for flight_phase in data_dict.keys():
        if flight_phase not in data_dict.keys():
            raise KeyError(f"{flight_phase} not found in loaded LOOP_5 data. Available keys: " + ", ".join(data_dict.keys()))
        df_crz = data_dict[flight_phase]


        try:
            capitalized_flight_phase = flight_phase.capitalize()
            Xrates_loaded[capitalized_flight_phase].to_csv(f'Xrates_temp_{capitalized_flight_phase}.csv', index=True)
        except Exception as e:
            log_message(f"Could not export Xrates_temp_{capitalized_flight_phase}.csv: {e}")
        
        sequential_run = False
        parallel_run = True
        
        if sequential_run == True:
            # Run fitting with optional parallelism and max_workers control
            loop_6_time_start = print_time_now()
            df_crz = loop_6_fit_signatures(
                df=df_crz,
                flight_phase=flight_phase,
                Xrates=Xrates_loaded,
                DebugOption=1,
                use_parallel=True,
                max_workers=None  # Set number of processes here if desired
            )
            loop_6_time_end = print_time_now()
            start_dt = dt.strptime(loop_6_time_start, "%H:%M:%S %d-%m-%y")
            end_dt = dt.strptime(loop_6_time_end, "%H:%M:%S %d-%m-%y")

            loop_6_time_duration = str(end_dt - start_dt)
            log_message(f"Loop 6 fitting completed - flight phase: {flight_phase}")
            log_message(f"Loop 6  - flight phase: {flight_phase}'s elapsed time {loop_6_time_duration}")
        if parallel_run == True:
            # Run all phases in parallel
            updated_data_dict = asyncio.run(loop_6_main_async(data_dict, Xrates_loaded))        
