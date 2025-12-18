"""
Improvments from loop_6_v5.py:
------------------------------
     - Multi-core execution via ThreadPoolExecutor
     - Significant speedup for large datasets
     - Same output structure and logic
     - Easy to extend to ProcessPoolExecutor if tasks become CPU-heavy
"""

import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
from concurrent.futures import ThreadPoolExecutor
from src.utils.log_file import log_message

def loop_6_fit_signatures(
    df: pd.DataFrame,
    flight_phase: str,
    Xrates: dict,
    lag_list: list = [50, 100, 200, 400],
    DebugOption: int = 1
) -> pd.DataFrame:

    df = df.copy()

    # Preallocate result columns
    for lag in lag_list:
        for i in range(1, 4):
            df.setdefault(f"VAR{i}_SHIFT{lag}", np.nan)
            df.setdefault(f"VAR{i}_MAGNITUDE{lag}", np.nan)
            df.setdefault(f"VAR{i}_IDENTIFIER{lag}", pd.Series(dtype=object))
        df.setdefault(f"ERROR_REL{lag}", np.nan)
        df.setdefault(f"ERROR_MAGNITUDE{lag}", np.nan)
        df.setdefault(f"OBS_MAGNITUDE{lag}", np.nan)

    df_old = df[df["NEW_FLAG"] != 1].copy()
    df_new = df[df["NEW_FLAG"] == 1].copy()
    if df_new.empty:
        return df.copy()

    # Prepare Xrates
    Xrates_fp = Xrates[flight_phase.capitalize()]
    if not all(isinstance(idx, str) for idx in Xrates_fp.index):
        Xrates_fp.index = [f"Xrate_{i + 1}" for i in range(len(Xrates_fp))]

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

    def process_row(idx):
        row = df_new.loc[idx].copy()
        for lag in lag_list:
            shift_columns_lag = [col + str(lag) for col in obs_mag_cols]
            try:
                obs_vec = row[shift_columns_lag].to_numpy(dtype=float)
            except KeyError:
                continue
            if np.isnan(obs_vec).any():
                continue

            obs_mag = np.linalg.norm(obs_vec)
            row[f"OBS_MAGNITUDE{lag}"] = obs_mag
            if obs_mag == 0.0:
                continue

            best_fit = None
            best_err = np.inf

            for F, norms, ids in signature_combos:
                coeffs, *_ = np.linalg.lstsq(F, obs_vec, rcond=None)
                fit_vec = F @ coeffs
                err = np.linalg.norm(obs_vec - fit_vec) / obs_mag
                magnitudes = np.abs(coeffs * norms)

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

    # Parallel execution
    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(process_row, df_new.index), total=len(df_new), desc=f" LOOP 6 {flight_phase} parallel"))

    # Rebuild df_new from results
    for idx, updated_row in results:
        df_new.loc[idx] = updated_row

    df_out = pd.concat([df_old, df_new]).sort_index()

    if DebugOption == 1:
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"LOOP_6_{flight_phase}.csv")
        log_message(f" File saved to: {path_temp}")
        df_out.to_csv(path_temp)

    return df_out
