import numpy as np
import pandas as pd
from tqdm import tqdm
import itertools
from src.utils.log_file import log_message


def loop_6_fit_signatures(
    df: pd.DataFrame,
    Xrates: dict,
    flight_phase: str,
    lag_list: list = [50, 100, 200, 400]
) -> pd.DataFrame:

    df = df.copy()
    df_cols = df.columns
    cols_to_check = []

    # Preallocate columns
    for lag in lag_list:
        for i in range(1, 4):
            cols_to_check.append(f"VAR{i}_SHIFT{lag}")
            cols_to_check.append(f"VAR{i}_MAGNITUDE{lag}")
            cols_to_check.append(f"VAR{i}_IDENTIFIER{lag}")
        cols_to_check.append(f"ERROR_REL{lag}")
        cols_to_check.append(f"ERROR_MAGNITUDE{lag}")

    for col in cols_to_check:
        if col not in df_cols:
            df[col] = np.nan

    # Split df
    df_old = df[df["NEW_FLAG"] != 1].copy()
    df_new = df[df["NEW_FLAG"] == 1].copy()
    if df_new.empty:
        return df.copy()
    # print(f">>> number of NEW ROWS {df_new.shape[0]}")
    # Ensure Xrates_fp index are strings
    Xrates_fp = Xrates[flight_phase.capitalize()]
    if not all(isinstance(idx, str) for idx in Xrates_fp.index):
        Xrates_fp.index = [f"Xrate_{i + 1}" for i in range(len(Xrates_fp))]
    #print(f">>> number of Xrates_fp rows {Xrates_fp.shape[0]}")

    # Pre-create columns and ensure object dtype for identifiers
    for lag in lag_list:
        for i in range(1, 4):
            df_new[f"VAR{i}_SHIFT{lag}"] = np.nan
            df_new[f"VAR{i}_MAGNITUDE{lag}"] = np.nan
            df_new[f"VAR{i}_IDENTIFIER{lag}"] = df_new.get(
                f"VAR{i}_IDENTIFIER{lag}", pd.Series(dtype=object)
            ).astype(object)
        df_new[f"ERROR_REL{lag}"] = np.nan
        df_new[f"ERROR_MAGNITUDE{lag}"] = np.nan

    # Loop over rows
    # for idx, row in df_new.iterrows():
    obs_mag_cols = ['PS26__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'T25__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'P30__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'T30__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'TGTU__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'NL__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'NI__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'NH__DEL_PC_E2E_MAV_NO_STEPS_LAG_', 
                    'FF__DEL_PC_E2E_MAV_NO_STEPS_LAG_' 
                    ]
    
    #for idx, row in df_new.iterrows():
    for idx, row in tqdm(df_new.iterrows(), desc=f" LOOP 6 {flight_phase} progress", unit="rows"):

        print("\n")
        
        for lag in lag_list:
            shift_columns_lag = [col+str(lag) for col in obs_mag_cols]
            #print(f"---> idx, row:{idx, row}")
            obs_vec = df_new.loc[idx,shift_columns_lag].to_numpy(dtype=float)
            obs_mag = np.linalg.norm(obs_vec)
            observed_mag_col_name ="OBS_MAGNITUDE"+str(lag)
            df_new[observed_mag_col_name] = obs_mag
            print(f"idx:{idx} of {df_new.shape[0]} lag:{lag} obs_mag:{obs_mag}")
            print(f"---> shift_columns_lag:\n{obs_vec}")
            
            #if pd.isna(obs_mag) or obs_mag == 0:
                #continue

            #obs_vec = row[Xrates_fp.columns[:-1]].to_numpy(dtype=float)

            best_fit = None
            best_err = np.inf

            # Try 1-3 signatures
            for n_sig in [1, 2, 3]:
                #print(f"range(Xrates_fp.shape[0]), n_sig) : {range(Xrates_fp.shape[0])}")
                for combo in itertools.combinations(
                        range(Xrates_fp.shape[0]), n_sig):
                    #print(f">>> combo:{print(combo)}")
                    
                    F = Xrates_fp.iloc[list(
                        combo), :-1].to_numpy(dtype=float).T
                    norms = Xrates_fp.iloc[list(
                        combo), -1].to_numpy(dtype=float)
                    coeffs, *_ = np.linalg.lstsq(F, obs_vec, rcond=None)
                    fit_vec = F @ coeffs
                    err = np.linalg.norm(
                        obs_vec - fit_vec) / np.linalg.norm(obs_vec)
                    magnitudes = np.abs(coeffs * norms)
              

                    if err < 0.3 and np.sum(
                            magnitudes) < 5 * obs_mag and err < best_err:
                        best_fit = (coeffs, err, magnitudes, combo, fit_vec)
                        best_err = err

            if best_fit:
                coeffs, err, magnitudes, combo, fit_vec = best_fit
                for i in range(3):
                    df_new.at[idx,
                              f"VAR{i + 1}_SHIFT{lag}"] = coeffs[i] if i < len(coeffs) else np.nan
                    df_new.at[idx, f"VAR{i + 1}_MAGNITUDE{lag}"] = magnitudes[i] if i < len(
                        magnitudes) else np.nan
                    # Safe string assignment
                    df_new.at[idx, f"VAR{i + 1}_IDENTIFIER{lag}"] = (
                        Xrates_fp.index[combo[i]] if i < len(combo) else np.nan
                    )
                df_new.at[idx, f"ERROR_REL{lag}"] = err
                df_new.at[idx, f"ERROR_MAGNITUDE{lag}"] = np.linalg.norm(
                    obs_vec - fit_vec)

    df_out = pd.concat([df_old, df_new]).sort_index()
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