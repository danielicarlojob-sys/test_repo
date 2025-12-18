import os
import pandas as pd
import numpy as np
from src.utils.log_file import log_message

def Loop_8(df: pd.DataFrame,
    lag_list: list[int] = [50, 100, 200, 400],
    lim: dict = {   
        'RelErrThresh': [0.3],
        'lim': 0.07,
        'nEtaThresh': 6,
        'nLag': 4,
        'nRelErrThresh': 1,
        'num': 3},
    save_csv: bool = True,
    flight_phase: str = "default",
    ) -> pd.DataFrame:
    """
    Loop_8 documentation in Google format goes here.
    """
    # Calculation from original MATLAB SCRIPT (might be unneccessary)
    nCdur = 3 + 2*(2 + lim['nEtaThresh'])*lim['nRelErrThresh']

    # Make a copy so the original DataFrame is not modified in place
    df = df.copy()

    # Split into two groups:
    df_new = df[df['NEW_FLAG'] == 1].copy()
    df_old = df[df['NEW_FLAG'] == 0]
    esn_with_new_data = df_new["ESN"].unique()
    ########################################################################
    # LOOP 8 cycle entry point 
    ########################################################################
    loop_8_list_merged = []
    rows = 0
    for esn in esn_with_new_data:
        df_temp = df_new[df_new['ESN']==esn].copy().reset_index(drop=True)
        indexes_temp = list(df_temp.index)
        for i in indexes_temp:
            for lag in lag_list:
                RangeStart = max(i - lag, )

    # Merge updated new rows with old rows and restore original row order
    df_final = pd.concat([df_old, df_new]).sort_index()

        # Optionally save results to CSV
    if save_csv:
        
        path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"LOOP_8_{flight_phase}.csv")
        df_final.to_csv(path_temp, index=False)
        log_message(f"File saved to: {path_temp}")
    
    return df