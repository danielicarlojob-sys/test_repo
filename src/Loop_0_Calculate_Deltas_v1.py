import os
import pandas as pd
from src.utils.log_file import log_message

def Loop_0_delta_calc(
    df: pd.DataFrame,
    flight_phase: str = None,
    n: int = 5,
    DebugOption: int = 1
) -> pd.DataFrame:
    """
    Function computes the percentage deltas of actual engine parameters from their nominal
    (baseline) values.

    Parameters
    ----------
     - df : pd.DataFrame, Input DataFrame containing the following columns:
            - 'ESN', 'reportdatetime', 'ACID', 'ENGPOS'
            - Sensor readings (e.g., 'P25__PSI', 'T25__DEGC', etc.)
            - Corresponding nominal values (e.g., 'PS26S__NOM_PSI', 'TS25S__NOM_K', etc.)
            - Other metadata: 'datestored', 'operator', 'equipmentid', 'DSCID'
     - flight_phase: str, string indicating flight phase
     - n: int, (default = 5) decimal digit to round
     - DebugOption : int, optional (default = 1), switch to create a copy of the output data in csv format.

    Returns
    -------
     - df_conc : pd.DataFrame, containig data and calculated deltas percentage.

    """
    # Define function's dysplay name
    Loop_0_delta_calc.display_name = "LOOP 0 - DELTA CALCULATION"
    
    #create df copy to avoid warnings
    df_out = df.copy()
    
    # Split the dataframe between old and new data
    df_new = df_out[df_out['NEW_FLAG']==1].copy()
    df_old = df_out[df_out['NEW_FLAG']==0]

    # Compute deltas
    df_new['PS26__DEL_PC'] = round(
        (df_new['P25__PSI'] - df_new['PS26S__NOM_PSI']) * 100 / df_new['PS26S__NOM_PSI'], n)
    df_new['T25__DEL_PC'] = round(
        (df_new['T25__DEGC'] - df_new['TS25S__NOM_K']) * 100 / df_new['TS25S__NOM_K'], n)
    df_new['P30__DEL_PC'] = round(
        (df_new['P30__PSI'] - df_new['PS30S__NOM_PSI']) * 100 / df_new['PS30S__NOM_PSI'], n)
    df_new['T30__DEL_PC'] = round(
        (df_new['T30__DEGC'] - df_new['TS30S__NOM_K']) * 100 / df_new['TS30S__NOM_K'], n)
    df_new['TGTU__DEL_PC'] = round(
        (df_new['TGTU_A__DEGC'] - df_new['TGTS__NOM_K']) * 100 / df_new['TGTS__NOM_K'], n)
    df_new['NL__DEL_PC'] = round(
        (df_new['NL__PC'] - df_new['NL__NOM_PC']) * 100 / df_new['NL__NOM_PC'], n)
    df_new['NI__DEL_PC'] = round(
        (df_new['NI__PC'] - df_new['NI__NOM_PC']) * 100 / df_new['NI__NOM_PC'], n)
    df_new['NH__DEL_PC'] = round(
        (df_new['NH__PC'] - df_new['NH__NOM_PC']) * 100 / df_new['NH__NOM_PC'], n)
    df_new['FF__DEL_PC'] = round(
        (df_new['FF__LBHR'] - df_new['FF__NOM_LBHR']) * 100 / df_new['FF__NOM_LBHR'], n)
    df_new['P160__DEL_PC'] = round(
        (df_new['PS160__PSI'] - df_new['P135S__NOM_PSI']) *100 /df_new['P135S__NOM_PSI'], n)
    
    # Concatenate back old and new data
    df_conc = pd.concat([df_old, df_new], ignore_index=True)
    # Remove duplicates
    df_conc = df_conc.sort_values(
        by='reportdatetime',
        ascending=True).drop_duplicates(keep='last')

    # Saves function output to CSV file
    if DebugOption == 1:
        dtypes_txt = False
        if dtypes_txt == True:
            with open(f'Loop_0_{flight_phase}_input_df_dtypes.txt', 'w') as f:
                f.write(str(df_out.dtypes.to_string()))
            with open(f'Loop_0_{flight_phase}_output_df_dtypes.txt', 'w') as f:
                f.write(str(df_conc.dtypes.to_string()))
        # Get current dir and Fleetstore_Data dir
        current_dir = os.getcwd()
        FleetStore_dir = os.path.join(current_dir, "Fleetstore_Data")
        if not os.path.exists(FleetStore_dir):
            os.makedirs(FleetStore_dir)
        # Save a temporary CSV file for debugging or traceability
        path_temp = os.path.join(FleetStore_dir, f"LOOP_0_{flight_phase}.csv")
        log_message(f"        File saved to: {path_temp}")
        df_conc.to_csv(path_temp)

    return df_conc


if __name__ == "__main__":
    df = pd.DataFrame(columns=["ESN", "P25__PSI", "PS26S__NOM_PSI"])
    df_out = Loop_0_delta_calc(df, flight_phase="Cruise", DebugOption=0)

"""
if __name__  == "__main__":

    root_dir = os.getcwd()
    Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')
try:
    data_dict = fetch_all_flight_phase_data(root_dir)
    log_message("Manual run - fetch_all_flight_phase_data completed!")

    flight_phase = 'cruise'
    #log_message(f"Manual run - line 132 - (data_dict.items():{data_dict.items()}")
    #log_message(f"Manual run - line 133 - flight_phase: {flight_phase}")
    # LOOP 0 - DELTA CALCULATION
    #log_message("Manual run - line135 - Start data processing")
    log_message(f"Manual run - line136  - Start LOOP 0  - DELTA CALCULATION")
    #log_message(f"Manual run - line137  - (data_dict[{flight_phase}]:{data_dict[flight_phase]}")
    for fp in list(data_dict.keys()):
        df_temp = Loop_0_delta_calc(data_dict[fp], fp)
    # asyncio.run(async_main(data_dict, Fleetstore_data_dir,Loop_0_delta_calc))

    log_message(f"Manual run - Completed LOOP 0 - DELTA CALCULATION")

except Exception as e:

    log_message(f"Manual run - line 146 - error fetching data: {e}")
    """
