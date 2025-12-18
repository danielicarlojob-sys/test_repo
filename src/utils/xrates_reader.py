# previously named Read_Perf_Exchange_Rates_engine_type_specific
from typing import Union
import pandas as pd
import numpy as np
import os
from pprint import pprint as pp
def Xrates_reader(d: int,
                  file: str = 'T1000 Pack B Xrates General Version 1_plus_one_extra_line.xlsx') -> Union[str,
                                                                                                         pd.DataFrame]:
    """
    Reads Xrates' data from file for each flight phase and stores the data in a pd.DataFrame
    Parameters
    ----------
    Args:
         - d (int): integer values in range(len(DSC))
         - file (str): file name containing Xrates values
    Returns
    -------
         - flight_phase (str): string containing flight phase
         - df (pd.DataFrame): DataFrame containg Xrates for the aforementioned flight phase
    """
    DSC = [52, 53, 54]  # Example DSC array
    # Determine the sheet and rows to read
    if DSC[d] == 52:
        sheet = 'Cruise Solver Input Sheet'
        Row = [3, 4, 6, 7, 8, 13, 14, 22, 29, 33, 35, 36, 41, 56, 61, 62,
               66, 73, 74, 75, 76, 78, 79, 80, 82, 83, 84, 85, 86, 87, 88, 94]
        rows_to_keep_idx = [2,3,5,6,7,12,13,21,28,32,34,35,40,55,60,61,65,72,73,
                            74,75,77,78,79,81,82,83,84,85,86,87,93]
        rows_to_keep = [
            'IPC ETA',
            'HPC ETA',
            'HPT ETA',
            'IPT ETA',
            'LPT ETA',
            'HPT CAPACITY',
            'IPT CAPACITY',
            'DPP COMB',
            'BI8435Q (IP8 Bleed to IPT Rear)',
            'BH326Q (HP3 Bleed to IPC Rear)',
            'BH342Q (HP3 Bleed to IP NGV Throat)',
            'BH3435Q (HP3 Bleed to IPT Rear)',
            'BH642Q (HP6 Bleed to IP NGV Throat)',
            'ESSAI switched from off to on',
            'SAS valve switch from OFF to ON',
            'plus 1 DEG VSV',
            '1% increase in P20 by simulating a MN error',
            'HP TCC flow',
            'IP TCC flow',
            'LP TCC flow',
            'TPR (representing measurment error) + 1%',
            '1% T20 (inc TPR)',
            'TGT',
            'P30',
            'WFE +1%',
            'PCNL +1%',
            'PCNI +1%',
            'PCNH +1%',
            'P26 +1%',
            'T26 +1%',
            'T30 +1%',
            'Additional shift (not IPC or HPC damage)']

        flight_phase = 'Cruise'
    elif DSC[d] == 53:
        sheet = 'Takeoff Solver Input Sheet'
        Row = [3, 4, 6, 7, 8, 13, 14, 22, 29, 33, 35, 36, 41, 58, 63, 64,
               68, 69, 70, 71, 72, 74, 75, 76, 78, 79, 80, 81, 82, 83, 84, 90]
        rows_to_keep_idx = [2,3,5,6,7,12,13,21,28,32,34,35,40,57,62,63,67,68,69,
            70,71,73,74,75,77,78,79,80,81,82,83,89]
        rows_to_keep = [
            'IPC ETA',
            'HPC ETA',
            'HPT ETA',
            'IPT ETA',
            'LPT ETA',
            'HPT CAPACITY',
            'IPT CAPACITY',
            'DPP COMB',
            'BI8435Q (IP8 Bleed to IPT Rear)',
            'BH326Q (HP3 Bleed to IPC Rear)',
            'BH342Q (HP3 Bleed to IP NGV Throat)',
            'BH3435Q (HP3 Bleed to IPT Rear)',
            'BH642Q (HP6 Bleed to IP NGV Throat)',
            'ESSAI switched from off to on',
            'SAS valve switch from OFF to ON',
            'plus 1 DEG VSV',
            '1% increase in P20 by simulating a MN error',
            'HP TCC flow',
            'IP TCC flow',
            'LP TCC flow',
            'TPR (representing measurment error)',
            '1% T20 (inc TPR)',
            'TGT',
            'P30',
            'WFE +1%',
            'PCNL +1%',
            'PCNI +1%',
            'PCNH +1%',
            'P26 +1%',
            'T26 +1%',
            'T30 +1%',
            'Additional shift (not IPC or HPC damage)']
        flight_phase = 'Take-off'
    elif DSC[d] == 54:
        sheet = 'Climb Solver Input Sheet'
        Row = [3, 4, 6, 7, 8, 13, 14, 22, 29, 33, 35, 36, 41, 58, 63, 64,
               68, 69, 70, 71, 72, 74, 75, 76, 78, 79, 80, 81, 82, 83, 84, 90]
        rows_to_keep_idx = [2,3,5,6,7,12,13,21,28,32,34,35,40,57,62,63,67,68,69,
            70,71,73,74,75,77,78,79,80,81,82,83,89]
        rows_to_keep = [
            'IPC ETA',
            'HPC ETA',
            'HPT ETA',
            'IPT ETA',
            'LPT ETA',
            'HPT CAPACITY',
            'IPT CAPACITY',
            'DPP COMB',
            'BI8435Q (IP8 Bleed to IPT Rear)',
            'BH326Q (HP3 Bleed to IPC Rear)',
            'BH342Q (HP3 Bleed to IP NGV Throat)',
            'BH3435Q (HP3 Bleed to IPT Rear)',
            'BH642Q (HP6 Bleed to IP NGV Throat)',
            'ESSAI switched from off to on',
            'SAS valve switch from OFF to ON',
            'plus 1 DEG VSV',
            '1% increase in P20 by simulating a MN error',
            'HP TCC flow',
            'IP TCC flow',
            'LP TCC flow',
            'TPR (representing measurment error)',
            '1% T20 (inc TPR)',
            'TGT',
            'P30',
            'WFE +1%',
            'PCNL +1%',
            'PCNI +1%',
            'PCNH +1%',
            'P26 +1%',
            'T26 +1%',
            'T30 +1%',
            'Additional shift (not IPC or HPC damage)']
        flight_phase = 'Climb'
    else:
        raise ValueError('Unknown DSC value')
    # Read the data
    src_dir_path = os.path.join(os.getcwd(), "src")
    working_dir_path = os.path.join(src_dir_path, "utils")
    file_path = os.path.join(working_dir_path, file)
    
    # Read the data (numeric columns only, exclude A)
    df = pd.read_excel(
        file_path,
        sheet_name=sheet,
        header=None,
        usecols="B:R",  # numeric columns
        skiprows=3,
        nrows=94
    )
    df = df.loc[rows_to_keep_idx]  # subset while index is still integer
    #print(f"line 162 df:\n {df}")
    # Column headers
    df_cols_headers = pd.read_excel(
        file_path,
        sheet_name=sheet,
        header=None,
        usecols="B:R",
        skiprows=2,
        nrows=1
    ).values.tolist()[0]
    #print(f"line 172 df_cols_headers:\n {df_cols_headers}")
    # Row headers
    df_rows_headers = pd.read_excel(
        file_path,
        sheet_name=sheet,
        header=None,
        usecols="A",
        skiprows=3,
        nrows=94
    ).values.flatten().tolist()
    df_rows_headers = [df_rows_headers[i] for i in rows_to_keep_idx]
    #print(f"line 183 df_rows_headers:\n {df_rows_headers}")
    # Assign headers and index
    df.columns = df_cols_headers
    df.index = df_rows_headers
    #print(f"line 187 df:\n {df}")

    # Convert to float
    df = df.astype(float)

    # Drop first 4 numeric columns
    df = df.iloc[:, 3:]

    # Scale
    df = df * 100

    pd.set_option("display.precision", 8)
    #print(f"line 1199 df:\n {df}")

    return flight_phase, df

if __name__ == "__main__":
    try:
        fp, df = Xrates_reader(d = 0)
        df.to_csv('Xrates_temp_Cruise_test.csv')
    except Exception as e:
        print(f"error: {e}")
