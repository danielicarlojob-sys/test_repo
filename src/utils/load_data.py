from pprint import pprint as pp
import os
import pandas as pd
from src.utils.read_and_clean_v1 import read_and_clean_csv
from src.utils.log_file import LOG_FILE, log_message


def load_temp_data(
        LOOP_str: str,
        Fleetstore_dir: str = 'Fleetstore_Data') -> dict:
    """
    Function used to load CSV data manually based on the processing stage contained in LOOP_str.

    Parameters
    ----------
    Args:
         - LOOP_str (str): string containing the info for on the CSV to be loaded
         - Fleetstore_dir (str): path to CSV folder

    Returns
    -------
         - data_dict (dict): dictionary containing CSV
    """
    rootdir = os.getcwd()
    Fleetstore_path = os.path.join(rootdir, Fleetstore_dir)

    file_list = [file for file in os.listdir(Fleetstore_path) if LOOP_str.lower(
    ) in file.lower()]  # gets the files containing LOOP_str

    data_dict = {}
    for file in file_list:
        file_path = os.path.join(Fleetstore_path, file)

        df_temp = read_and_clean_csv(file_path)  # reads and clean csv file

        flight_phases = ["take-off", "climb", "cruise"]
        flight_phase = [
            txt for txt in flight_phases if txt.lower() in file.lower()]
        data_dict[flight_phase[0]] = df_temp

    return data_dict


def load_csv_to_df(
        CSV_str: str,
        Fleetstore_dir: str = 'Fleetstore_Data') -> pd.DataFrame:
    """
    Function used to load CSV data manually based on the processing stage contained in CSV_str.

    Parameters
    ----------
    Args:
         - CSV_str (str): string containing the info for the CSV to be loaded
         - Fleetstore_dir (str): path to CSV folder

    Returns
    -------
         - df_previous (pd.DataFrame): pd.DataFrame containing flightphase specific data from previous run
    """
    rootdir = os.getcwd()
    Fleetstore_path = os.path.join(rootdir, Fleetstore_dir)
    # log_message(f"Fleetstore_path from load_csv_to_df: {Fleetstore_path}")
    file_path = os.path.join(Fleetstore_path, CSV_str)
    # log_message(f"file_path from load_csv_to_df: {file_path}")
    df_previous = read_and_clean_csv(file_path)  # reads and clean csv file
    # log_message(f"read_and_clean_csv succeful from {file_path}")
    return df_previous


if __name__ == "__main__":

    project_dir = os.getcwd()
    LOOP_str = "LOOP_5"

    try:
        data_dict = load_temp_data(LOOP_str)
        df = data_dict['cruise']
        df1 = df.copy()
        log_message("df.columns.tolist(): \n")
        # Define log file path
        LOG_FILE = os.path.join(os.getcwd(), "COLUMNS_log.txt")
        with open(LOG_FILE, "a", encoding="utf-8") as f:

            [f.write(f"({idx}, {i})\n") for idx, i in enumerate(df.columns)]

        log_message("Data extraction completed!")
    except Exception as e:

        log_message(f"error fetching data: {e}")
