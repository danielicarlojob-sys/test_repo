import pyodbc
from typing import Union
from dotenv import load_dotenv
import os
import pandas as pd
import warnings
from datetime import datetime as dt
from src.utils.filter_latest_n_pts_per_esn import filter_latest_n_pts_per_esn
from src.utils.log_file import LOG_FILE, log_message
from backups.df_merger import df_merger




# Load credentials to access Fleetstore
load_dotenv(override=True)


def is_datetime(string: str, format: str = "%Y-%m-%d %H:%M:%S") -> bool:
    """
    Checks if a given string can be parsed into a datetime object using the specified format.
    
    Parameters
    ----------
    Args:
        - string (str): The string to check for datetime compatibility.
        - format (str, optional): The datetime format to use for parsing. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns
    -------
        - bool: True if the string matches the datetime format, False otherwise.
    """
    try:
        dt.strptime(string.strip(),format)
        return True
    except ValueError:
        return False

def start_timestamp_finder() -> Union[str, None]:
    """
    Finds and returns a valid timestamp from a file in the 'working_data' directory.

    This function navigates to the parent directory, ensures the existence of a 'working_data' folder,
    and checks for a 'timestamp.txt' file within it. If the file exists, it reads its contents and
    validates whether the content is a proper datetime using the `is_datetime` function. If valid,
    the timestamp string is returned; otherwise, or if the file does not exist, None is returned.

    Returns
    -------
        - str or None: The valid timestamp string if found and valid, otherwise None.
    """

    current_dir = os.getcwd()
    working_dir_path = os.path.join(current_dir, "working_data")
    if not os.path.exists(working_dir_path):
        os.makedirs(working_dir_path)
    timestamp_path = os.path.join(working_dir_path, "timestamp.txt")
    if os.path.exists(timestamp_path):
        with open(timestamp_path, "r", encoding="utf-8") as f:
            timestamp = f.read()
        if is_datetime(timestamp):
            return timestamp
    return None

def load_and_replace_sql(query_var: str) -> Union[str, str]:
    """
    Replaces the '%startTimestamp%' placeholder in a SQL query string with a timestamp value.
    If a timestamp is found using `start_timestamp_finder()`, it is used (and wrapped in single quotes).
    Otherwise, the default SQL expression "DATEADD(DAY, -14, GETDATE())" is used as the replacement.

    Parameters
    ----------
    Args:
        - query_var (str): The SQL query string containing the '%startTimestamp%' placeholder.
    
    Returns
    -------
        - modified_query, timestamp_str (, str): The modified SQL query string with the placeholder replaced' the inital timestamp
    
    Raises
    ------
        - FileNotFoundError: If the input file is not found (though this is not directly used in this function).
        RuntimeError: If any other error occurs during processing.
    """
    try:
        timestamp_str = start_timestamp_finder()
        replacement = timestamp_str or "DATEADD(DAY, -14, GETDATE())"
        if timestamp_str is not None:
            replacement = f"'{replacement}'"
        modified_query = query_var.replace("%startTimestamp%", replacement)
        """        
        LOG_FILE = os.path.join(os.getcwd(), f"{query_var}_log.txt")
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"(modified query {query_var}:\n {modified_query}\n")"""
        return modified_query, timestamp_str
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {query_var}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while processing the file: {e}")

def connect_to_db() -> pyodbc.Connection:
    '''
    Function to connect to fleetstore by fetching credentails stored in .env file

    Returns
    -------
        - conn (pyodbc.Connection): connection to SQL database
    '''
    user = os.environ.get("PG_USER")
    password = os.environ.get("PG_PASSWORD")
    database = os.environ.get("PG_DATABASE")
    server = os.environ.get("PG_SERVER") 
    driver = os.environ.get("PG_DRIVER")

      
    connectionString = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password};Authentication=ActiveDirectoryPassword'
    conn = pyodbc.connect(connectionString) 
    log_message('in conn function')
    return conn

def close_connection(conn: pyodbc.Connection):
    '''
    Function to close connection to database

    Parameters
    ----------
    Args:
        - conn (pyodbc.Connection): connection to database
    '''
    conn.close()
    log_message("connection closed")

def execute_query_from_file(sql_file: str, conn: pyodbc.Connection) -> Union[pd.DataFrame, str, None]:
    """
    Executes a SQL query from a specified file against a given database connection.
    This function reads a SQL query from a file, optionally processes the query using
    the `load_and_replace_sql` function, and executes it using the provided database
    connection. The results are returned as a pandas DataFrame. Any warnings during
    query execution are suppressed. If an error occurs, it is printed and None is returned.

    Parameters
    ----------
    Args:
        - sql_file (str): The path to the SQL file containing the query to execute.
        - conn (sqlite3.Connection or similar): The database connection object.
    
    Returns
    -------
        - (pandas.DataFrame, str) or None: The result of the query as a DataFrame along with the initial timestamp, or None if an error occurs.
    """

    try:
        # Read the SQL query from the file
        queries_folder = os.path.join(os.getcwd(), "Queries")
        with open(sql_file, "r") as file:
            query = file.read()
            query, timestamp_str_initial = load_and_replace_sql(query)
            
        # Fetch data from Fleet store and store it in a dataframe
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = pd.read_sql_query(query, conn)
        log_message(f"        {sql_file} executed successfully!")

        return data, timestamp_str_initial
    except Exception as e:
        log_message(f"Error executing {sql_file}: {e}")
        return None

def execute_query_from_file2(sql_file_path: str, conn: pyodbc.Connection) -> Union[pd.DataFrame, str, None]:
    """
    Executes a SQL query from a specified file against a given database connection.
    This function reads a SQL query from a file, optionally processes the query using
    the `load_and_replace_sql` function, and executes it using the provided database
    connection. The results are returned as a pandas DataFrame. Any warnings during
    query execution are suppressed. If an error occurs, it is printed and None is returned.

    Parameters
    ----------
    Args:
        - sql_file (str): The path to the SQL file containing the query to execute.
        - conn (sqlite3.Connection or similar): The database connection object.
    
    Returns
    -------
        - (pandas.DataFrame, str) or None: The result of the query as a DataFrame along with the initial timestamp, or None if an error occurs.
    """

    try:

        # Read the SQL query from the file
        with open(sql_file_path, "r") as file:
            query = file.read()
            query, timestamp_str_initial = load_and_replace_sql(query)
            
        # Fetch data from Fleet store and store it in a dataframe
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = pd.read_sql_query(query, conn)
        log_message(f"        {os.path.basename(sql_file_path)} executed successfully!")

        return data, timestamp_str_initial
    except Exception as e:
        log_message(f"Error executing {os.path.basename(sql_file_path)}: {e}")
        return None

def fetch_all_flight_phase_data(path: str) -> pd.DataFrame:

    """
    Extracts flight phase data from SQL query files and saves them as CSV files.
    This function searches for SQL files corresponding to different flight phases
    (CRZ, CLM, TKO) in the 'Queries' subdirectory of the given path. For each
    matching file, it executes the SQL query using a database connection, and if
    the result is not empty, saves the data as a CSV file in the 'Fleetstore_Data'
    subdirectory. The database connection is closed after processing all files.
    
    Parameters
    ----------
    Args:
        - path (str): The base directory path containing 'Queries' and 'Fleetstore_Data' subdirectories.
    
    Raises
    ------
        - Any exceptions raised by database connection, query execution, or file operations.
    
    Side Effects
    ------------
        - Writes CSV files ('data_cruise.csv', 'data_climb.csv', 'data_take-off.csv') to the 'Fleetstore_Data' directory.
        Changes the current working directory temporarily during file writing.
    
    Returns
    -------
        - pd.DataFrame: dataframe containing queries output
    """
    log_message("Start Data extraction")
    root_dir = os.getcwd()
    data_dir = os.path.join(root_dir,"Fleetstore_Data")
    sql_files_path = os.path.join(path,'Queries')

    conn = connect_to_db()
    keys = ['cruise', 'climb', 'take-off']
    data_dict = {key: None for key in keys}
    timestamps = []
    for file in os.listdir(sql_files_path):
        if file.endswith('CRZ.sql'):
            flight_phase = 'Cruise'
            SQL_file_path = os.path.join(sql_files_path, file)
            data, timestamp_str_initial = execute_query_from_file(SQL_file_path, conn)
            if not data.empty:
                latest_ts = data['reportdatetime'].max() # extract latest timestamp from the query just run
                timestamps.append(latest_ts) # append latest_ts to timestamps list
                data = df_merger(data, flight_phase) # Merge historical data from previous run with query output
                file_name = 'data_cruise.csv'
                file_path = os.path.join(data_dir,file_name)
                data = filter_latest_n_pts_per_esn(data, file_path, flight_phase, timestamp_str_initial)
                data_dict['cruise'] = data
                log_message(f"data from {file} extracted")
                

        elif file.endswith('CLM.sql'):
            flight_phase = 'Climb'
            SQL_file_path = os.path.join(sql_files_path, file)
            data, timestamp_str_initial = execute_query_from_file(SQL_file_path, conn)
            if not data.empty:
                latest_ts = data['reportdatetime'].max() # extract latest timestamp from the query just run
                timestamps.append(latest_ts) # append latest_ts to timestamps list
                data = df_merger(data, flight_phase) # Merge historical data from previous run with query output
                file_name = 'data_climb.csv'
                file_path = os.path.join(data_dir,file_name)
                data = filter_latest_n_pts_per_esn(data, file_path, flight_phase, timestamp_str_initial)
                data_dict['climb'] = data
                log_message(f"data from {file} extracted")
                
                
        elif file.endswith('TKO.sql'):
            flight_phase = 'Take-off'
            SQL_file_path = os.path.join(sql_files_path, file)
            data, timestamp_str_initial = execute_query_from_file(SQL_file_path, conn)
            if not data.empty:
                latest_ts = data['reportdatetime'].max() # extract latest timestamp from the query just run
                timestamps.append(latest_ts) # append latest_ts to timestamps list
                data = df_merger(data, flight_phase) # Merge historical data from previous run with query output
                file_name = 'data_take-off.csv'
                file_path = os.path.join(data_dir,file_name)
                data = filter_latest_n_pts_per_esn(data, file_path, flight_phase, timestamp_str_initial)
                data_dict['take-off'] = data
                log_message(f"data from {file} extracted")
    
    
    # Close database connection
    close_connection(conn)
    # Write tmstp to working_data\timestamp.txt (overwrite if exists)
    tmstp = min(timestamps)
    output_txt = os.path.join(root_dir,"working_data")
    output_txt = os.path.join(output_txt,"timestamp.txt")




    with open(output_txt, 'w', encoding='utf-8') as f_out:
        f_out.write(tmstp.strftime('%Y-%m-%d %H:%M:%S'))
    log_message(f" timestamp.txt updated from {timestamp_str_initial} to {tmstp}")
    return data_dict






#fetch_all_flight_phase_data()

if __name__  == "__main__":
    
    project_dir = os.getcwd()

    try:
        fetch_all_flight_phase_data(project_dir)
        log_message("Data extraction completed!") 
    except Exception as e:

        log_message(f"error fetching data: {e}")

