import os
import pandas as pd
import pyodbc
from datetime import datetime as dt
import warnings
from typing import Union
from dotenv import load_dotenv
from src.utils.log_file import LOG_FILE, log_message, debug_info, f_lineno as line
from src.utils.df_merger_new import df_merger_new
from src.utils.filter_latest_n_pts_per_esn import filter_latest_n_pts_per_esn


# Load credentials to access Fleetstore
load_dotenv(override=True)


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
        dt.strptime(string.strip(), format)
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
        if timestamp_str == None:
            replacement = "DATEADD(DAY, -14, GETDATE())"
        else:
            replacement = f"'{replacement}'"
        """
        replacement = timestamp_str or "DATEADD(DAY, -14, GETDATE())"
        if timestamp_str is not None:
            replacement = f"'{replacement}'"
        """
        modified_query = query_var.replace("%startTimestamp%", replacement)

        return modified_query, timestamp_str
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {query_var}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while processing the file: {e}")


def execute_query_from_file(
        sql_file_path: str, conn: pyodbc.Connection) -> Union[pd.DataFrame, str, None]:
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
        - data (pandas.DataFrame, str) or None: The result of the query as a DataFrame along with the initial timestamp, or None if an error occurs.
        - timestamp_str_initial (str): timestamp indicating the start o the query data extraction in format = '%Y-%M-%d %H:%M:%s'
    """

    try:

        # Read the SQL query from the file
        with open(sql_file_path, "r") as file:
            query = file.read()
            query, timestamp_str_initial = load_and_replace_sql(query)

        # Fetch data from Fleet store and store it in a dataframe
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
        """
        data = pd.read_sql_query(query, conn)
        log_message(f"{os.path.basename(sql_file_path)} executed successfully!")

        return data, timestamp_str_initial
    except Exception as e:
        log_message(f"Error executing {os.path.basename(sql_file_path)}: {e}")
        return None

def query_run(file_name: str, flight_phase: str, timestamp_container: list,
              query_folder: str = 'Queries') -> tuple[pd.DataFrame, str, list]:
    """RUN SINGLE SQL QUERY, file_name, IN query_folder for a specific flight phases,
     saves the initial_timestamp for the query run and appends the last timestamp in timestamp_container

     Parameters:
     -----------
     Args:
        - file_name: str, name of the file containing the SQL query.
        - flight_phase: str, string for flight phase.
        - timestamp_container: list, list of timestamp needed for the next run of the whole script.
        - query_folder: str, folder containing all SQL queries files.

    Return:
    -------
        - data: pd.DataFrame,
        - timestamp_str_initial: str,
        - timestamp_container: list,

     """
    try:
        file_path = os.path.join(query_folder, file_name)
        conn = connect_to_db()
        data, timestamp_str_initial = execute_query_from_file(file_path, conn)
        close_connection(conn)

        if not data.empty:
            # extract latest timestamp from the query just run
            latest_ts = data['reportdatetime'].max()
            timestamp_container.append(latest_ts)
            
            """
                """
            #  QUERY OUTPUT TEMP DATA SAVE
            x = dt.now()
            timenow = x.strftime("%Y-%m-%d %H-%M-%S")
            data = df_merger_new(data, flight_phase)
            data = filter_latest_n_pts_per_esn(
                data, flight_phase, timestamp_str_initial)

        return data, timestamp_str_initial, timestamp_container
    except Exception as e:
        log_message(
            f"        ERROR in {debug_info()} for flight phase:{flight_phase} - {str(e)}")


def data_ingestion(root_dir: str = os.getcwd()) -> dict:
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
        - root_dir (str): The base directory path containing 'Queries' and 'Fleetstore_Data' subdirectories.

    Raises
    ------
        - Any exceptions raised by database connection, query execution, or file operations.

    Side Effects
    ------------
        - Writes CSV files ('data_cruise.csv', 'data_climb.csv', 'data_take-off.csv') to the 'Fleetstore_Data' directory.
        Changes the current working directory temporarily during file writing.

    Returns
    -------
        - dataa_dict: dict, containing pd.DataFrames for each fligth phase.
    """
    log_message("Start Data extraction")

    query_folder = os.path.join(root_dir, 'Queries')
    SQL_queries = os.listdir(query_folder)
    keys = ['cruise', 'climb', 'take-off']
    data_dict = {key: None for key in keys}
    timestamps = []
    flight_phases = {
        'CRZ.sql': 'Cruise',
        'CLM.sql': 'Climb',
        'TKO.sql': 'Take-off'}
    for SQL_query in SQL_queries:
        flight_phase = flight_phases[SQL_query[-7:]]
        data, timestamp_str_initial, timestamps = query_run(
            SQL_query, flight_phase, timestamps)
        data_dict[flight_phase.lower()] = data
        
    # Write tmstp to working_data\timestamp.txt (overwrite if exists)
    tmstp = min(timestamps)
    output_txt = os.path.join(root_dir, "working_data")
    output_txt = os.path.join(output_txt, "timestamp.txt")
    with open(output_txt, 'w', encoding='utf-8') as f_out:
        f_out.write(tmstp.strftime('%Y-%m-%d %H:%M:%S'))
    log_message(
        f" timestamp.txt updated from {timestamp_str_initial} to {tmstp}")
    return data_dict

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus


def connect_to_db_sqlalchemy(connection_info: bool = False) -> Engine:
    '''
    Function to connect to fleetstore by fetching credentails stored in .env file

    Returns
    -------
        - conn (Engine): connection to SQL database
    '''
    from urllib.parse import quote_plus
    # Get environment variables and strip any surrounding single quotes
    
    user = os.environ.get("PG_USER", "").strip("'")
    password = os.environ.get("PG_PASSWORD", "").strip("'")
    database = os.environ.get("PG_DATABASE", "").strip("'")
    server = os.environ.get("PG_SERVER", "").strip("'")
    driver = os.environ.get("PG_DRIVER", "").strip("'") # e.g., {ODBC Driver 17 for SQL Server}

    # Build ODBC connection string manually
    odbc_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Authentication=ActiveDirectoryPassword"
    )

    
    connection_str = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
    conn = create_engine(connection_str)
    log_message('SQLALCHEMY connection opened')
    if connection_info ==True:
        log_message(f"odbc_str:\n{odbc_str}")
        log_message(f"connection_str:\n{connection_str}")
        log_message(f'in SQLALCHEMY conn function:\n{connection_str}')
        

    return conn
# ==============================
# Script entry point for testing
# ==============================

if __name__ == "__main__":

    try:
        """
        data_dict = data_ingestion()
        log_message("Data extraction completed!")
        for f_p in data_dict.keys():
            path_temp = os.path.join(os.getcwd(), "Fleetstore_Data", f"data_ingestion_output_{f_p}.csv")
            data_dict[f_p].to_csv(path_temp, index=False)
            log_message(f"Data ingestion file saved to: {path_temp}")
        """
        file_path = os.path.join(os.getcwd(), 'Queries', 'Sql_fleetstore_CRZ.sql')
        """
        conn = connect_to_db()
        data, timestamp_str_initial = execute_query_from_file(file_path, conn)
        close_connection(conn)
        """

        conn = connect_to_db_sqlalchemy()
        # Read the SQL query from the file
        with open(file_path, "r") as file:
            query = file.read()
            query, timestamp_str_initial = load_and_replace_sql(query)
        # print(query)
        data = pd.read_sql_query(query, conn)
        conn.dispose()
        log_message('SQLALCHEMY connection closed')
        print("data:\n",data)
    except Exception as e:

        log_message(f"error fetching data: {e}")
