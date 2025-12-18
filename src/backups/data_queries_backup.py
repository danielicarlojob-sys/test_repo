import pyodbc
from dotenv import load_dotenv
import os
import pandas as pd
import warnings
from datetime import datetime as dt

def var_check(string):
    if string in locals():
        return True
    else:
        return False

def is_datetime(string, format="%Y-%m-%d %H:%M:%S"):
    try:
        dt.strptime(string.strip(),format)
        return True
    except ValueError:
        return False

def start_timestamp_finder():
    start_dir = os.getcwd()
    os.chdir("..")
    current_dir = os.getcwd()
    working_dir_path = current_dir + r'\working_data'
    if not os.path.exists(working_dir_path):
        os.makedirs(working_dir_path)
    timestamp_path = working_dir_path + r'\timestamp.txt'
    if os.path.exists(timestamp_path):
        with open(timestamp_path, 'r', encoding='utf-8') as f:
            timestamp = f.read()
        timestamp_check = is_datetime(timestamp)
        if timestamp_check == True:
            return timestamp
        else:
            timestamp = None
            return timestamp
    else:
        timestamp = None
        return timestamp




def load_and_replace_sql(query_var):
    """
    Load a .sql file and replace %startTimestamp% with a given string,
    or with the default DATEADD function if not provided.

    Parameters:
        file_path (str): Path to the .sql file.
        timestamp_str (str, optional): The string to replace %startTimestamp%.

    Returns:
        str: Modified SQL query as a string.
    """
    timestamp_str = start_timestamp_finder()
    
    try:
        
        replacement = timestamp_str or "DATEADD(DAY, -14, GETDATE())"
        if timestamp_str is not None:
            replacement = f"'{replacement}'"
        modified_query = query_var.replace("%startTimestamp%", replacement)
        
        return modified_query

    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {query_var}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while processing the file: {e}")

load_dotenv(override=True)

def connect_to_db():
    '''
    Function to connect to fleetstore by fetching credentails stored in .env file
    '''
    user = os.environ.get("PG_USER")
    password = os.environ.get("PG_PASSWORD")
    database = os.environ.get("PG_DATABASE")
    server = os.environ.get("PG_SERVER") 
    driver = os.environ.get("PG_DRIVER")

      
    connectionString = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password};Authentication=ActiveDirectoryPassword'
    conn = pyodbc.connect(connectionString) 
    print('in conn function')
    return conn

def close_connection(conn):
    '''
    Function to close connection to database
    '''
    conn.close()
    print("connection closed")

def execute_query_from_file(sql_file, conn):
    '''
    Function to execute a query from a .sql file
    '''
    try:
        # Read the SQL query from the file
        queries_folder = os.getcwd() + r"\Queries"
        os.chdir(queries_folder)
        with open(sql_file, "r") as file:
            query = file.read()
            query = load_and_replace_sql(query)
            
        # Fetch data from Fleet store and store it in a dataframe
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            data = pd.read_sql_query(query, conn)
        print(f"        {sql_file} executed successfully!")

        return data
    except Exception as e:
        print(f"Error executing {sql_file}: {e}")
        return None



def fetch_all_flight_phase_data(path):
    print("Start Data extraction")
    sql_files_path = os.path.join(path,'Queries')

    conn = connect_to_db()
    for file in os.listdir(sql_files_path):

        if file.endswith('CRZ.sql'):
            data = execute_query_from_file(file, conn)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_cruise.csv')
                os.chdir("..")

        elif file.endswith('CLM.sql'):
            data = execute_query_from_file(file, conn)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_climb.csv')
                os.chdir("..")
                
        elif file.endswith('TKO.sql'):
            data = execute_query_from_file(file, conn)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_take-off.csv')
                os.chdir("..")
        
    # Close database connection
    close_connection(conn)


import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

#fetch_all_flight_phase_data()

if __name__  == "__main__":
    
    project_dir = os.getcwd()

    try:
        fetch_all_flight_phase_data(project_dir)
        print("Data extraction completed!") 
    except Exception as e:
        logger.error(f"error fetching data: {e}")
        print(f"error fetching data: {e}")
