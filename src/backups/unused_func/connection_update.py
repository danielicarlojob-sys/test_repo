
# src/connection_fetch_db.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(override=True)

def get_db_engine():
    """
    Create and return a SQLAlchemy engine using credentials from the .env file
    """
    user = os.environ.get("PG_USER")
    password = os.environ.get("PG_PASSWORD")
    database = os.environ.get("PG_DATABASE")
    server = os.environ.get("PG_SERVER")
    driver = os.environ.get("PG_DRIVER") or "ODBC Driver 17 for SQL Server"

    # Format for SQLAlchemy + pyodbc connection string
    connection_string = (
        f"mssql+pyodbc://{user}:{password}@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}&Authentication=ActiveDirectoryPassword"
    )

    print(f"Connecting to: SERVER={server}, DATABASE={database}")
    engine = create_engine(connection_string, fast_executemany=True)
    return engine

def execute_query_from_file(sql_file_path, engine):
    """
    Execute a SQL query from a file and return the result as a pandas DataFrame
    """
    try:
        os.chdir(r'C:\Users\u614867\Documents\python\fleetstore\queries')
        with open(sql_file_path, "r") as file:
            query = file.read()

        with engine.connect() as connection:
            data = pd.read_sql_query(text(query), con=connection)
            print("     Query executed successfully!")
            return data

    except Exception as e:
        print(f"Error executing the query: {e}")
        return None

def fetch_all_flight_phase_data(path, engine):
    """
    Read all .sql files from path/Queries, execute them, and save results to CSV
    """
    sql_file_path = os.path.join(path, 'Queries')
    output_dir = os.path.join(path, 'Fleetstore_Data')
    os.makedirs(output_dir, exist_ok=True)

    for file in os.listdir(sql_file_path):
        if file.endswith(('.CRZ.sql', '.CLM.sql', '.FS.sql', '.SS.sql', '.TKO.sql')):
            print(f"Processing: {file}")
            data = execute_query_from_file(os.path.join(sql_file_path, file), engine)
            if data is not None and not data.empty:
                filename = f"data_{file.split('.')[0].lower()}.csv"
                output_path = os.path.join(output_dir, filename)
                data.to_csv(output_path, index=False)
                print(f"Saved to {output_path}")

if __name__ == "__main__":
    engine = get_db_engine()

    # Run a specific query
    query_file = r'C:\Users\u614867\Documents\python\fleetstore\queries\q2.sql'
    result = execute_query_from_file(query_file, engine)
    if result is not None:
        print(result.head())  # preview

    # Or fetch all files
    # base_path = r'C:\Users\u614867\Documents\python\fleetstore'
    # fetch_all_flight_phase_data(base_path, engine)

