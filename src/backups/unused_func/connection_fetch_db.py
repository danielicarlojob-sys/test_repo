import pyodbc
from dotenv import load_dotenv # pip install python-dotenv
import os
import pandas as pd
from pathlib import Path


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
    print('    in conn function')
    return conn

def close_connection(conn):
    '''
    Function to close connection to database
    '''
    conn.close()
    print("     connection closed")

def execute_query_from_file(sql_file_path):
    '''
    Function to execute a query from a .sql file
    '''
    try:
        conn = connect_to_db()
        # Read the SQL query from the file
        os.chdir(r'C:\Users\u614867\Documents\python\fleetstore\queries')
        with open(sql_file_path, "r") as file:
            query = file.read()
        # Fetch data from Fleet store and store it in a dataframe
        data = pd.read_sql_query(query, conn)
        print("     Query executed successfully!")
        # Close database connection
        close_connection(conn)
        return data
    except Exception as e:
        print(f"Error executing the query: {e}")
        return None



def fetch_all_flight_phase_data(path):

    sql_file_path = os.path.join(path,'Queries')

    for file in os.listdir(sql_file_path):

        if file.endswith('CRZ.sql'):
            data = execute_query_from_file(file)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_cruise.csv')

        elif file.endswith('CLM.sql'):
            data = execute_query_from_file(file)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_climb.csv')

        elif file.endswith('FS.sql'):
            data = execute_query_from_file(file)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_fs.csv')

        elif file.endswith('SS.sql'):
            data = execute_query_from_file(file)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_ss.csv')
                
        elif file.endswith('TKO.sql'):
            data = execute_query_from_file(file)
            if not data.empty:
                os.chdir(os.path.join(path,'Fleetstore_Data'))
                data.to_csv('data_take-off.csv')

import logging
import pprint
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
def fetch_data(conn,logger):
    query ="""
    /****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (10) [EngineId]
      ,[StartDatetime]
      ,[AdditionalKey]
      ,[EndDatetime]
      ,[EngineSerialNumber]
      ,[ProvidedEngineSerialNumber]
      ,[ParentStartDatetime]
      ,[AircraftId]
      ,[AircraftIdentifier]
      ,[ProvidedAircraftIdentifier]
      ,[EnginePosition]
      ,[ProvidedEnginePosition]
      ,[OperatorId]
      ,[OperatorCode]
      ,[ProvidedOperatorCode]
      ,[LastGeneratedMessageId]
      ,[FirstGeneratedMessageId]
      ,[LastGeneratedDatetime]
      ,[FirstGeneratedDatetime]
      ,[LastReceivedMessageId]
      ,[FirstReceivedMessageId]
      ,[LastReceivedDatetime]
      ,[FirstReceivedDatetime]
      ,[Changed]
      ,[Created]
      ,[Deleted]
      ,[Migrated]
      ,[CalendarId]
      ,[MN]
      ,[TPRC]
      ,[TPRX]
      ,[ALT__FT]
      ,[EECT__DEGC]
      ,[FF__LBHR]
      ,[FFDP__PSI]
      ,[FMP__DEG]
      ,[NL__PC]
      ,[NI__PC]
      ,[NH__PC]
      ,[OFDP__PSI]
      ,[OIP__PSI]
      ,[OIQ__USQT]
      ,[OIT__DEGC]
      ,[OSDP__PSI]
      ,[P160__PSI]
      ,[P20__PSI]
      ,[P30__PSI]
      ,[P50__PSI]
      ,[T20__DEGC]
      ,[T25__DEGC]
      ,[T30__DEGC]
      ,[TAT__DEGC]
      ,[TCAF__DEGC]
      ,[TCAR__DEGC]
      ,[TPRA]
      ,[VSV__DEG]
      ,[VSVD__DEG]
      ,[TGT__DEGC]
      ,[FT__DEGC]
      ,[VBBB_A__ACU]
      ,[CAS__KNTS]
      ,[FPH__DEG]
      ,[RPH__DEG]
      ,[TRA__DEG]
      ,[VBLP_A__ACU]
      ,[VBIP_A__ACU]
      ,[VBHP_A__ACU]
      ,[VBBB_B__ACU]
      ,[VBLP_B__ACU]
      ,[VBIP_B__ACU]
      ,[VBHP_B__ACU]
      ,[OIP_PSI]
      ,[ACOC__DEG]
      ,[EECLOOPIC]
      ,[ESSAIPD__PSI]
      ,[FMD__DEG]
      ,[GLE1__PC]
      ,[GLE2__PC]
      ,[P0__PSI]
      ,[PS26__PSI]
      ,[PS42__PSI]
      ,[PS44__PSI]
      ,[T50__DEGC]
      ,[TGTU_A__DEGC]
      ,[TGTU_B__DEGC]
      ,[TRP__PC]
      ,[TSAS__DEGC]
      ,[VBSAGBBB__ACU]
      ,[VBTBHBB__IPS]
      ,[VFSGOIT_1__DEGC]
      ,[VFSGOIT_2__DEGC]
      ,[SASV]
      ,[SMALLDEB]
      ,[LARGEDEB]
      ,[ERATV]
      ,[TN1__OHMS]
      ,[TN2__OHMS]
      ,[TN3__OHMS]
      ,[TN4__OHMS]
      ,[TN5__OHMS]
      ,[TN6__OHMS]
      ,[WAISEL]
      ,[TFM__DEGC]
      ,[ATCCHPVPOSDMD_PC]
      ,[ATCCHPVPOSSEL_PC]
      ,[ATCCIPVPOSDMD_PC]
      ,[ATCCIPVPOSSEL_PC]
      ,[FOHEDP_PSID]
      ,[P30_LOC_A__PSI]
      ,[P30_LOC_B__PSI]
      ,[ECW1]
      ,[ESN]
      ,[ECSSET]
      ,[HPBV2]
      ,[HPBV3]
      ,[IPBV1]
      ,[IPBV3]
      ,[LANEIC]
      ,[NAI]
      ,[WAI]
      ,[BUMPSEL]
      ,[HPIPTCC]
      ,[LPTCC]
      ,[IPBV4]
      ,[EEC_CHA_IN_CTRL]
      ,[EEC_CHB_IN_CTRL]
      ,[MAS_ON]
  FROM [B787-Trent1000].[Cruise-AircraftEngine-DA]
    """
    #query ="""SELECT *
    #FROM PARAMETERS;
    #""" 
    try:
        cursor = conn.cursor()
        results = cursor.execute(query)
        print("results: ", results)
        cols = [desc[0] for desc in results.description]
        print("cols: ", cols)
        rows = results.fetchall()
        rows_t = list(map(list,zip(*rows)))
        print("type(rows_t): ", type(rows_t))
        rows_len = [len(row) for row in rows_t]
        print("rows_len: ", len(rows_t))
        
        print("each_rows_len: ", rows_len)
        # pd.DataFrame(data=rows, columns=cols)
        temp = [row for row in rows]
        r = len(temp)
        c = len(temp[0]) if temp else 0
        print("temp dim: ", (r,c))
        r = len(cols)
        c = len(cols[0]) if cols else 0
        print("cols dim: ", (r,c))
        return pd.DataFrame(data = [row for row in rows],columns=cols)
    except Exception as e:
            logger.error(f"error fetching data: {e}")
            return pd.DataFrame()

#fetch_all_flight_phase_data()
if __name__  == "__main__":
    
    conn= connect_to_db()
    sql_file_path = r'C:\Users\u614867\Documents\python\fleetstore'
    fetch_all_flight_phase_data(sql_file_path)
    # sql_data = execute_query_from_file(sql_file_path)
    # print('DATA: ',type(sql_data))
    
    # data = fetch_data(conn,logger)
    # print("DATA: ", data)
    close_connection(conn)