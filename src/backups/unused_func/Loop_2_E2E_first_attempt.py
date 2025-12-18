import pandas as pd
import os
from pprint import pprint as pp

def Loop_2_E2E(df: pd.DataFrame) -> pd.DataFrame:
    dates_to_drop = []
    rows_to_keep =[]
    input_columns_E2E_calculation = ['PS26__DEL_PC',
    'T25__DEL_PC',
    'P30__DEL_PC',
    'T30__DEL_PC',
    'TGTU__DEL_PC',
    'NL__DEL_PC',
    'NI__DEL_PC',
    'NH__DEL_PC',
    'FF__DEL_PC',
    'P160__DEL_PC']
    ACID_list = df['ACID'].unique()
    
    for AC in ACID_list:    # Proceed to evaluate each ACID one by one
        df_ACID = df[df['ACID'] == AC]
        rdt_list = df_ACID['reportdatetime'].unique()
        for rdt in rdt_list:    # Proceed to evaluate each reportdatetime record one by one for each ACID
            df_ACID_rdt = df_ACID[df_ACID['reportdatetime']==rdt]
            ESNs = df_ACID_rdt['ESN'].unique()
            if ESNs < 2: # for the selected datetime there is only one ESN
                dates_to_drop.append(rdt)
            elif ESNs == 2:
                for esn in ESNs:
                    sister_engine = [int(eng) for eng in ESNs if eng != esn][0]
                    df_ACID_rdt_ESN = df_ACID_rdt[df_ACID_rdt['ESN']==esn]
                    df_ACID_rdt_ESN['SISTER_ESN'] = sister_engine
                    for parameter in input_columns_E2E_calculation:
                        output_parameter = parameter+"_E2E"
                        df_ACID_rdt_ESN[output_parameter] = float(df_ACID_rdt[df_ACID_rdt['ESN']==esn][parameter].iloc[0]) - float(df_ACID_rdt[df_ACID_rdt['ESN']!=esn][parameter].iloc[0])
                    
                    rows_to_keep.append(df_ACID_rdt_ESN)

    # df_E2E = df[~df['reportdatetime'].isin(dates_to_drop)]

    return rows_to_keep



# Define known dtypes
dtype_spec = {
    'ESN': 'Int64',
    'operator': 'string',
    'equipmentid': 'Int64',
    'ACID': 'string',
    'ENGPOS': 'Int64',
    'DSCID': 'Int64'
}

# List of datetime columns to parse
datetime_cols = ['reportdatetime', 'datestored']

# Read CSV
project_dir = os.getcwd()
fleetdata_dir = os.path.join(project_dir, "Fleetstore_Data")
csv_path = os.path.join(fleetdata_dir, "LOOP_0_Merged_crz.csv")
df = pd.read_csv(csv_path, dtype=dtype_spec)
df = df.drop(columns=['Unnamed: 0'])


if __name__  == "__main__":

    try:
        Loop_2_E2E(df)
        print("Done!") 
    except Exception as e:
            print(f"error fetching data: {e}")
