import pandas as pd
import numpy as np
import os
import warnings
from sklearn.covariance import MinCovDet
from src.utils.load_data import load_temp_data as ltd

def loop_5(df: pd.DataFrame, flight_phase: str = None, Lag: list = [50, 100, 200, 400], DebugOption: int = 1) -> pd.DataFrame:
    
    # Define new Loop 5 columns to process
    Loop5_cols = [
        'PS26__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'T25__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'P30__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'T30__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'TGTU__DEL_PC_E2E_MAV_NO_STEPS_SHIFT',
        'NL__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'NI__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'NH__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'FF__DEL_PC_E2E_MAV_NO_STEPS_SHIFT', 
        'P160__DEL_PC_E2E_MAV_NO_STEPS_SHIFT'
    ]
    
    # Define corresponding LOOP5 and output columns
    Loop5_outout_cols = [col + str(l) for l in Lag for col in Loop5_cols ]



    # Ensure required columns exist
    for col in Loop5_outout_cols:
        if col not in df.columns:
            df[col] = np.nan
    temp_check_col = 'PS26__DEL_PC_E2E_MAV_NO_STEPS'

    
    print(f"df columns:{temp_check_col in list(df.columns)}")
    # Filter ESNs with NEW_FLAG == 1
    new_data = df[df['NEW_FLAG'] == 1]
    esns = df[df['NEW_FLAG'] == 1]['ESN'].unique()
    for idx, esn in enumerate(esns, 1):
        print(f"        LOOP 5 {flight_phase}: {round(100 * idx / len(esns), 1)}% complete")
        index_new_data_esn = new_data[new_data['ESN']==esn].index
        esn_index_min = min(index_new_data_esn)
        df_esn_new_data = new_data[new_data['ESN']==esn]
        #print(f"--->>>df_esn_new_data.columns: {'PS26__DEL_PC_E2E_MAV_NO_STEPS_SHIFT50' in list(df_esn_new_data.columns)}")
        print(f"index_new_data_esn:{list(index_new_data_esn)}")
        print(f">>> esn_index_min:{esn_index_min}")
        # print(f">>> df_esn_new_data:{df_esn_new_data}")
        print(f">>> df_esn_new_data (rows, cols):{df_esn_new_data.shape}")
        print(f">>> df_esn_new_data index:{df_esn_new_data.index}")
        for index in list(index_new_data_esn):
            for l in Lag: # starts the cycle for shift of 50, 100, 200, 400 pts
                sub_cols = [col+str(l) for col in Loop5_cols] # gets the column names respective shift

                for sub_col in sub_cols:
                    print(f"sub_col: {sub_col}")
                    original_column = sub_col[:-9] if l > 99 else sub_col[:-8] 
                    RangeStart = max(index-l, esn_index_min)
                    print(f">>> RangeStart:{RangeStart}")
                    df_temp = df_esn_new_data.loc[RangeStart, original_column]
                    t = list(range(RangeStart,index+1))
                    print(f">>> idx:{index}")

                    print(f">>> t:{t}")

                    print(f">>> df_temp:{df_temp}")

                    RelIndPrevPt = min(list(((df_esn_new_data.loc[RangeStart:index+1, sub_col]).dropna()).index)) # find the min index for 
                    print(f">>> RelIndPrevPt:{RelIndPrevPt}")

                    if len(RelIndPrevPt)==1:
                        AbsIndPrevPt=RangeStart+RelIndPrevPt-1
                        df.loc[index, sub_col] = df.loc[index, sub_col] - df.loc[AbsIndPrevPt, sub_col]



    # Save file to CSV
    if DebugOption == 1:
            # Save a temporary CSV file for debugging or traceability
            current_dir = os.getcwd()
            fleetore_dir = os.path.join(current_dir,"Fleetstore_Data")
            path_temp = os.path.join(fleetore_dir, f"LOOP_5_{flight_phase}.csv")
            print(f"        File saved to: {path_temp}")
            df.to_csv(path_temp)
    return df

# Manually run LOOP_5
if __name__  == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_4"        
    try:
        data_dict = ltd(LOOP_str)
        flight_phase = 'cruise'
        df = data_dict[flight_phase]
        df = loop_5(df,flight_phase)
        print("Loop 5 completed!") 
        path_temp = os.path.join(current_dir, f"loop5_{flight_phase}_temp.csv")
        print(f"        file will be saved to : {path_temp}")
        df.to_csv(path_temp)
        
    except Exception as e:
        
        print(f"        error fetching data: {e}")



####
"""data = {
    'Name': ['one','two','three','two','three','two','three'],
    'Age': [55, 25, 35,45,55,65,75],
    'City': ['London','Paris', 'Rome','New York', 'Dallas', 'Oxford', 'Nice']
}
ddf = pd.DataFrame(data)"""