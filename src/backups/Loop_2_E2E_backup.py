import pandas as pd
import os
from pprint import pprint as pp

def Loop_2_E2E(df: pd.DataFrame, flight_phase: str = None, DebugOption: int = 1) -> pd.DataFrame:
    """
    For each (ACID, reportdatetime) combination in the input DataFrame that has 
    exactly two engines (ESNs) reporting, calculate the engine-to-engine (E2E) 
    delta for selected parameters.

    Parameters:
    ----------
    df : pd.DataFrame
        Input DataFrame containing columns 'ACID', 'reportdatetime', 'ESN', and
        a set of parameter columns suffixed with '__DEL_PC'.

    Returns:
    -------
    pd.DataFrame
        A new DataFrame containing all original data plus:
            - 'SISTER_ESN': the ESN of the "sister" engine
            - '[PARAM]_E2E': the difference between the engine's value and its sister's
    """
    
    # List of parameter columns to calculate E2E deltas for
    input_columns = [
        'PS26__DEL_PC', 'T25__DEL_PC', 'P30__DEL_PC', 'T30__DEL_PC',
        'TGTU__DEL_PC', 'NL__DEL_PC', 'NI__DEL_PC', 'NH__DEL_PC',
        'FF__DEL_PC', 'P160__DEL_PC'
    ]

    grouped = df.groupby(['ACID', 'reportdatetime'])
    """
    # print each sub-DataFrame grouped by tuple ('ACID', 'reportdatetime')
    for (acid, rdt), group in grouped:
        print(f"Group: ACID={acid}, reportdatetime={rdt}")
        print(group)

    """
    # Filter: keep only ACID+reportdatetime pairs with exactly 2 unique ESNs
    valid_groups = grouped.filter(lambda x: x['ESN'].nunique() == 2)

    # Group valid_groups by (ACID, reportdatetime)
    valid_groups.groupby(['ACID', 'reportdatetime'])
    print("valid_groups.groupby(['ACID', 'reportdatetime']) Done!")

    # Initialize a list to store each processed row's results
    result_rows = []

    # Obtain total cycle loops for the next for loop cycle
    total_cycle_loops = len(list(valid_groups.index))
    #Initialize a counter to count the iteration for each foor loop cycle
    counter = 0
    # Iterate over each valid group of (ACID, reportdatetime)
    for (acid, rdt), group in valid_groups.groupby(['ACID', 'reportdatetime']):

        #Sub routine to get the PC completion of the foor loop cycle printed to the terminal
        previous_pc = round(100*counter/total_cycle_loops,1)
        counter += 1
        current_pc = round(100*counter/total_cycle_loops,1)
        
        if current_pc > previous_pc:
            print(f"        LOOP 2 {flight_phase}: {current_pc}% complete")
  

        # Sort the group by ESN so that engine order is consistent
        group_sorted = group.sort_values('ESN').reset_index(drop=True)

        # Assign engine 1 and engine 2
        eng1 = group_sorted.iloc[0]
        eng2 = group_sorted.iloc[1]

        # For each engine in the pair
        for i, row in group_sorted.iterrows():
            # Assign the "sister" engine: if this is eng1, sister is eng2 and vice versa
            sister = eng2 if i == 0 else eng1

            # Convert the row to a dictionary for output
            data = row.to_dict()

            # Add the ESN of the sister engine
            data['SISTER_ESN'] = int(sister['ESN'])

            # Calculate E2E deltas for each input parameter
            for param in input_columns:
                try:
                    delta = float(row[param]) - float(sister[param])
                except (ValueError, KeyError):
                    delta = float('nan')  # Assign NaN if conversion or lookup fails
                data[param + '_E2E'] = round(delta, 5)

            # Add the enriched row to the result list
            result_rows.append(data)

    # Create a DataFrame from all result rows
    df_out = pd.DataFrame(result_rows)

    if DebugOption == 1:
        # Save a temporary CSV file for debugging or traceability
        current_dir = os.getcwd()
        fleetore_dir = os.path.join(current_dir,"Fleetstore_Data")
        path_temp = os.path.join(fleetore_dir, f"LOOP_2_{flight_phase}.csv")
        print(f"        File saved to: {path_temp}")
        df_out.to_csv(path_temp)
    return df_out

"""

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
        df = Loop_2_E2E(df)
        print(df) 
    except Exception as e:
            print(f"error fetching data: {e}")
"""
from src.utils.load_data import load_temp_data as ltd

# Manually run LOOP_3
if __name__  == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_0"        
    try:
        data_dict = ltd(LOOP_str)
        flight_phase = 'cruise'
        df = data_dict[flight_phase]
        print(f"data from {LOOP_str} loaded!")


        """        
        df = df.copy()
        df['FlagSV'] = 0 # Set up new column for SV shop visit
        df['FlagSisChg'] = 0 # Set up new column for engine change
        # Identify ESNs with new data
        esns_with_new = df.loc[df['NEW_FLAG'] == 1, 'ESN'].unique()
        print(f"    >>>>esns_with_new:\n {esns_with_new}")
        """
        df = Loop_2_E2E(df,flight_phase,DebugOption=1)
        print("Loop 2 completed!") 
        
    except Exception as e:
        
        print(f"        error fetching data: {e}")