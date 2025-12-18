import os
import asyncio
import pandas as pd
from src.utils.print_time_now import print_time_now
from Loop_4_movavg import loop_4_robcov as Loop4
from src.utils.load_data import load_temp_data as ltd



async def process_flight_phase(flight_phase: str, data_dict: dict, Fleetstore_data_dir: str) -> pd.DataFrame:
    print(f"        Start {flight_phase} cycle at {str(print_time_now())}")
    
    # Run blocking Loop4 in a separate thread
    df = await asyncio.to_thread(Loop4, data_dict[flight_phase], flight_phase)
    data_dict[flight_phase] = df

    return data_dict
async def main(data_dict: dict, Fleetstore_data_dir: str):
    tasks = [
        process_flight_phase(flight_phase, data_dict, Fleetstore_data_dir)
        for flight_phase in data_dict.keys()
    ]
    await asyncio.gather(*tasks)
    print("    Completed LOOP 4 - Moving average")

# Usage (from a synchronous context):
# asyncio.run(main(data_dict, Fleetstore_data_dir))

root_dir = os.getcwd()
Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')

if __name__  == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_3"

    try:
        data_dict = ltd(LOOP_str)
        
        asyncio.run(main(data_dict, Fleetstore_data_dir))

        print("     Loop 4 completed!") 

    except Exception as e:
        
        print(f"        error fetching data: {e}")
