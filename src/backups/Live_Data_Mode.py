import os
import pandas as pd
from src.utils.print_time_now import print_time_now
from backups.data_queries import *
from src.utils.Initialise_Algorithm_Settings_engine_type_specific import *
from backups.xrates_reader import *
from src.Loop_0_Calculate_Deltas import Loop_0_delta_calc as Loop0
from src.Loop_2_E2E import Loop_2_E2E as Loop2
from src.Loop_3_flag_sv_and_eng_change import Loop_3_flag_sv_and_eng_change as Loop3
from Loop_4_movavg import loop_4_robcov as Loop4
from unused_func.Loop_4_async import *
from Loop_5_performance_trend import loop5_performance_trend as Loop5

Live_Data_Mode_time_start = print_time_now()
print(f"Start Live_Data_Mode at {str(Live_Data_Mode_time_start)}")

root_dir = os.getcwd()
Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')

lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()

Xrates = Xrates_dic_vector_norm(Xrates)

data_dict = fetch_all_flight_phase_data(root_dir)
print(" Data extraction completed!")

# LOOP 0 - DELTA CALCULATION
print(" Start data processing")
print(f"    Start LOOP 0  - DELTA CALCULATION at {str(print_time_now())}")
flight_phase_dict = {
    'cruise':'crz',
    'take-off':'tko',
    'climb':'climb'
}
for flight_phase in data_dict.keys():
    print(f"        Start {flight_phase} cycle")
    df_Loop0 = Loop0(data_dict[flight_phase], flight_phase)
    data_dict[flight_phase] = df_Loop0
    print(f"        {flight_phase} completed")
    print_time_now()

print(f"    Completed LOOP 0 - DELTA CALCULATION  at {str(print_time_now())}")

# LOOP 2 - E2E calculation
print(f"    Start LOOP 2  - E2E CALCULATION at {str(print_time_now())}")

for flight_phase in data_dict.keys():
    print(f"        Start {flight_phase} cycle at {str(print_time_now())}")
    df_Loop2 = Loop2(data_dict[flight_phase], flight_phase)
    data_dict[flight_phase] = df_Loop2
    print(f"        {flight_phase} cycle completed at {str(print_time_now())}")
print(f"        Completed LOOP 2  - E2E CALCULATION at {str(print_time_now())}")

# LOOP 3 - Shop Visit SV and Engine change checks
print(f"    Start LOOP 3 - Shop Visit SV and Engine change checks at {str(print_time_now())}")

for flight_phase in data_dict.keys():
    print(f"        Start {flight_phase} cycle at {str(print_time_now())}")
    df_Loop3 = Loop3(data_dict[flight_phase], flight_phase)
    data_dict[flight_phase] = df_Loop3
    print(f"        {flight_phase} cycle completed at {str(print_time_now())}")

print(f"    Completed LOOP 3 - Shop Visit SV and Engine change checks at {str(print_time_now())}")


# LOOP 4 - Moving average 21 pts
print(f"    Start LOOP 4 - Moving average Async at {str(print_time_now())}")
asyncio.run(main(data_dict, Fleetstore_data_dir))
print(f"    Completed LOOP 4 - Moving average at {str(print_time_now())}")

# LOOP 5 - changes in E2E deltas over lagged windows
print(f"    Start LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")

for flight_phase in data_dict.keys():
    print(f"        Start {flight_phase} cycle at {str(print_time_now())}")
    df_Loop5 = Loop5(data_dict[flight_phase], flight_phase)
    data_dict[flight_phase] = df_Loop5
    print(f"        {flight_phase} cycle completed at {str(print_time_now())}")

print(f"    Completed LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")









####################################################################################################

print(f"    Final check on data_dict, verify all keys are included: {list(data_dict.keys())}")

Live_Data_Mode_time_completion = print_time_now()

start_dt = dt.strptime(Live_Data_Mode_time_start, "%H:%M:%S %d-%m-%y")
end_dt = dt.strptime(Live_Data_Mode_time_completion, "%H:%M:%S %d-%m-%y")

Live_Data_Mode_time_duration = str(end_dt - start_dt)

print(f"Live_Data_Mode completed at {str(Live_Data_Mode_time_completion)}")
print(f"Live_Data_Mode elapsed time {Live_Data_Mode_time_duration}")


