import asyncio
import os
from datetime import datetime as dt
from backups.async_main import main as async_main
from src.utils.log_file import log_message
from src.utils.Initialise_Algorithm_Settings_engine_type_specific import Initialise_Algorithm_Settings_engine_type_specific, Xrates_dic_vector_norm
from src.utils.data_ingestion import data_ingestion
from src.utils.print_time_now import print_time_now

from src.Loop_0_Calculate_Deltas import Loop_0_delta_calc as Loop0
from src.Loop_2_E2E import Loop_2_E2E as Loop2
from src.Loop_3_flag_sv_and_eng_change import Loop_3_flag_sv_and_eng_change as Loop3
from Loop_4_movavg import loop_4_robcov as Loop4
from Loop_5_performance_trend import loop5_performance_trend as Loop5
from functools import partial
from src.Loop_6_v9 import loop_6_fit_signatures as Loop6
from Loop_7_IPC_HPC_PerfShift import loop_7 as Loop7


root_dir = os.getcwd()
Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')
# from src.utils.data_queries import *


Live_Data_Mode_time_start = print_time_now()
lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()

Xrates = Xrates_dic_vector_norm(Xrates)
# Data SQL queries
log_message(f"Start Live_Data_Mode at {str(Live_Data_Mode_time_start)}")
data_dict = data_ingestion(root_dir)
log_message(" Data extraction completed!")


# LOOP 0 - DELTA CALCULATION
log_message(" Start data processing")
log_message(
    f"    Start LOOP 0  - DELTA CALCULATION at {str(print_time_now())}")

asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop0))

log_message(
    f"    Completed LOOP 0 - DELTA CALCULATION  at {str(print_time_now())}")

# LOOP 2 - E2E calculation
log_message(f"    Start LOOP 2  - E2E CALCULATION at {str(print_time_now())}")

asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop2))

log_message(
    f"        Completed LOOP 2  - E2E CALCULATION at {str(print_time_now())}")

# LOOP 3 - Shop Visit SV and Engine change checks
log_message(
    f"    Start LOOP 3 - Shop Visit SV and Engine change checks at {str(print_time_now())}")

asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop3))

log_message(
    f"    Completed LOOP 3 - Shop Visit SV and Engine change checks at {str(print_time_now())}")


# LOOP 4 - Moving average 21 pts
log_message(
    f"    Start LOOP 4 - Moving average Async at {str(print_time_now())}")

asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop4))

log_message(
    f"    Completed LOOP 4 - Moving average at {str(print_time_now())}")

# LOOP 5 - changes in E2E deltas over lagged windows
log_message(
    f"    Start LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")

asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop5))

log_message(
    f"    Completed LOOP 5 - changes in E2E deltas over lagged windows at {str(print_time_now())}")

# LOOP 6 - signatures fit
log_message(
    f"    Start LOOP 6 - signatures fit at {str(print_time_now())}")
# Loop6 requires Xrates as a positional argument, so we wrap it with partial
Loop_6_wrapped = partial(Loop6, Xrates=Xrates)
asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop_6_wrapped))
#asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop6))

log_message(
    f"    Completed LOOP 6 - signatures fit at {str(print_time_now())}")
"""
"""
##########################################################################

log_message(    f"    Final check on data_dict, verify all keys are included: {        list(            data_dict.keys())}")

Live_Data_Mode_time_completion = print_time_now()

start_dt = dt.strptime(Live_Data_Mode_time_start, "%H:%M:%S %d-%m-%y")
end_dt = dt.strptime(Live_Data_Mode_time_completion, "%H:%M:%S %d-%m-%y")

Live_Data_Mode_time_duration = str(end_dt - start_dt)

log_message(    f"Live_Data_Mode completed at {        str(Live_Data_Mode_time_completion)}")
log_message(f"Live_Data_Mode elapsed time {Live_Data_Mode_time_duration}")
