import asyncio
import os
from datetime import datetime as dt
from src.utils.async_main import main as async_main
from src.utils.log_file import log_message
from src.utils.Initialise_Algorithm_Settings_engine_type_specific import Initialise_Algorithm_Settings_engine_type_specific, Xrates_dic_vector_norm
from src.utils.data_ing import data_ingestion
from src.utils.print_time_now import print_time_now
from functools import partial


from src.Loop_0_Calculate_Deltas_v1 import Loop_0_delta_calc as Loop0
from src.Loop_2_E2E_v1 import Loop_2_E2E as Loop2
from src.Loop_3_flag_sv_and_eng_change_v1 import Loop_3_flag_sv_and_eng_change as Loop3
from src.Loop_4_movavg_mod_v1 import Loop_4_movavg as Loop4
from src.Loop_5_performance_trend import Loop5_performance_trend as Loop5
from src.Loop_6_fit_signatures import Loop_6_fit_signatures as Loop6
from src.Loop_7_IPC_HPC_PerfShift import Loop_7_IPC_HPC_PerfShift as Loop7
from src.Loop_8_Summary_Stats import Loop_8_Summary_Stats as Loop8
from src.Loop_9_combine_DSC import Loop_9_combine_DSC as Loop9


def Live_Data_Mode():
    """
    function to group all the functions and loops neccesary to run IPC Rotor 8 script.
    """
    
    Live_Data_Mode_time_start = print_time_now()
    log_message(f"Start {Live_Data_Mode.__name__} at {str(Live_Data_Mode_time_start)}")
    
    # Xrates data extraction
    root_dir = os.getcwd()
    Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')
    # Check if the directory exists, and create it if it doesn't
    if not os.path.exists(Fleetstore_data_dir):
        os.makedirs(Fleetstore_data_dir)
        log_message(f"Created directory: {Fleetstore_data_dir}")
    else:
        log_message(f"Directory already exists: {Fleetstore_data_dir}")
    lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()
    Xrates = Xrates_dic_vector_norm(Xrates)

    # Data SQL queries and historical data ingestion (if available)
    data_dict = data_ingestion(root_dir)
    log_message(" Data extraction completed!")

    run_loops = True
    if run_loops == True:
        # LOOP 0 - DELTA CALCULATION
        log_message(" Start data processing")
        process_func = Loop0
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop0))

            log_message(
                f"Completed {process_func.display_name}  at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")

        # LOOP 2 - E2E calculation
        process_func = Loop2
        log_message(f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop2))

            log_message(
                f"    Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")

        # LOOP 3 - Shop Visit SV and Engine change checks
        process_func = Loop3
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop3))
            log_message(
            f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")

        # LOOP 4 - Moving average 21 pts
        process_func = Loop4
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop4))
            log_message(
            f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")



        # LOOP 5 - changes in E2E deltas over lagged windows
        process_func = Loop5
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(data_dict, Fleetstore_data_dir, Loop5))
            log_message(
            f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")

        # LOOP 6 - signatures fit
        process_func = Loop6
        # Set how to process Loop 6
        process_async = True
        
        if process_async == True:
            try:
                # LOOP 6 - signatures fit
                log_message(
                    f"Start LOOP 6 - signatures fit at {str(print_time_now())}")
                data_dict = asyncio.run(async_main(
                                                    data_dict = data_dict, 
                                                    Fleetstore_data_dir=Fleetstore_data_dir, 
                                                    process_function = process_func,
                                                    Xrates = Xrates))
                log_message(
                    f"Completed LOOP 6 - signatures fit at {str(print_time_now())}")
            except Exception as e:
                log_message(f"Could not run LOOP 6 - signatures fit: {e}")
        
        else:
            try:
                # LOOP 6 - signatures fit
                flight_phases = data_dict.keys()
                print(flight_phases)
                for flight_phase in flight_phases:
                    log_message(
                    f"Start {process_func.__name__} flight phase: {flight_phase} at {str(print_time_now())}")
                    try:
                        data_dict = process_func(  data_dict[flight_phase],
                                                            flight_phase,
                                                            Xrates)
                        log_message(
                        f"Completed {process_func.__name__} flight phase: {flight_phase} at {str(print_time_now())}")
                    except Exception as e:
                        log_message(f"Could not run {process_func.__name__} flight phase: {flight_phase} at {str(print_time_now())}: {e}")
            except Exception as e:
                log_message(f"Could not run {process_func.__name__} flight phase: {flight_phase} at {str(print_time_now())}: {e}")
            
            

        # LOOP 7 - IPC HPC Performance Shift
        process_func = Loop7
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(
                    data_dict = data_dict, 
                    Fleetstore_data_dir=Fleetstore_data_dir, 
                    process_function = process_func))
            log_message(
                    f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}")

         # LOOP 8 - Summary Stats
        process_func = Loop8
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict = asyncio.run(async_main(
                    data_dict = data_dict, 
                    Fleetstore_data_dir=Fleetstore_data_dir, 
                    process_function = process_func,
                    Lim_dict = lim_dict))
            log_message(
                    f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}") 

         # LOOP 9 - Combine DSC
        process_func = Loop9
        log_message(
            f"Start {process_func.__name__} at {str(print_time_now())}")
        try:
            data_dict, df_combined = process_func(data_dict=data_dict, Lim_dict=lim_dict)
            log_message(
                    f"Completed {process_func.display_name} at {str(print_time_now())}")
        except Exception as e:
            log_message(f"Could not execute {process_func.display_name}: {e}") 
    
    ##########################################################################

    # log_message(f"Final check on data_dict, verify all keys are included: {list(data_dict.keys())}")

    Live_Data_Mode_time_completion = print_time_now()

    start_dt = dt.strptime(Live_Data_Mode_time_start, "%H:%M:%S %d-%m-%y")
    end_dt = dt.strptime(Live_Data_Mode_time_completion, "%H:%M:%S %d-%m-%y")

    Live_Data_Mode_time_duration = str(end_dt - start_dt)

    log_message(f"{Live_Data_Mode.__name__} completed at {str(Live_Data_Mode_time_completion)}")
    log_message(f"{Live_Data_Mode.__name__} elapsed time {Live_Data_Mode_time_duration}")
    ##########################################################################
    # Data output save
    ##########################################################################
    for flight_phase in data_dict.keys():
        path_output_data = os.path.join(Fleetstore_data_dir, f"data_output_{flight_phase}.csv")
        data_dict[flight_phase].to_csv(path_output_data, index=False)
        log_message(f"{flight_phase.capitalize()} data saved to: {path_output_data}")

if __name__ == "__main__":
    Live_Data_Mode()