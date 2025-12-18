import os
import asyncio
import pandas as pd
from functools import partial
import traceback
from datetime import datetime as dt
from src.utils.log_file import LOG_FILE, log_message


"""
from src.utils.log_file import LOG_FILE, log_message
from src.Loop_0_Calculate_Deltas import Loop_0_delta_calc as Loop_0
from src.Loop_2_E2E import Loop_2_E2E as Loop_2
from src.Loop_3_flag_sv_and_eng_change import Loop_3_flag_sv_and_eng_change as Loop_3
from src.Loop_4_robcov import loop_4_robcov as Loop_4
from src.Loop_5 import loop5_performance_trend as Loop_5
"""

from src.utils.load_data import load_temp_data as ltd


async def process_flight_phase(
    flight_phase: str,
    data_dict: dict,
    Fleetstore_data_dir: str,
    process_function,
    **kwargs
) -> dict:
    """
    Async processing of a single flight phase with timing and error handling.

    Parameters
    ----------
    Args:
         - flight_phase (str): string flight phase
         - data_dict (dict): dictionary containing pd.DataFrames
         - Fleetstore_data_dir (str): path to fleetstore_data
         - process_function
         - **kwargs

    Returns
    -------
         - data_dict (pd.DataFrame)
    """
    # Handle both regular functions and partial-wrapped ones
    func_name = (
        process_function.func.__name__
        if isinstance(process_function, partial)
        else process_function.__name__
    )
    start_time = dt.now()
    log_message(f"Start {flight_phase} cycle for {func_name}")

    try:
        # Remove Xrates from kwargs if already bound via partial
        safe_kwargs = {k: v for k, v in kwargs.items() if k != "Xrates"}

        # Run processing in a background thread

        df = await asyncio.to_thread(
            process_function,
            df=data_dict[flight_phase],
            flight_phase=flight_phase,
            **safe_kwargs
        )


        data_dict[flight_phase] = df

        elapsed = dt.now() - start_time
        log_message(            f"Completed {flight_phase} for {                func_name} in {                elapsed.total_seconds():.2f} seconds")

    except Exception as e:
        log_message(f"ERROR in {flight_phase}: {str(e)}")
        log_message(traceback.format_exc())

    finally:
        return data_dict


async def main(
    data_dict: dict,
    Fleetstore_data_dir: str,
    process_function,
    **kwargs
):
    """
    Main async runner for all flight phases, with progress tracking and logging.

    Parameters
    ----------
    Args:
         - data_dict (pd.DataFrame)
         - Fleetstore_data_dir (str): path to fleetstore_data
         - process_function
         - **kwargs
    """
    # Handle both regular functions and partial-wrapped ones
    func_name = (
        process_function.func.__name__
        if isinstance(process_function, partial)
        else process_function.__name__
    )
    log_message(f"-> {func_name} - Starting processing")

    tasks = [
        process_flight_phase(
            flight_phase,
            data_dict,
            Fleetstore_data_dir,
            process_function,
            **kwargs
        )
        for flight_phase in data_dict.keys()
    ]

    log_message(
        f"-> {func_name} - Completed all flight phases")
    await asyncio.gather(*tasks)
    #results = await asyncio.gather(*tasks)
    #return {phase: df_out for phase, df_out in results}



#################


root_dir = os.getcwd()
Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')

if __name__ == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "data_"

    try:
        data_dict = ltd(LOOP_str)
        cols_LOG_FILE = os.path.join(os.getcwd(), "cols_log_data.txt")
        for flight_phase in data_dict.keys():
            cols = data_dict[flight_phase].columns
            for idx, col in enumerate(cols, 1):
                with open(cols_LOG_FILE, "a", encoding="utf-8") as f:
                    if idx == 1:
                        f.write(
                            f"flight phase: {flight_phase}, idx: {idx}, col: {col}\n")
                    else:
                        f.write(f"                   idx: {idx}, col: {col}\n")

        """
        process_function = Loop_0
        asyncio.run(main(data_dict, Fleetstore_data_dir,process_function))
        print(f"     {process_function.__name__} completed!")

        """
    except Exception as e:

        print(f"        error fetching data: {e}")
