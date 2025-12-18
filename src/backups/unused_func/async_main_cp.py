import os
import asyncio
import pandas as pd
import traceback
from datetime import datetime
from tqdm import tqdm as tqdm_sync

from src.utils.print_time_now import print_time_now
from Loop_4_movavg import loop_4_robcov as Loop4
from src.utils.load_data import load_temp_data as ltd

# Define log file path
LOG_FILE = os.path.join(os.getcwd(), "process_log.txt")

def log_message(message: str, to_file: bool = True):
    """Log message with timestamp to console and optionally to file."""
    timestamped = f"[{print_time_now()}] {message}"
    print(timestamped)
    if to_file:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(timestamped + "\n")

async def process_flight_phase(
    flight_phase: str,
    data_dict: dict,
    Fleetstore_data_dir: str,
    process_function,
    progress_bar,
    **kwargs
) -> pd.DataFrame:
    """Async processing of a single flight phase with timing and error handling."""

    start_time = datetime.now()
    log_message(f"Start {flight_phase} cycle")

    try:
        # Run processing in a background thread
        df = await asyncio.to_thread(
            process_function,
            data_dict[flight_phase],
            flight_phase,
            **kwargs
        )
        data_dict[flight_phase] = df

        elapsed = datetime.now() - start_time
        log_message(f"Completed {flight_phase} in {elapsed.total_seconds():.2f} seconds")

    except Exception as e:
        log_message(f"ERROR in {flight_phase}: {str(e)}")
        log_message(traceback.format_exc())

    finally:
        progress_bar.update(1)
        return data_dict

async def main(
    data_dict: dict,
    Fleetstore_data_dir: str,
    process_function,
    **kwargs
):
    """Main async runner for all flight phases, with progress tracking and logging."""

    log_message("Starting LOOP processing")

    total = len(data_dict)
    progress_bar = tqdm_sync(total=total, desc="Processing flight phases", unit="phase")

    tasks = [
        process_flight_phase(
            flight_phase,
            data_dict,
            Fleetstore_data_dir,
            process_function,
            progress_bar,
            **kwargs
        )
        for flight_phase in data_dict.keys()
    ]

    await asyncio.gather(*tasks)
    progress_bar.close()

    log_message("Completed all flight phases")




#################




root_dir = os.getcwd()
Fleetstore_data_dir = os.path.join(root_dir, 'Fleetstore_Data')

if __name__  == "__main__":
    current_dir = os.getcwd()
    LOOP_str = "LOOP_3"

    try:
        data_dict = ltd(LOOP_str)
        
        asyncio.run(main(data_dict, Fleetstore_data_dir,Loop4))

        print("     Loop 4 completed!") 

    except Exception as e:
        
        print(f"        error fetching data: {e}")
