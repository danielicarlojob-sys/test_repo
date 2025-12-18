import os
from src.utils.print_time_now import print_time_now
import traceback
import inspect
# Define log file path
LOG_FILE = os.path.join(os.getcwd(), "process_log.txt")


def log_message(message: str, to_file: bool = True):
    """
    Log message with timestamp to console and optionally to file.

    Parameters
    ----------
    Args:
         - message (str): logged message
         - to_file (bool): switch to log message to file
    """
    timestamped = f"[{print_time_now()}] {message}"
    print(timestamped)
    if to_file:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(timestamped + "\n")


def debug_info():
    # Get the caller’s frame (1 step back in the stack)
    frame = inspect.stack()[1]
    filename = os.path.basename(frame.filename)
    function = frame.function
    lineno = frame.lineno
    return f"[DEBUG] Called from file: {filename}, function: {function}, line: {lineno}"

def f_lineno():
    return inspect.currentframe().f_back.f_lineno
