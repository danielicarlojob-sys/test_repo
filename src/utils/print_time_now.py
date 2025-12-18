from datetime import datetime as dt


def print_time_now(print_switch: int = 0) -> str:
    """
    Prints time now

    Parameters
    ----------
    Args:
         - print_switch (int): switch to generate print

    Returns
    -------
         - time_now (str): string time now
    """
    time_now = str(dt.now().strftime("%H:%M:%S %d-%m-%y"))
    if print_switch == 1:
        print(time_now)
    return time_now
