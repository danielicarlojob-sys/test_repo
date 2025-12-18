import time
import os
import psutil
import functools
import sys
import dis

# Define the path of the log file (in the root folder where the script runs)
LOG_FILE = os.path.join(os.getcwd(), "function_log.txt")

# Set of arithmetic operations we want to count (based on Python bytecode names)
ARITHMETIC_OPS = {
    "BINARY_ADD", "BINARY_SUBTRACT", "BINARY_MULTIPLY", "BINARY_TRUE_DIVIDE",
    "BINARY_FLOOR_DIVIDE", "BINARY_MODULO", "BINARY_POWER",
    "INPLACE_ADD", "INPLACE_SUBTRACT", "INPLACE_MULTIPLY", "INPLACE_TRUE_DIVIDE",
    "INPLACE_FLOOR_DIVIDE", "INPLACE_MODULO", "INPLACE_POWER",
    "UNARY_NEGATIVE", "UNARY_POSITIVE"
}

def monitor_function(func):
    """
    Decorator to monitor execution metrics of a function.

    This decorator logs the following information for each function call:
      - Execution duration (wall-clock time).
      - CPU user time and system time consumed by the process.
      - Number of arithmetic operations performed (counted via bytecode tracing).
      - Average CPU utilization percentage during the function's execution.
      - Estimated number of logical CPUs effectively used.
    
    The metrics are logged into a file named `function_log.txt` in the root folder.

    Args:
        func (callable): The function to wrap and monitor.

    Returns:
        callable: A wrapped version of the function with monitoring enabled.

    Example:
        >>> @monitor_function
        ... def my_calc(n):
        ... x = 1
        ... for i in range(1, n):
        ... x *= i
        ... return x
        ...
        >>> my_calc(100000)
        # Logs will be written to function_log.txt
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Create a psutil.Process object for monitoring CPU stats of the current process
        process = psutil.Process(os.getpid())

        # Dictionary to store the count of arithmetic operations
        calc_count = {"ops": 0}

        # Define a tracer function to inspect each bytecode instruction executed
        def tracer(frame, event, arg):
            """
            Tracer function to hook into Python execution.
            Captures arithmetic operations based on opcode names.
            """
            if event == "opcode": # Only inspect opcode events
                instr = dis.opname[frame.f_code.co_code[frame.f_lasti]] # Get the current opcode name
                if instr in ARITHMETIC_OPS: # Check if it's an arithmetic operation
                    calc_count["ops"] += 1 # Increment counter
            return tracer # Must return itself to keep tracing enabled

        # Get the number of logical CPUs available on the machine
        cpu_count = psutil.cpu_count(logical=True)

        # Start metrics collection
        start_time = time.perf_counter() # High-resolution wall clock timer
        start_cpu_times = process.cpu_times() # Snapshot of user/system CPU time
        start_cpu_percent = process.cpu_percent(interval=None) # Reset CPU % measurement

        # Enable tracing of Python bytecode execution
        sys.settrace(tracer)

        try:
            # Run the original function
            result = func(*args, **kwargs)
        finally:
            # Disable tracing after function execution
            sys.settrace(None)

        # End metrics collection
        end_time = time.perf_counter() # End wall clock timer
        end_cpu_times = process.cpu_times() # New snapshot of CPU times
        end_cpu_percent = process.cpu_percent(interval=None) # Average CPU % since last call

        # Compute execution duration
        duration = end_time - start_time

        # Compute CPU usage breakdown
        cpu_user = end_cpu_times.user - start_cpu_times.user
        cpu_system = end_cpu_times.system - start_cpu_times.system

        # Estimate effective CPUs used:
        # Example: if CPU% = 200% on an 8-core machine, avg_cpus_used ≈ 2
        avg_cpus_used = (end_cpu_percent / 100.0) * cpu_count

        # Write collected metrics into the log file
        with open(LOG_FILE, "a") as f:
            f.write(
                f"Function: {func.__name__}\n"
                f"Duration: {duration:.6f} seconds\n"
                f"CPU User Time: {cpu_user:.6f} s\n"
                f"CPU System Time: {cpu_system:.6f} s\n"
                f"Arithmetic operations: {calc_count['ops']}\n"
                f"Average CPU%: {end_cpu_percent:.2f}%\n"
                f"Estimated CPUs used: {avg_cpus_used:.2f} / {cpu_count}\n"
                f"{'-'*40}\n"
            )

        # Return the function's original result
        return result

    return wrapper
