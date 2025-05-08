####
## Logging Config File for Logging Purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import logging
import os
from datetime import datetime

# Function to create log directories and return log file path
def get_log_file_path(log_type):
    """
    Generates the log file path based on the log type.

    Parameters:
    - log_type (str): Type of logging category ('process_flatten', 'file_render', 'etl_process').

    Returns:
    - str: Full log file path.
    """
    current_time = datetime.now()
    log_dir = os.path.join("log", log_type, str(current_time.year), str(current_time.month).zfill(2), str(current_time.day).zfill(2))
    
    # Ensure directory is created only when needed
    os.makedirs(log_dir, exist_ok=True)

    log_filename = f"{log_type}_log_{current_time.strftime('%Y-%m-%d-%H%M')}.log"
    return os.path.join(log_dir, log_filename)


# Dictionary to store created loggers
loggers = {}

# Function to retrieve specific logger dynamically
def get_logger(log_type="etl_process"):
    """
    Retrieves the logger for the specified log type.

    Parameters:
    - log_type (str): Type of logger ('process_flatten', 'file_render', 'etl_process').

    Returns:
    - logging.Logger: Logger instance for the specified log type.
    """
    if log_type not in loggers:
        log_path = get_log_file_path(log_type)

        logger = logging.getLogger(log_type)
        logger.setLevel(logging.INFO)

        # Create file handler
        file_handler = logging.FileHandler(log_path)
        log_formatter = logging.Formatter('%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

        # Optional: Create stream handler for console output
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(log_formatter)
        logger.addHandler(stream_handler)

        loggers[log_type] = logger

    return loggers[log_type]