####
## Logging Config File for Logging Purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import logging
import os
from datetime import datetime

# Variables
current_time = datetime.now()
log_dir      = os.path.join("log", str(current_time.year), str(current_time.month).zfill(2), str(current_time.day).zfill(2))

# Create log directory if not exists
os.makedirs(log_dir, exist_ok=True)

# Generate log filename with date
log_filename = os.path.join(log_dir, f"etl_process_log_{current_time.strftime('%Y-%m-%d-%H%M')}.log")

# Capture filename, function, and line number
log_formatter = logging.Formatter(
    '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s'
)

# Set Up Logging Config
logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# Custom logger function
def get_logger():
    logger = logging.getLogger()
    for handler in logger.handlers:
        handler.setFormatter(log_formatter)
    return logger

# Initialize logger
logger = get_logger()
