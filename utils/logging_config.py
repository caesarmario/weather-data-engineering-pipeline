#### 
## Logging Config File for Logging Purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import logging

# Capture filename, function, and line number
log_formatter = logging.Formatter(
    '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s'
)

# Set Up Logging Config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"),
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
