####
## Logging config file for logging purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import logging
import sys

# Get the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in list(logger.handlers):
    logger.removeHandler(handler)

# Single formatter for all messages
formatter = logging.Formatter(
    '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s'
)

# One handler that writes INFO+ to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)
