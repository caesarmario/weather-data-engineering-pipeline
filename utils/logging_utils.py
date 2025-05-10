####
## Logging Config File for Logging Purposes
## Mario Caesar // caesarmario87@gmail.com
####

# Importing Libraries
import logging

logger = logging.getLogger("")
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    # Format log: waktu, file, line, level, message
    log_formatter = logging.Formatter(
        '%(asctime)s - %(filename)s - Line: %(lineno)d - %(levelname)s - %(message)s'
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
