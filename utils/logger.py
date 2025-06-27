import logging
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Set log name and level
logger = logging.getLogger("de_lab")
logger.setLevel(logging.DEBUG)

# Set console and file handler
console_handler = logging.StreamHandler()
info_log_handler = logging.FileHandler(os.path.join(LOG_DIR, "info.log"))
error_log_handler = logging.FileHandler(os.path.join(LOG_DIR, "error.log"))

# Add formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
info_log_handler.setFormatter(formatter)
error_log_handler.setFormatter(formatter)


# Attach handlers (only once)
if not logger.hasHandlers():
    logger.addHandler(console_handler)
    logger.addHandler(info_log_handler)
    logger.addHandler(error_log_handler)