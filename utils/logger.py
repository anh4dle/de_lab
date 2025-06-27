import logging
import os

    
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Set log name and level
logger = logging.getLogger("de_lab")
logger.setLevel(logging.DEBUG)

# Set console and file handler
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler(os.path.join(LOG_DIR, "app_log.log"))

# Add formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)


# Attach handlers (only once)
if not logger.hasHandlers():
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    