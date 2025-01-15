# src/etl/utils/logger.py
import logging
import os

# Define the log directory and file
LOG_DIR = os.path.join(os.path.dirname(__file__), '../../../data/logs')
LOG_FILE = os.path.join(LOG_DIR, 'etl_logs.txt')

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging
def get_logger():
    logger = logging.getLogger('ETL_Logger')
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if the logger is called multiple times
    if not logger.handlers:
        # Create a file handler
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.INFO)

        # Create a console handler for debugging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Define the logging format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger