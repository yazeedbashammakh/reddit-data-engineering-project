import logging
import logging.config
from logging.handlers import RotatingFileHandler
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG") 


def setup_logging():
    """
    Set up logging configuration
    """
    # Define logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))

    # Get the root logger and set basic configuration
    logging.basicConfig(level=LOG_LEVEL, format=log_format, datefmt=date_format, handlers=[console_handler])
    logging.getLogger().setLevel(LOG_LEVEL)

# Setup the logging
setup_logging()


def get_logger(module_name):
    """
    Return a logger instance with the specified module name.
    
    Parameters:
    - module_name (str): Name of the module to identify the logger.

    Returns:
    - logging.Logger: Configured logger for the specified module.
    """
    return logging.getLogger(module_name)
