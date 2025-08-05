import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logging(module_name, data_type=None, log_dir="./logs/Transformer"):
    """
    Set up logging to write to a rotating daily file in a data-type and module-specific subfolder and to the console.

    Args:
        module_name (str): Name of the module (e.g., 'Main', 'Transformer', 'Tools').
        data_type (str, optional): Data type (e.g., '5min', '15min', 'mgw'). 
                                   If None, logs go to log_dir/module_name/.
        log_dir (str): Base directory where data-type and module-specific log subfolders will be stored.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Build log directory path
    if data_type:
        module_log_dir = os.path.join(log_dir, data_type, module_name)
        logger_name = f"{data_type}.{module_name}"
    else:
        module_log_dir = os.path.join(log_dir, module_name)
        logger_name = module_name

    os.makedirs(module_log_dir, exist_ok=True)

    # Daily log filename
    date_str = datetime.now().strftime("%Y%m%d")
    log_filename = f"{module_name.lower()}_{date_str}.log"
    log_path = os.path.join(module_log_dir, log_filename)

    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # Prevent duplicate handlers

    # Create rotating file handler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50 * 1024 * 1024,  # 50 MB max per file
        backupCount=5,              # Keep 5 old log files
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Define log format
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Log initialization message (only for new or empty log files)
    if not os.path.exists(log_path) or os.path.getsize(log_path) == 0:
        logger.info(f"Logging initialized for {logger_name}. Logs are being written to: {log_path}")

    return logger
