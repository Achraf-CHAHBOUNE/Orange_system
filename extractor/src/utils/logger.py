import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logging(module_name, log_dir="./logs/Extractor"):
    """
    Set up logging to write to a rotating daily file in a module-specific subfolder and to the console.
    
    Args:
        module_name (str): Name of the module (e.g., 'Extractor', 'Loader').
        log_dir (str): Base directory where module-specific log subfolders will be stored.
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create module-specific log subfolder (e.g., ./logs/Extractor/Extractor/)
    module_log_dir = os.path.join(log_dir, module_name)
    os.makedirs(module_log_dir, exist_ok=True)

    # Daily log filename (e.g., extractor_20250428.log)
    date_str = datetime.now().strftime("%Y%m%d")
    log_filename = f"{module_name.lower()}_{date_str}.log"
    log_path = os.path.join(module_log_dir, log_filename)

    # Create a logger
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # Prevent duplicate logs

    # Create rotating file handler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=5               # keep 5 backups
    )
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Define log format
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Log the first message if the file is new or empty
    if not os.path.exists(log_path) or os.path.getsize(log_path) == 0:
        logger.info(f"Logging initialized for {module_name}. Logs are being written to: {log_path}")

    return logger
