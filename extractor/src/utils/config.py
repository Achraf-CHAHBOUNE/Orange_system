import os
from dotenv import load_dotenv
import re
from typing import Dict, Pattern
from datetime import datetime


# Load environment variables
load_dotenv()

# Source MySQL Configuration (Company Server)
SOURCE_MYSQL_HOST: str = os.getenv("SOURCE_MYSQL_HOST")
SOURCE_MYSQL_USER: str = os.getenv("SOURCE_MYSQL_USER")
SOURCE_MYSQL_PASSWORD: str = os.getenv("SOURCE_MYSQL_PASSWORD")
SOURCE_MYSQL_PORT: int = int(os.getenv("SOURCE_MYSQL_PORT", 3306))
SOURCE_MYSQL_DB: str = os.getenv("SOURCE_MYSQL_DB")

# Destination MySQL Configuration (Target Server)

DEST_MYSQL_HOST: str = os.getenv("DEST_MYSQL_HOST", "localhost")
DEST_MYSQL_USER: str = os.getenv("DEST_MYSQL_USER", "root")
DEST_MYSQL_PASSWORD: str = os.getenv("DEST_MYSQL_PASSWORD", "")
DEST_MYSQL_PORT: int = int(os.getenv("DEST_MYSQL_PORT", 3306))
DEST_MYSQL_DB: str = os.getenv("DEST_MYSQL_DB")

# Source Configuration
SOURCE_CONFIG = {
    'host': SOURCE_MYSQL_HOST,
    'user': SOURCE_MYSQL_USER,
    'password': SOURCE_MYSQL_PASSWORD,
    'port': SOURCE_MYSQL_PORT,
    'database': SOURCE_MYSQL_DB
}

# Destination Configuration
DESTINATION_CONFIG = {
    'host': DEST_MYSQL_HOST,
    'user': DEST_MYSQL_USER,
    'password': DEST_MYSQL_PASSWORD,
    'port': DEST_MYSQL_PORT,
    'database': DEST_MYSQL_DB
}


# Data patterns
patterns: Dict[str, Pattern] = {
    '5min': re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]5_S\d+_A\d{4}$', re.IGNORECASE),
    '15min': re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]15_S\d+_A\d{4}$', re.IGNORECASE),
    'mgw': re.compile(r'^([A-Za-z0-9]+)MGW_S\d+_A\d{4}$', re.IGNORECASE)
}

# The Date to start extracting data
START_DATE: datetime = datetime(2024, 1, 1)  # change as needed


# Paths to store and load the extracted data
files_paths: Dict[str, str] = {
    '5min': './data/our_data/result_5min.txt',
    '15min': './data/our_data/result_15min.txt',
    'mgw': './data/our_data/result_mgw.txt',
    'last_extracted': './data/last_extracted.json'
}