import MySQLdb
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.logger import setup_logging
import re

def get_tools_logger(data_type=None):
    return setup_logging("Tools", data_type=data_type)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def connect_database(config: Dict[str, Any], data_type=None):
    """Connect to the database using mysqlclient with retries."""
    logger = get_tools_logger(data_type)
    try:
        logger.info(f"Connecting to database config: {config}")
        conn = MySQLdb.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['password'],
            port=config['port'],
            db=config['database']
        )
        logger.info(f"Successfully connected to database: {config['database']} on {config['host']}")
        return conn
    except MySQLdb.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def create_tables(cursor, tables: Dict[str, Any], data_type=None):
    """Create kpi_summary and tables with suffix column."""
    logger = get_tools_logger(data_type)
    try:
        # Create kpi_summary table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kpi_summary (
                id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                date DATETIME NOT NULL,
                node VARCHAR(50) NOT NULL,
                INDEX idx_date_node (date, node)
            );
        """)
        logger.info("Table 'kpi_summary' created or already exists.")

        # Create tables with suffix column
        for table_name, table_config in tables.items():
            columns = [
                'id INT NOT NULL AUTO_INCREMENT PRIMARY KEY',
                'kpi_id INT NOT NULL',
                'operator VARCHAR(50)',
                'suffix VARCHAR(255)'
            ]
            for kpi in table_config['kpis']:
                columns.append(f"{kpi} FLOAT")
            columns.append('FOREIGN KEY (kpi_id) REFERENCES kpi_summary(id)')
            columns.append('INDEX idx_kpi_id_operator_suffix (kpi_id, operator, suffix)')

            query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns)}
                );
            """
            cursor.execute(query)
            logger.info(f"Table '{table_name}' created or already exists.")

        logger.info("All tables created successfully.")
    except MySQLdb.Error as e:
        logger.error(f"Error creating tables: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating tables: {e}")
        raise

def extract_noeud(pattern, texts, data_type=None):
    """Extracts prefixes from the provided list of texts using the given regex pattern."""
    logger = get_tools_logger(data_type)
    matches = []
    for text in texts:
        match = pattern.match(text)
        if match:
            prefix = match.group(1).upper()
            matches.append((text, prefix))
            logger.debug(f"Extracted node prefix '{prefix}' from text '{text}'")
    return matches

def extract_indicateur_suffixe(indicateur, data_type=None):
    """Extract the suffix from the KPI name, preserving the full suffix string."""
    logger = get_tools_logger(data_type)
    if not isinstance(indicateur, str):
        logger.error("Indicateur must be a string")
        raise ValueError("Indicateur must be a string")
    
    parts = indicateur.split('.', 1)
    if len(parts) != 2:
        logger.warning(f"Invalid indicateur format: '{indicateur}'. Expected one '.' separator.")
        return parts[0], None
    
    prefix, suffix = parts
    logger.debug(f"Extracted indicateur '{prefix}' with suffix '{suffix}'")
    return prefix, suffix