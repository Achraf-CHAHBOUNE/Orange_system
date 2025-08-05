import time
from utils.tools import connect_database, process_tables_names, store_txt, extract_table_data
from utils.config import patterns, START_DATE
from utils.logger import setup_logging

# Logging setup
logger = setup_logging("Extractor")

class Extractor:
    def __init__(self, config):
        self.config = config
        self.db = None
        self.cursor = None
        self.tables = None
        self.connect()

    def connect(self):
        """Connect to the database."""
        self.db = connect_database(self.config)  # Retries handled in tools.py
        self.cursor = self.db.cursor()

    def extract_tables_names(self):
        """Extract all table names from the database and store them in a file."""
        try:
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]
            tables_file_path = "./data/our_tables/tables.txt"
            store_txt(tables, tables_file_path)
            self.tables = tables
            logger.info(f"Extracted {len(tables)} table names")
        except Exception as e:
            logger.error(f"Error extracting table names: {e}")
            raise

    def process_tables_names(self):
        """Process table names by filtering and sorting them."""
        try:
            self.extract_tables_names()
            tables_names = process_tables_names(self.tables, patterns, START_DATE)
            logger.info(f"Processed table names: {tables_names}")
            return tables_names
        except Exception as e:
            logger.error(f"Error processing table names: {e}")
            raise

    def extract_table_data(self, table_name, offset, batch_size=5000):
        """Extract data from a specific table in batches with retries."""
        max_retries = 3
        retry_delay = 4

        for attempt in range(max_retries + 1):
            try:
                data = extract_table_data(table_name, self.cursor, offset, batch_size)
                return data
            except Exception as e:
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"Retry {attempt + 1}/{max_retries} for table '{table_name}' after error: {e}. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries ({max_retries}) reached for table '{table_name}': {e}")
                    raise