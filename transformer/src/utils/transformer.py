import pandas as pd
from typing import Dict, List, Any, Optional
from utils.logger import setup_logging
from utils.tools import (
    connect_database,
    create_tables,
    extract_noeud,
    extract_indicateur_suffixe,
)
from tenacity import retry, stop_after_attempt, wait_fixed
from time import time, sleep
class Transformer:
    def __init__(
        self,
        source_db_config: Dict[str, Any],
        dest_db_config: Dict[str, Any],
        tables: Dict[str, Any],
        all_counters: List[str],
        node_pattern: str,
        suffix_operator_mapping: Dict[str, Any],
        file_path: str,
        data_type: str,
    ) -> None:
        """Initialize the Transformer with database configurations."""
        self.source_conn = connect_database(source_db_config, data_type=data_type)
        self.source_cursor = self.source_conn.cursor()
        self.dest_conn = connect_database(dest_db_config, data_type=data_type)
        self.dest_cursor = self.dest_conn.cursor()
        self.tables = tables
        self.all_counters = all_counters
        self.node_pattern = node_pattern
        self.suffix_operator_mapping = suffix_operator_mapping
        self.file_path = file_path
        self.data_type = data_type
        self.logger = setup_logging("Transformer", data_type=data_type)
        self.source_tables = self.load_tables()
        self.batch_size = 98000  # For bulk inserts

    def load_tables(self) -> List[str]:
        """Load source table names from result_type.txt."""
        try:
            with open(self.file_path, "r") as f:
                tables = [line.strip() for line in f if line.strip()]
            if not tables:
                self.logger.warning(f"No tables found in {self.file_path}. Check file content or path.")
            else:
                self.logger.info(f"Loaded {len(tables)} tables from {self.file_path}: {tables}")
            return tables
        except FileNotFoundError:
            self.logger.error(f"Table file not found: {self.file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading tables from file: {e}")
            raise

    def create_tables(self):
        """Create tables in the destination database."""
        try:
            create_tables(self.dest_cursor, self.tables, self.data_type)
            self.dest_conn.commit()
            self.logger.info("Tables created successfully in destination database.")
        except Exception as e:
            self.logger.error(f"Error creating tables in destination database: {e}")
            self.dest_conn.rollback()
            raise

    def get_distinct_dates(self, table: str) -> List[str]:
        """Retrieve distinct Date values from a table in the source database."""
        try:
            query = f"SELECT DISTINCT Date FROM {table} ORDER BY Date"
            self.source_cursor.execute(query)
            dates = [str(row[0]) for row in self.source_cursor.fetchall()]
            self.logger.info(f"Extracted {len(dates)} distinct dates from {table}")
            return dates
        except Exception as e:
            self.logger.error(f"Error getting distinct dates from {table}: {e}")
            raise

    def extract_node(self, table: str) -> Optional[str]:
        """Extract Node from table name."""
        matches = extract_noeud(self.node_pattern, [table], self.data_type)
        if matches:
            node = matches[0][1]
            self.logger.info(f"Extracted node '{node}' from table '{table}'")
            return node
        self.logger.warning(f"No node found in table name: {table}")
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def extract_data(self, table: str, dates: List[str]) -> pd.DataFrame:
        """Extract relevant counters for multiple dates from the source database."""
        try:
            query = f"""
                SELECT Date, indicateur, valeur
                FROM {table}
                WHERE Date IN %s AND ({' OR '.join(['indicateur LIKE %s' for _ in self.all_counters])})
            """
            params = [tuple(dates)] + [f"{counter}%" for counter in self.all_counters]
            self.source_cursor.execute(query, params)
            data = self.source_cursor.fetchall()
            df = pd.DataFrame(data, columns=["Date", "indicateur", "valeur"])
            if df.empty:
                self.logger.warning(f"No data found for {table} on dates {dates}")
            else:
                self.logger.info(f"Extracted {len(df)} rows for {table} on {len(dates)} dates")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting data from {table} for dates {dates}: {e}")
            raise

    def calculate_kpis(self, counters: Dict[str, float], kpi_configs: Dict[str, Any]) -> Dict[str, float]:
        """Calculate KPI values for a table."""
        kpi_values = {}
        for kpi, config in kpi_configs.items():
            try:
                numerator = [counters.get(counter, 0) for counter in config.get('numerator', [])]
                if 'denominator' in config:
                    denominator = [counters.get(counter, 0) for counter in config.get('denominator', [])]
                    kpi_values[kpi] = config['formula'](numerator, denominator)
                else:
                    kpi_values[kpi] = config['formula'](numerator)
                self.logger.debug(f"Calculated {kpi}: {kpi_values[kpi]}")
            except ZeroDivisionError:
                self.logger.warning(f"ZeroDivisionError for {kpi}: denominator={config.get('denominator', [])}")
                kpi_values[kpi] = None
            except Exception as e:
                self.logger.error(f"Error calculating {kpi}: {e}")
                kpi_values[kpi] = None
        return kpi_values

    def aggregate_by_suffix(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Group counter values by full suffix using vectorized operations."""
        try:
            df['prefix'], df['suffix'] = zip(*df['indicateur'].apply(lambda x: extract_indicateur_suffixe(x, self.data_type)))
            df = df[df['suffix'].notnull() & (df['suffix'] != 'M')]
            df['operator'] = df['suffix'].str.lower().apply(
                lambda s: next((v for k, v in self.suffix_operator_mapping.items() if k and k.lower() in s), 'Other')
            )
            grouped_df = df.groupby(['suffix', 'prefix'])['valeur'].sum().unstack(fill_value=0)
            grouped = {suffix: {'operator': df[df['suffix'] == suffix]['operator'].iloc[0], 'counters': row.to_dict()}
                       for suffix, row in grouped_df.iterrows()}
            unmapped = set(df[df['operator'] == 'Other']['suffix'].unique())
            if unmapped:
                self.logger.warning(f"Unmapped suffixes: {sorted(unmapped)}")
            self.logger.info(f"Grouped data by suffixes: {list(grouped.keys())}")
            return grouped
        except Exception as e:
            self.logger.error(f"Error aggregating by suffix: {str(e)}")
            raise

    def insert_kpi_summary(self, date: str, node: str) -> int:
        """Insert or retrieve kpi_summary ID for a date-node pair."""
        try:
            query = "SELECT id FROM kpi_summary WHERE date = %s AND node = %s"
            self.dest_cursor.execute(query, (date, node))
            result = self.dest_cursor.fetchone()
            if result:
                kpi_id = result[0]
                self.logger.debug(f"Found existing kpi_summary ID={kpi_id} for Date={date}, Node={node}")
                return kpi_id

            query = "INSERT INTO kpi_summary (date, node) VALUES (%s, %s)"
            self.dest_cursor.execute(query, (date, node))
            self.dest_cursor.execute("SELECT LAST_INSERT_ID()")
            kpi_id = self.dest_cursor.fetchone()[0]
            self.logger.info(f"Inserted into kpi_summary: Date={date}, Node={node}, ID={kpi_id}")
            return kpi_id
        except Exception as e:
            self.logger.error(f"Error in kpi_summary upsert: {e}")
            raise

    def insert_kpi_details(self, table_name: str, batch: List[Dict[str, Any]]):
        """Insert batch of KPI values into the specified table with full suffix."""
        try:
            kpi_configs = self.tables[table_name]['kpis']
            columns = ['kpi_id', 'operator', 'suffix'] + list(kpi_configs.keys())
            params = ['%s'] * len(columns)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(params)})"
            values = []
            for row in batch:
                valid_kpis = {kpi: value for kpi, value in row['kpi_values'].items() if value is not None}
                if not valid_kpis:
                    self.logger.warning(f"No valid KPI values for {table_name}: kpi_id={row['kpi_id']}, operator={row['operator']}, suffix={row['suffix']}")
                    continue
                row_values = [row['kpi_id'], row['operator'], row['suffix']] + [valid_kpis.get(kpi, None) for kpi in kpi_configs]
                values.append(tuple(row_values))
            if values:
                self.dest_cursor.executemany(query, values)
                self.dest_conn.commit()
                self.logger.info(f"Inserted batch of {len(values)} rows into {table_name}")
        except Exception as e:
            self.logger.error(f"Error inserting into {table_name}: {e}")
            self.dest_conn.rollback()
            raise

    def process(self):
        """Main process to handle all source tables and insert into destination tables."""
        start = time()
        self.create_tables()
        self.logger.info(f"Table creation: {time() - start:.2f}s")
        batch_data = {table_name: [] for table_name in self.tables}

        for table in self.source_tables:
            table_start = time()
            node = self.extract_node(table)
            if not node:
                continue

            dates = self.get_distinct_dates(table)
            # dates = dates[1364:]  # Removed test skip for production
            
            
            # # print(f"the first date we will process is {dates[0]}")

            
            # # sleep(1000000)  # Simulate delay for testing

        
            # # batch_size = 500 if table in ['traffic_entree', 'traffic_sortie'] else 5
            batch_size = 500
            date_batches = [dates[i:i + batch_size] for i in range(0, len(dates), batch_size)]
            self.logger.info(f"Dates for {table}: {time() - table_start:.2f}s")

            for date_batch in date_batches:
                batch_start = time()
                try:
                    df = self.extract_data(table, date_batch)
                    self.logger.info(f"Extract {table}/{len(date_batch)} dates: {time() - batch_start:.2f}s")
                    if df.empty:
                        self.logger.info(f"No data for table {table}, dates {date_batch}")
                        continue

                    for date, date_df in df.groupby('Date'):
                        date_start = time()
                        if len(date_df) != 196 and table in ['traffic_entree', 'traffic_sortie']:
                            self.logger.warning(f"Expected 196 rows for {table}/{date}, got {len(date_df)}")

                        kpi_id = self.insert_kpi_summary(str(date), node)
                        suffix_data = self.aggregate_by_suffix(date_df)

                        for table_name, table_config in self.tables.items():
                            kpi_configs = table_config['kpis']
                            for suffix, data in suffix_data.items():
                                operator = data['operator']
                                kpi_values = self.calculate_kpis(data['counters'], kpi_configs)
                                if any(v is not None for v in kpi_values.values()):
                                    batch_data[table_name].append({
                                        'kpi_id': kpi_id,
                                        'operator': operator,
                                        'kpi_values': kpi_values,
                                        'suffix': suffix
                                    })
                                    self.logger.debug(f"Added to {table_name} batch: kpi_id={kpi_id}, operator={operator}, suffix={suffix}, kpi_values={kpi_values}")
                                else:
                                    self.logger.warning(f"No valid KPIs for {table_name}: kpi_id={kpi_id}, operator={operator}, suffix={suffix}")

                            if len(batch_data[table_name]) >= self.batch_size:
                                try:
                                    self.insert_kpi_details(table_name, batch_data[table_name])
                                    batch_data[table_name] = []
                                except Exception as e:
                                    self.logger.error(f"Failed to commit batch for {table_name}: {e}")
                                    raise

                        self.logger.info(f"Process {table}/{date}: {time() - date_start:.2f}s")

                except Exception as e:
                    self.logger.error(f"Error processing batch for {table}, dates {date_batch}: {e}")
                    raise

            
            # try:
            #     self.insert_kpi_details(table_name, batch_data[table_name])
            #     batch_data[table_name] = []
            #     self.logger.info(f"Committed final batch of {len(batch_data[table_name])} rows for {table_name}")
            # except Exception as e:
            #     self.logger.error(f"Failed to commit final batch for {table_name}: {e}")
            #     raise
            # self.logger.info("stoped change the table")
            # sleep(100000)

        for table_name in batch_data:
            if batch_data[table_name]:
                try:
                    self.insert_kpi_details(table_name, batch_data[table_name])
                    batch_data[table_name] = []
                    self.logger.info(f"Committed final batch of {len(batch_data[table_name])} rows for {table_name}")
                except Exception as e:
                    self.logger.error(f"Failed to commit final batch for {table_name}: {e}")
                    raise

        self.logger.info(f"Total process: {time() - start:.2f}s")

    def __del__(self):
        """Cleanup database connections safely."""
        try:
            if hasattr(self, 'source_cursor') and self.source_cursor:
                self.source_cursor.close()
            if hasattr(self, 'source_conn') and self.source_conn:
                self.source_conn.close()
            if hasattr(self, 'dest_cursor') and self.dest_cursor:
                self.dest_cursor.close()
            if hasattr(self, 'dest_conn') and self.dest_conn:
                self.dest_conn.close()
            if hasattr(self, 'logger'):
                self.logger.info("Database connections closed.")
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Error closing database connections: {e}")
            else:
                print(f"Error closing database connections: {e}")