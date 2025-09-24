"""MySQL database loading module for the Flight Delays ETL Pipeline."""

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, List, Optional, Any
import time

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class MySQLLoader:
    """Handles loading data into MySQL database tables."""
    
    def __init__(self):
        """Initialize the MySQL loader."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
        self.engine = None
        self.session_factory = None
        
    def connect(self) -> bool:
        """Establish connection to MySQL database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info("Connecting to MySQL database")
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                settings.database_url,
                pool_pre_ping=True,  # Verify connections before use
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False           # Set to True for SQL debugging
            )
            
            # Test the connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            self.logger.info("Successfully connected to MySQL database")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
    
    def execute_sql_file(self, sql_file_path: str) -> bool:
        """Execute SQL statements from a file.
        
        Args:
            sql_file_path: Path to SQL file
            
        Returns:
            True if execution successful, False otherwise
        """
        try:
            self.logger.info(f"Executing SQL file: {sql_file_path}")
            
            # Read SQL file
            with open(sql_file_path, 'r') as file:
                sql_content = file.read()
            
            # Split into individual statements
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    for i, statement in enumerate(statements):
                        if statement:
                            self.logger.debug(f"Executing statement {i+1}/{len(statements)}")
                            conn.execute(text(statement))
                    
                    trans.commit()
                    self.logger.info(f"Successfully executed {len(statements)} SQL statements")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    self.logger.error(f"Error executing SQL statement: {e}")
                    raise
                    
        except FileNotFoundError:
            self.logger.error(f"SQL file not found: {sql_file_path}")
            return False
        except Exception as e:
            self.logger.error(f"Error executing SQL file: {e}")
            return False
    
    def create_schema(self) -> bool:
        """Create the database schema using the DDL script.
        
        Returns:
            True if schema creation successful, False otherwise
        """
        ddl_file = "flights_db.sql"
        self.logger.info("Creating database schema")
        return self.execute_sql_file(ddl_file)
    
    def create_views(self) -> bool:
        """Create analytical views using the views script.
        
        Returns:
            True if view creation successful, False otherwise
        """
        views_file = "flights_views.sql"
        self.logger.info("Creating analytical views")
        return self.execute_sql_file(views_file)
    
    def check_table_has_data(self, table_name: str) -> bool:
        """Check if a table has any data records.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table has data, False if empty or doesn't exist
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.fetchone()[0]
                has_data = count > 0
                
                self.logger.debug(f"Table {table_name} has {count:,} records")
                return has_data
                
        except Exception as e:
            # Table might not exist or other error - assume no data
            self.logger.debug(f"Could not check table {table_name}: {e}")
            return False
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        batch_size: Optional[int] = None
    ) -> bool:
        """Load a DataFrame into a database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            batch_size: Number of records to insert per batch
            
        Returns:
            True if loading successful, False otherwise
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame provided for table {table_name}")
            return True
        
        batch_size = batch_size or settings.BATCH_SIZE
        total_records = len(df)
        
        self.logger.info(f"Loading {total_records:,} records into table: {table_name}")
        self.progress_logger.start_process(f"Loading {table_name}", 0)
        
        try:
            start_time = time.time()
            
            # Load data in batches
            records_loaded = 0
            batch_num = 1
            
            for i in range(0, total_records, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                batch_start_time = time.time()
                
                batch_df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists=if_exists if i == 0 else 'append',
                    index=False,
                    method='multi'
                )
                
                records_loaded += len(batch_df)
                batch_time = time.time() - batch_start_time
                records_per_second = len(batch_df) / batch_time if batch_time > 0 else 0
                
                self.progress_logger.log_step(
                    f"Loaded batch {batch_num} ({records_loaded:,}/{total_records:,} records) "
                    f"- {records_per_second:.0f} records/sec",
                    len(batch_df)
                )
                
                batch_num += 1
            
            total_time = time.time() - start_time
            avg_records_per_second = total_records / total_time if total_time > 0 else 0
            
            self.progress_logger.complete_process(
                f"Loading {table_name}",
                total_records
            )
            self.logger.info(
                f"Successfully loaded {total_records:,} records in {total_time:.2f} seconds "
                f"({avg_records_per_second:.0f} records/sec average)"
            )
            
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error loading data into {table_name}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error loading data into {table_name}: {e}")
            return False
    
    def load_dimensions(self, dimensions: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, int]]:
        """Load all dimensional data with smart duplicate handling.
        
        Args:
            dimensions: Dictionary of dimension name -> DataFrame
            
        Returns:
            Dictionary of lookup tables for foreign key resolution
        """
        self.logger.info("Loading dimensional data with smart duplicate handling")
        self.progress_logger.start_process("Dimension Loading", len(dimensions))
        
        lookup_tables = {}
        
        # Define the loading order (dimensions first, then fact)
        dimension_order = ['airlines', 'airports', 'dates', 'times', 'delay_causes']
        
        for dim_name in dimension_order:
            if dim_name not in dimensions:
                self.logger.warning(f"Dimension {dim_name} not found in input data")
                continue
            
            df = dimensions[dim_name]
            table_name = f"dim_{dim_name[:-1]}" if dim_name.endswith('s') else f"dim_{dim_name}"
            
            # Special handling for dimension table names
            table_mapping = {
                'airlines': 'dim_airline',
                'airports': 'dim_airport',
                'dates': 'dim_date',
                'times': 'dim_time',
                'delay_causes': 'dim_delay_cause'
            }
            table_name = table_mapping.get(dim_name, table_name)
            
            # Smart loading: check if table already has data
            if self.check_table_has_data(table_name):
                self.logger.info(f"Table {table_name} already contains data - skipping insert, creating lookup from existing data")
                lookup_tables[dim_name] = self._create_lookup_table(table_name, dim_name)
                self.progress_logger.log_step(f"Reused existing dimension: {dim_name}", 0)
            else:
                # Table is empty, safe to insert new data
                self.logger.info(f"Table {table_name} is empty - inserting {len(df):,} records")
                success = self.load_dataframe(df, table_name)
                
                if success:
                    # Create lookup table for foreign key resolution
                    lookup_tables[dim_name] = self._create_lookup_table(table_name, dim_name)
                    self.progress_logger.log_step(f"Loaded new dimension: {dim_name}", len(df))
                else:
                    self.logger.error(f"Failed to load dimension: {dim_name}")
                    return {}
        
        total_records = sum(len(df) for df in dimensions.values())
        self.progress_logger.complete_process("Dimension Loading", total_records)
        return lookup_tables
    
    def load_fact_data(self, fact_df: pd.DataFrame) -> bool:
        """Load fact table data.
        
        Args:
            fact_df: DataFrame containing fact data
            
        Returns:
            True if loading successful, False otherwise
        """
        table_name = 'fact_flights'
        
        return self.load_dataframe(fact_df, table_name)
    
    def _create_lookup_table(self, table_name: str, dimension_name: str) -> Dict[str, int]:
        """Create a lookup dictionary for a dimension table.
        
        Args:
            table_name: Database table name
            dimension_name: Logical dimension name
            
        Returns:
            Dictionary mapping dimension codes to keys
        """
        try:
            # Define the code column for each dimension
            code_columns = {
                'airlines': 'airline_code',
                'airports': 'airport_code',
                'dates': 'date_key',
                'times': 'time_value',
                'delay_causes': 'cause_code'
            }
            
            code_column = code_columns.get(dimension_name)
            if not code_column:
                self.logger.warning(f"No code column defined for dimension: {dimension_name}")
                return {}
            
            # Query the database to get the lookup data
            key_column = f"{dimension_name[:-1]}_key" if dimension_name.endswith('s') else f"{dimension_name}_key"
            
            # Special handling for dimension key names
            key_mapping = {
                'airlines': 'airline_key',
                'airports': 'airport_key', 
                'dates': 'date_key',
                'times': 'time_key',
                'delay_causes': 'delay_cause_key'
            }
            key_column = key_mapping.get(dimension_name, key_column)
            
            query = f"SELECT {key_column}, {code_column} FROM {table_name}"
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                lookup_dict = {row[1]: row[0] for row in result.fetchall()}
            
            self.logger.info(f"Created lookup table for {dimension_name} with {len(lookup_dict)} entries")
            return lookup_dict
            
        except Exception as e:
            self.logger.error(f"Error creating lookup table for {dimension_name}: {e}")
            return {}
    
    def create_indexes(self) -> bool:
        """Create database indexes for optimal query performance.
        
        Returns:
            True if index creation successful, False otherwise
        """
        if not settings.CREATE_INDEXES:
            self.logger.info("Index creation disabled in configuration")
            return True
        
        self.logger.info("Creating database indexes")
        
        # Index definitions (these should match the DDL script)
        index_statements = [
            # Drop existing indexes first, then create new ones
            "DROP INDEX IF EXISTS idx_fact_flights_date ON fact_flights",
            "CREATE INDEX idx_fact_flights_date ON fact_flights(date_key)",
            
            "DROP INDEX IF EXISTS idx_fact_flights_airline ON fact_flights", 
            "CREATE INDEX idx_fact_flights_airline ON fact_flights(airline_key)",
            
            "DROP INDEX IF EXISTS idx_fact_flights_origin ON fact_flights",
            "CREATE INDEX idx_fact_flights_origin ON fact_flights(origin_airport_key)",
            
            "DROP INDEX IF EXISTS idx_fact_flights_destination ON fact_flights",
            "CREATE INDEX idx_fact_flights_destination ON fact_flights(destination_airport_key)",
            
            "DROP INDEX IF EXISTS idx_fact_flights_delay_cause ON fact_flights",
            "CREATE INDEX idx_fact_flights_delay_cause ON fact_flights(delay_cause_key)",
            
            "DROP INDEX IF EXISTS idx_fact_flights_departure_delay ON fact_flights",
            "CREATE INDEX idx_fact_flights_departure_delay ON fact_flights(departure_delay_minutes)"
        ]
        
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    for i, statement in enumerate(index_statements):
                        self.logger.debug(f"Creating index {i+1}/{len(index_statements)}")
                        conn.execute(text(statement))
                    
                    trans.commit()
                    self.logger.info(f"Successfully created {len(index_statements)} indexes")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    self.logger.error(f"Error creating indexes: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Unexpected error creating indexes: {e}")
            return False
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a database table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        try:
            stats = {}
            
            with self.engine.connect() as conn:
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats['row_count'] = result.fetchone()[0]
                
                # Get table size information
                size_query = """
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                FROM information_schema.TABLES 
                WHERE table_schema = DATABASE() AND table_name = :table_name
                """
                result = conn.execute(text(size_query), {"table_name": table_name})
                size_row = result.fetchone()
                stats['size_mb'] = size_row[0] if size_row else 0
                
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting stats for table {table_name}: {e}")
            return {}
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        """Validate data integrity across all tables.
        
        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating data integrity")
        
        validation_results = {}
        
        try:
            with self.engine.connect() as conn:
                # Check for orphaned records in fact table
                orphan_queries = {
                    'orphaned_airlines': """
                        SELECT COUNT(*) FROM fact_flights f 
                        LEFT JOIN dim_airline a ON f.airline_key = a.airline_key 
                        WHERE a.airline_key IS NULL AND f.airline_key > 0
                    """,
                    'orphaned_origin_airports': """
                        SELECT COUNT(*) FROM fact_flights f 
                        LEFT JOIN dim_airport a ON f.origin_airport_key = a.airport_key 
                        WHERE a.airport_key IS NULL AND f.origin_airport_key > 0
                    """,
                    'orphaned_destination_airports': """
                        SELECT COUNT(*) FROM fact_flights f 
                        LEFT JOIN dim_airport a ON f.destination_airport_key = a.airport_key 
                        WHERE a.airport_key IS NULL AND f.destination_airport_key > 0
                    """,
                    'orphaned_dates': """
                        SELECT COUNT(*) FROM fact_flights f 
                        LEFT JOIN dim_date d ON f.date_key = d.date 
                        WHERE d.date IS NULL
                    """,
                    'orphaned_delay_causes': """
                        SELECT COUNT(*) FROM fact_flights f 
                        LEFT JOIN dim_delay_cause dc ON f.delay_cause_key = dc.delay_cause_key 
                        WHERE dc.delay_cause_key IS NULL AND f.delay_cause_key > 0
                    """
                }
                
                for check_name, query in orphan_queries.items():
                    result = conn.execute(text(query))
                    validation_results[check_name] = result.fetchone()[0]
                
                # Check for data quality issues
                quality_queries = {
                    'negative_distances': "SELECT COUNT(*) FROM fact_flights WHERE distance_miles < 0",
                    'extreme_delays': "SELECT COUNT(*) FROM fact_flights WHERE departure_delay_minutes > 1440",
                    'cancelled_but_not_flagged': """
                        SELECT COUNT(*) FROM fact_flights 
                        WHERE departure_delay_minutes IS NULL AND is_cancelled = 0
                    """
                }
                
                for check_name, query in quality_queries.items():
                    result = conn.execute(text(query))
                    validation_results[check_name] = result.fetchone()[0]
                
            self.logger.info("Data integrity validation completed")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating data integrity: {e}")
            return {}
