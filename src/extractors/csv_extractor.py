"""CSV data extraction module for the Flight Delays ETL Pipeline."""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from datetime import datetime

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class CSVExtractor:
    """Handles extraction of data from CSV source files."""
    
    def __init__(self):
        """Initialize the CSV extractor."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def extract_airlines(self) -> pd.DataFrame:
        """Extract airline data from CSV file.
        
        Returns:
            DataFrame with airline information
            
        Raises:
            FileNotFoundError: If airlines.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.airlines_path
        self.logger.info(f"Extracting airlines data from {file_path}")
        
        try:
            # Read airlines CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'IATA_CODE': 'string',
                    'AIRLINE': 'string'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a']
            )
            
            # Validate required columns
            required_columns = ['IATA_CODE', 'AIRLINE']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in airlines.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['IATA_CODE', 'AIRLINE'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing airline data")
            
            self.logger.info(f"Successfully extracted {len(df)} airline records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Airlines file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Airlines file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading airlines file: {e}")
            raise
    
    def extract_airports(self) -> pd.DataFrame:
        """Extract airport data from CSV file.
        
        Returns:
            DataFrame with airport information
            
        Raises:
            FileNotFoundError: If airports.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.airports_path
        self.logger.info(f"Extracting airports data from {file_path}")
        
        try:
            # Read airports CSV with specific data types
            df = pd.read_csv(
                file_path,
                dtype={
                    'IATA_CODE': 'string',
                    'AIRPORT': 'string',
                    'CITY': 'string',
                    'STATE': 'string',
                    'COUNTRY': 'string',
                    'LATITUDE': 'float64',
                    'LONGITUDE': 'float64'
                },
                na_values=['', 'NULL', 'null', 'N/A', 'n/a']
            )
            
            # Validate required columns
            required_columns = ['IATA_CODE', 'AIRPORT', 'CITY', 'STATE', 'COUNTRY']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in airports.csv: {missing_columns}")
            
            # Basic data validation
            initial_count = len(df)
            df = df.dropna(subset=['IATA_CODE', 'AIRPORT', 'CITY'])
            final_count = len(df)
            
            if initial_count != final_count:
                self.logger.warning(f"Dropped {initial_count - final_count} rows with missing airport data")
            
            self.logger.info(f"Successfully extracted {len(df)} airport records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Airports file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Airports file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading airports file: {e}")
            raise
    
    def extract_flights(self, chunk_size: Optional[int] = None) -> pd.DataFrame:
        """Extract flight data from CSV file.
        
        Args:
            chunk_size: Number of rows to read at once (for memory management)
            
        Returns:
            DataFrame with flight information
            
        Raises:
            FileNotFoundError: If flights.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        file_path = settings.flights_path
        chunk_size = chunk_size or settings.BATCH_SIZE
        
        self.logger.info(f"Extracting flights data from {file_path}")
        self.logger.info(f"Using chunk size: {chunk_size:,}")
        
        try:
            # Get total number of rows for progress tracking
            total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
            self.logger.info(f"Total flight records to process: {total_rows:,}")
            
            # Read flights CSV in chunks
            chunk_list = []
            processed_rows = 0
            
            # Define data types for key columns
            dtype_dict = {
                'YEAR': 'int64',
                'MONTH': 'int64',
                'DAY': 'int64',
                'DAY_OF_WEEK': 'int64',
                'AIRLINE': 'string',
                'FLIGHT_NUMBER': 'int64',
                'TAIL_NUMBER': 'string',
                'ORIGIN_AIRPORT': 'string',
                'DESTINATION_AIRPORT': 'string',
                'SCHEDULED_DEPARTURE': 'string',
                'DEPARTURE_TIME': 'string',
                'DEPARTURE_DELAY': 'float64',
                'TAXI_OUT': 'float64',
                'WHEELS_OFF': 'string',
                'SCHEDULED_TIME': 'float64',
                'ELAPSED_TIME': 'float64',
                'AIR_TIME': 'float64',
                'DISTANCE': 'int64',
                'WHEELS_ON': 'string',
                'TAXI_IN': 'float64',
                'SCHEDULED_ARRIVAL': 'string',
                'ARRIVAL_TIME': 'string',
                'ARRIVAL_DELAY': 'float64',
                'DIVERTED': 'int64',
                'CANCELLED': 'int64',
                'CANCELLATION_REASON': 'string',
                'AIR_SYSTEM_DELAY': 'float64',
                'SECURITY_DELAY': 'float64',
                'AIRLINE_DELAY': 'float64',
                'LATE_AIRCRAFT_DELAY': 'float64',
                'WEATHER_DELAY': 'float64'
            }
            
            chunk_reader = pd.read_csv(
                file_path,
                chunksize=chunk_size,
                dtype=dtype_dict,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a'],
                low_memory=False
            )
            
            for chunk_num, chunk in enumerate(chunk_reader, 1):
                processed_rows += len(chunk)
                progress_pct = (processed_rows / total_rows) * 100
                
                self.logger.info(
                    f"Processing chunk {chunk_num}: {len(chunk):,} records "
                    f"({processed_rows:,}/{total_rows:,} - {progress_pct:.1f}%)"
                )
                
                chunk_list.append(chunk)
            
            # Combine all chunks
            self.logger.info("Combining all chunks into single DataFrame")
            df = pd.concat(chunk_list, ignore_index=True)
            
            # Validate required columns
            required_columns = [
                'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT', 
                'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE'
            ]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in flights.csv: {missing_columns}")
            
            self.logger.info(f"Successfully extracted {len(df):,} flight records")
            return df
            
        except FileNotFoundError:
            self.logger.error(f"Flights file not found: {file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"Flights file is empty: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading flights file: {e}")
            raise
    
    def validate_source_files(self) -> Dict[str, bool]:
        """Validate that all required source files exist and are readable.
        
        Returns:
            Dictionary mapping file names to their validation status
        """
        self.logger.info("Validating source CSV files")
        
        validation_results = {}
        files_to_check = {
            'airlines': settings.airlines_path,
            'airports': settings.airports_path,
            'flights': settings.flights_path
        }
        
        for file_name, file_path in files_to_check.items():
            try:
                if not file_path.exists():
                    self.logger.error(f"{file_name}.csv not found: {file_path}")
                    validation_results[file_name] = False
                    continue
                
                # Try to read the first few rows to validate file structure
                df_sample = pd.read_csv(file_path, nrows=5)
                if df_sample.empty:
                    self.logger.error(f"{file_name}.csv is empty")
                    validation_results[file_name] = False
                else:
                    self.logger.info(f"{file_name}.csv validation passed ({len(df_sample.columns)} columns)")
                    validation_results[file_name] = True
                    
            except Exception as e:
                self.logger.error(f"Error validating {file_name}.csv: {e}")
                validation_results[file_name] = False
        
        return validation_results
    
    def get_data_summary(self) -> Dict[str, Dict[str, any]]:
        """Get summary information about all source data files.
        
        Returns:
            Dictionary with summary statistics for each file
        """
        self.logger.info("Generating data summary")
        
        summary = {}
        
        try:
            # Airlines summary
            airlines_df = self.extract_airlines()
            summary['airlines'] = {
                'record_count': len(airlines_df),
                'columns': list(airlines_df.columns),
                'unique_airlines': airlines_df['IATA_CODE'].nunique()
            }
            
            # Airports summary
            airports_df = self.extract_airports()
            summary['airports'] = {
                'record_count': len(airports_df),
                'columns': list(airports_df.columns),
                'unique_airports': airports_df['IATA_CODE'].nunique(),
                'countries': airports_df['COUNTRY'].nunique() if 'COUNTRY' in airports_df.columns else 0
            }
            
            # Flights summary (sample only for performance)
            flights_sample = pd.read_csv(settings.flights_path, nrows=10000)
            total_flights = sum(1 for _ in open(settings.flights_path)) - 1
            
            summary['flights'] = {
                'record_count': total_flights,
                'sample_size': len(flights_sample),
                'columns': list(flights_sample.columns),
                'date_range': {
                    'min_year': flights_sample['YEAR'].min(),
                    'max_year': flights_sample['YEAR'].max()
                } if 'YEAR' in flights_sample.columns else None,
                'unique_airlines': flights_sample['AIRLINE'].nunique() if 'AIRLINE' in flights_sample.columns else 0
            }
            
        except Exception as e:
            self.logger.error(f"Error generating data summary: {e}")
            summary['error'] = str(e)
        
        return summary
