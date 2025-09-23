"""Configuration settings for the Flight Delays ETL Pipeline."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Settings:
    """Centralized configuration management for the ETL pipeline."""
    
    def __init__(self):
        """Initialize configuration settings from environment variables."""
        # Database Configuration
        self.DATABASE_HOST = os.getenv('DATABASE_HOST', '127.0.0.1')
        self.DATABASE_PORT = int(os.getenv('DATABASE_PORT', '3306'))
        self.DATABASE_USER = os.getenv('DATABASE_USER', 'azalia2')
        self.DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', '123456')
        self.DATABASE_NAME = os.getenv('DATABASE_NAME', 'flights_db')
        
        # Data Source Paths
        self.DATA_PATH = Path(os.getenv('DATA_PATH', 'data/'))
        self.AIRLINES_FILE = os.getenv('AIRLINES_FILE', 'airlines.csv')
        self.AIRPORTS_FILE = os.getenv('AIRPORTS_FILE', 'airports.csv')
        self.FLIGHTS_FILE = os.getenv('FLIGHTS_FILE', 'flights.csv')
        
        # ETL Configuration
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100000'))
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FILE = os.getenv('LOG_FILE', 'logs/etl_pipeline.log')
        
        # Processing Options
        self.SKIP_DATA_VALIDATION = os.getenv('SKIP_DATA_VALIDATION', 'false').lower() == 'true'
        self.TRUNCATE_TABLES = os.getenv('TRUNCATE_TABLES', 'true').lower() == 'true'
        self.CREATE_INDEXES = os.getenv('CREATE_INDEXES', 'true').lower() == 'true'
    
    @property
    def database_url(self) -> str:
        """Get the complete database URL for SQLAlchemy."""
        return (f"mysql+pymysql://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}"
                f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}")
    
    @property
    def airlines_path(self) -> Path:
        """Get the full path to the airlines CSV file."""
        return self.DATA_PATH / self.AIRLINES_FILE
    
    @property
    def airports_path(self) -> Path:
        """Get the full path to the airports CSV file."""
        return self.DATA_PATH / self.AIRPORTS_FILE
    
    @property
    def flights_path(self) -> Path:
        """Get the full path to the flights CSV file."""
        return self.DATA_PATH / self.FLIGHTS_FILE
    
    def validate_file_paths(self) -> list[str]:
        """Validate that all required CSV files exist.
        
        Returns:
            List of missing file paths (empty if all files exist)
        """
        missing_files = []
        
        for file_path in [self.airlines_path, self.airports_path, self.flights_path]:
            if not file_path.exists():
                missing_files.append(str(file_path))
        
        return missing_files
    
    def create_log_directory(self) -> None:
        """Create the log directory if it doesn't exist."""
        log_path = Path(self.LOG_FILE).parent
        log_path.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()
