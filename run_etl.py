#!/usr/bin/env python3
"""Main ETL Pipeline Script for Flight Delays Dimensional Modeling Project.

This script orchestrates the complete ETL process:
1. Extract data from CSV files
2. Transform data for dimensional model
3. Load data into MySQL database
4. Create indexes and validate data integrity

Usage:
    python run_etl.py [options]
    
Options:
    --skip-schema: Skip database schema creation
    --skip-views: Skip analytical views creation
    --skip-validation: Skip data integrity validation
    --sample-size: Process only N flight records (for testing)
    --help: Show this help message
"""

import sys
import argparse
import time
from pathlib import Path
from typing import Dict, Any, Optional

# Add src directory to Python path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config.settings import settings
from src.utils.logging_config import setup_logging, ETLProgressLogger
from src.extractors.csv_extractor import CSVExtractor
from src.transformers.dimension_transformer import DimensionTransformer
from src.transformers.fact_transformer import FactTransformer
from src.loaders.mysql_loader import MySQLLoader


class FlightDelaysETL:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self):
        """Initialize the ETL pipeline."""
        # Setup logging
        self.logger = setup_logging()
        self.progress_logger = ETLProgressLogger(self.logger)
        
        # Initialize components
        self.extractor = CSVExtractor()
        self.dim_transformer = DimensionTransformer()
        self.fact_transformer = FactTransformer()
        self.loader = MySQLLoader()
        
        # Pipeline statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_flights_processed': 0,
            'total_records_loaded': 0,
            'errors': [],
            'warnings': []
        }
    
    def run_pipeline(
        self,
        skip_schema: bool = False,
        skip_views: bool = False,
        skip_validation: bool = False,
        sample_size: Optional[int] = None
    ) -> bool:
        """Run the complete ETL pipeline.
        
        Args:
            skip_schema: Skip database schema creation
            skip_views: Skip analytical views creation
            skip_validation: Skip data integrity validation
            sample_size: Process only N flight records (for testing)
            
        Returns:
            True if pipeline completed successfully, False otherwise
        """
        self.stats['start_time'] = time.time()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info("STARTING FLIGHT DELAYS ETL PIPELINE")
            self.logger.info("=" * 80)
            
            self.progress_logger.start_process("Complete ETL Pipeline", 7)
            
            # Step 1: Validate source files
            if not self._validate_source_files():
                return False
            self.progress_logger.log_step("Source file validation completed")
            
            # Step 2: Setup database connection and schema
            if not self._setup_database(skip_schema):
                return False
            self.progress_logger.log_step("Database setup completed")
            
            # Step 3: Extract data from CSV files
            raw_data = self._extract_data(sample_size)
            if not raw_data:
                return False
            self.progress_logger.log_step("Data extraction completed")
            
            # Step 4: Transform dimensional data
            dimensions = self._transform_dimensions(raw_data)
            if not dimensions:
                return False
            self.progress_logger.log_step("Dimension transformation completed")
            
            # Step 5: Load dimensions and get lookup tables
            lookup_tables = self._load_dimensions(dimensions)
            if not lookup_tables:
                return False
            self.progress_logger.log_step("Dimension loading completed")
            
            # Step 6: Transform and load fact data
            if not self._process_fact_data(raw_data['flights'], lookup_tables):
                return False
            self.progress_logger.log_step("Fact data processing completed")
            
            # Step 7: Post-load operations
            if not self._post_load_operations(skip_views, skip_validation):
                return False
            self.progress_logger.log_step("Post-load operations completed")
            
            self.progress_logger.complete_process("Complete ETL Pipeline")
            
            # Generate final report
            self._generate_final_report()
            
            self.logger.info("=" * 80)
            self.logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed with error: {e}")
            self.stats['errors'].append(str(e))
            return False
        
        finally:
            self.stats['end_time'] = time.time()
            self._cleanup()
    
    def _validate_source_files(self) -> bool:
        """Validate that all required source files exist and are readable."""
        self.logger.info("Validating source CSV files")
        
        # Check if files exist
        missing_files = settings.validate_file_paths()
        if missing_files:
            self.logger.error(f"Missing required files: {missing_files}")
            return False
        
        # Validate file structure
        validation_results = self.extractor.validate_source_files()
        failed_files = [name for name, status in validation_results.items() if not status]
        
        if failed_files:
            self.logger.error(f"File validation failed for: {failed_files}")
            return False
        
        self.logger.info("All source files validated successfully")
        return True
    
    def _setup_database(self, skip_schema: bool) -> bool:
        """Setup database connection and create schema if needed."""
        self.logger.info("Setting up database connection and schema")
        
        # Connect to database
        if not self.loader.connect():
            self.logger.error("Failed to connect to database")
            return False
        
        # Create schema if not skipping
        if not skip_schema:
            if not self.loader.create_schema():
                self.logger.error("Failed to create database schema")
                return False
            self.logger.info("Database schema created successfully")
        else:
            self.logger.info("Skipping database schema creation")
        
        return True
    
    def _extract_data(self, sample_size: Optional[int]) -> Dict[str, Any]:
        """Extract data from all CSV sources."""
        self.logger.info("Extracting data from CSV files")
        
        try:
            raw_data = {}
            
            # Extract airlines
            raw_data['airlines'] = self.extractor.extract_airlines()
            self.logger.info(f"Extracted {len(raw_data['airlines'])} airline records")
            
            # Extract airports
            raw_data['airports'] = self.extractor.extract_airports()
            self.logger.info(f"Extracted {len(raw_data['airports'])} airport records")
            
            # Extract flights (potentially sampled)
            flights_df = self.extractor.extract_flights()
            
            if sample_size and sample_size < len(flights_df):
                self.logger.info(f"Sampling {sample_size:,} flights from {len(flights_df):,} total records")
                flights_df = flights_df.sample(n=sample_size, random_state=42).reset_index(drop=True)
            
            raw_data['flights'] = flights_df
            self.stats['total_flights_processed'] = len(flights_df)
            self.logger.info(f"Processing {len(flights_df):,} flight records")
            
            return raw_data
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            return {}
    
    def _transform_dimensions(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all dimensional data."""
        self.logger.info("Transforming dimensional data")
        
        try:
            dimensions = {}
            
            # Transform airlines
            dimensions['airlines'] = self.dim_transformer.transform_airlines(raw_data['airlines'])
            
            # Transform airports
            dimensions['airports'] = self.dim_transformer.transform_airports(raw_data['airports'])
            
            # Create date dimension
            dimensions['dates'] = self.dim_transformer.create_date_dimension()
            
            # Create time dimension
            dimensions['times'] = self.dim_transformer.create_time_dimension()
            
            # Create delay cause dimension
            dimensions['delay_causes'] = self.dim_transformer.create_delay_cause_dimension()
            
            # Log summary
            summary = self.dim_transformer.get_dimension_summary(dimensions)
            for dim_name, dim_stats in summary.items():
                self.logger.info(f"Dimension {dim_name}: {dim_stats['record_count']:,} records")
            
            return dimensions
            
        except Exception as e:
            self.logger.error(f"Dimension transformation failed: {e}")
            return {}
    
    def _load_dimensions(self, dimensions: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
        """Load dimensional data and return lookup tables."""
        self.logger.info("Loading dimensional data to database")
        
        try:
            lookup_tables = self.loader.load_dimensions(dimensions)
            
            if not lookup_tables:
                self.logger.error("Failed to load dimensional data")
                return {}
            
            # Log lookup table sizes
            for dim_name, lookup_dict in lookup_tables.items():
                self.logger.info(f"Created lookup table for {dim_name}: {len(lookup_dict)} entries")
                self.stats['total_records_loaded'] += len(lookup_dict)
            
            return lookup_tables
            
        except Exception as e:
            self.logger.error(f"Dimension loading failed: {e}")
            return {}
    
    def _process_fact_data(self, flights_df, lookup_tables: Dict[str, Dict[str, int]]) -> bool:
        """Transform and load fact table data."""
        self.logger.info("Processing fact table data")
        
        try:
            # Transform flights data for fact table
            fact_df = self.fact_transformer.transform_flights(flights_df, lookup_tables)
            
            if fact_df.empty:
                self.logger.error("Fact transformation produced empty result")
                return False
            
            # Log transformation summary
            fact_summary = self.fact_transformer.get_fact_summary(fact_df)
            self.logger.info(f"Fact transformation summary:")
            self.logger.info(f"  Total flights: {fact_summary['total_flights']:,}")
            self.logger.info(f"  Delayed flights: {fact_summary['delayed_flights']:,}")
            self.logger.info(f"  Average delay: {fact_summary['delay_statistics']['avg_delay_minutes']:.1f} minutes")
            
            # Load fact data
            if not self.loader.load_fact_data(fact_df):
                self.logger.error("Failed to load fact table data")
                return False
            
            self.stats['total_records_loaded'] += len(fact_df)
            return True
            
        except Exception as e:
            self.logger.error(f"Fact data processing failed: {e}")
            return False
    
    def _post_load_operations(self, skip_views: bool, skip_validation: bool) -> bool:
        """Perform post-load operations like indexing and validation."""
        self.logger.info("Performing post-load operations")
        
        try:
            # Create indexes
            if not self.loader.create_indexes():
                self.logger.warning("Index creation failed, but continuing")
                self.stats['warnings'].append("Index creation failed")
            
            # Create analytical views
            if not skip_views:
                if not self.loader.create_views():
                    self.logger.warning("View creation failed, but continuing")
                    self.stats['warnings'].append("View creation failed")
            else:
                self.logger.info("Skipping analytical views creation")
            
            # Validate data integrity
            if not skip_validation:
                validation_results = self.loader.validate_data_integrity()
                self._log_validation_results(validation_results)
            else:
                self.logger.info("Skipping data integrity validation")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Post-load operations failed: {e}")
            return False
    
    def _log_validation_results(self, validation_results: Dict[str, Any]):
        """Log data validation results."""
        self.logger.info("Data Integrity Validation Results:")
        
        # Check for orphaned records
        orphan_checks = [
            'orphaned_airlines', 'orphaned_origin_airports', 'orphaned_destination_airports',
            'orphaned_dates', 'orphaned_delay_causes'
        ]
        
        total_orphans = sum(validation_results.get(check, 0) for check in orphan_checks)
        if total_orphans > 0:
            self.logger.warning(f"Found {total_orphans} orphaned records")
            for check in orphan_checks:
                count = validation_results.get(check, 0)
                if count > 0:
                    self.logger.warning(f"  {check}: {count}")
        else:
            self.logger.info("No orphaned records found")
        
        # Check for data quality issues
        quality_issues = sum(validation_results.get(check, 0) for check in 
                           ['negative_distances', 'extreme_delays', 'cancelled_but_not_flagged'])
        
        if quality_issues > 0:
            self.logger.warning(f"Found {quality_issues} data quality issues")
        else:
            self.logger.info("No data quality issues found")
    
    def _generate_final_report(self):
        """Generate final ETL pipeline report."""
        # Handle case where end_time might not be set due to early failure
        if self.stats['end_time'] and self.stats['start_time']:
            total_time = self.stats['end_time'] - self.stats['start_time']
        else:
            total_time = 0
        
        self.logger.info("=" * 50)
        self.logger.info("ETL PIPELINE SUMMARY REPORT")
        self.logger.info("=" * 50)
        self.logger.info(f"Total execution time: {total_time:.2f} seconds")
        self.logger.info(f"Flights processed: {self.stats['total_flights_processed']:,}")
        self.logger.info(f"Total records loaded: {self.stats['total_records_loaded']:,}")
        
        if self.stats['warnings']:
            self.logger.info(f"Warnings: {len(self.stats['warnings'])}")
            for warning in self.stats['warnings']:
                self.logger.info(f"  - {warning}")
        
        if self.stats['errors']:
            self.logger.info(f"Errors: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                self.logger.info(f"  - {error}")
        
        # Performance metrics
        if total_time > 0:
            flights_per_sec = self.stats['total_flights_processed'] / total_time
            records_per_sec = self.stats['total_records_loaded'] / total_time
            self.logger.info(f"Processing rate: {flights_per_sec:.0f} flights/sec")
            self.logger.info(f"Loading rate: {records_per_sec:.0f} records/sec")
        
        self.logger.info("=" * 50)
    
    def _cleanup(self):
        """Cleanup resources."""
        if self.loader:
            self.loader.disconnect()


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Flight Delays ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--skip-schema',
        action='store_true',
        help='Skip database schema creation'
    )
    
    parser.add_argument(
        '--skip-views',
        action='store_true',
        help='Skip analytical views creation'
    )
    
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip data integrity validation'
    )
    
    parser.add_argument(
        '--sample-size',
        type=int,
        help='Process only N flight records (for testing)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Create logs directory
    settings.create_log_directory()
    
    # Initialize and run pipeline
    etl = FlightDelaysETL()
    
    success = etl.run_pipeline(
        skip_schema=args.skip_schema,
        skip_views=args.skip_views,
        skip_validation=args.skip_validation,
        sample_size=args.sample_size
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
