"""Data transformation module for fact tables in the Flight Delays ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import numpy as np

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class FactTransformer:
    """Handles transformation of data for the fact_flights table."""
    
    def __init__(self):
        """Initialize the fact transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_flights(
        self, 
        flights_df: pd.DataFrame,
        dim_lookups: Dict[str, Dict[str, int]],
        batch_start_row: int = 1
    ) -> pd.DataFrame:
        """Transform flights data for fact_flights table.
        
        Args:
            flights_df: Raw flights DataFrame from CSV
            dim_lookups: Dictionary containing dimension key lookups
                        Format: {'airlines': {'AA': 1, 'DL': 2}, 'airports': {...}, etc.}
            batch_start_row: Starting row number for this batch (for error logging)
            
        Returns:
            Transformed DataFrame ready for fact_flights table
        """
        self.logger.info(f"Transforming {len(flights_df):,} flight records for fact table")
        self.progress_logger.start_process("Flight Fact Transformation", 8)
        
        # Create a copy to avoid modifying the original
        df = flights_df.copy()
        
        # Step 1: Row-level data cleaning with detailed logging
        self.progress_logger.log_step("Row-level data cleaning and validation")
        df = self._clean_flight_data_with_row_logging(df, batch_start_row)
        
        # Step 2: Create date dimension keys
        self.progress_logger.log_step("Creating date dimension keys")
        df = self._add_date_keys(df)
        
        # Step 3: Create time dimension keys
        self.progress_logger.log_step("Creating time dimension keys")
        df = self._add_time_keys(df)
        
        # Step 4: Add dimension foreign keys
        self.progress_logger.log_step("Adding dimension foreign keys")
        df = self._add_dimension_keys(df, dim_lookups)
        
        # Step 5: Calculate delay metrics
        self.progress_logger.log_step("Calculating delay metrics and derived measures")
        df = self._calculate_delay_metrics(df)
        
        # Step 6: Determine delay causes
        self.progress_logger.log_step("Determining primary delay causes")
        df = self._determine_delay_causes(df, dim_lookups.get('delay_causes', {}))
        
        # Step 7: Add business flags
        self.progress_logger.log_step("Adding business flags and indicators")
        df = self._add_business_flags(df)
        
        # Step 8: Final cleanup and column selection
        self.progress_logger.log_step("Final cleanup and column selection")
        df = self._finalize_fact_data(df)
        
        self.progress_logger.complete_process("Flight Fact Transformation", len(df))
        return df
    
    def _clean_flight_data_with_row_logging(self, df: pd.DataFrame, batch_start_row: int) -> pd.DataFrame:
        """Clean flight data with row-level error logging and filtering.
        
        Args:
            df: Raw flights DataFrame batch
            batch_start_row: Starting row number for this batch (for error logging)
            
        Returns:
            Cleaned DataFrame with problematic rows removed
        """
        initial_count = len(df)
        
        # Define critical columns that must have data
        critical_columns = ['YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT']
        
        # Check for missing columns in the batch
        missing_columns = [col for col in critical_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Batch missing entire columns: {missing_columns}")
            return pd.DataFrame()  # Return empty DataFrame if critical columns missing
        
        # Add row indices for logging
        df = df.reset_index(drop=True)
        
        # Identify rows with missing critical data
        missing_mask = df[critical_columns].isnull().any(axis=1)
        bad_rows_df = df[missing_mask]
        
        # Log specific problematic rows with detailed information
        if len(bad_rows_df) > 0:
            for idx, row in bad_rows_df.iterrows():
                actual_row_number = batch_start_row + idx
                missing_cols = [col for col in critical_columns if pd.isnull(row[col])]
                self.logger.warning(f"Skipping row {actual_row_number:,}: missing critical data in columns {missing_cols}")
        
        # Filter out rows with missing critical data
        df_clean = df[~missing_mask].copy()
        
        # Standardize airline and airport codes for remaining rows
        if 'AIRLINE' in df_clean.columns:
            df_clean['AIRLINE'] = df_clean['AIRLINE'].str.strip().str.upper()
        if 'ORIGIN_AIRPORT' in df_clean.columns:
            df_clean['ORIGIN_AIRPORT'] = df_clean['ORIGIN_AIRPORT'].str.strip().str.upper()
        if 'DESTINATION_AIRPORT' in df_clean.columns:
            df_clean['DESTINATION_AIRPORT'] = df_clean['DESTINATION_AIRPORT'].str.strip().str.upper()
        
        # Clean flight numbers and tail numbers
        df_clean['flight_number'] = pd.to_numeric(df_clean['FLIGHT_NUMBER'], errors='coerce').fillna(0).astype(int)
        df_clean['tail_number'] = df_clean['TAIL_NUMBER'].fillna('').astype(str) if 'TAIL_NUMBER' in df_clean.columns else ''
        
        # Handle cancelled and diverted flags
        df_clean['CANCELLED'] = df_clean['CANCELLED'].fillna(0).astype(int)
        df_clean['DIVERTED'] = df_clean['DIVERTED'].fillna(0).astype(int)
        
        # Clean delay columns - set NaN to 0 for non-cancelled flights
        delay_columns = ['DEPARTURE_DELAY', 'ARRIVAL_DELAY']
        for col in delay_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna(0)
        
        # Clean time columns
        time_columns = ['SCHEDULED_TIME', 'ELAPSED_TIME', 'AIR_TIME']
        for col in time_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
        
        # Clean distance
        if 'DISTANCE' in df_clean.columns:
            df_clean['DISTANCE'] = pd.to_numeric(df_clean['DISTANCE'], errors='coerce').fillna(0)
        
        cleaned_count = len(df_clean)
        dropped_count = initial_count - cleaned_count
        
        if dropped_count > 0:
            self.logger.info(f"Row-level filtering: kept {cleaned_count:,}/{initial_count:,} rows, skipped {dropped_count:,} rows with missing data")
        
        return df_clean
    
    def _add_date_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add date dimension keys to the DataFrame.
        
        Args:
            df: Flights DataFrame
            
        Returns:
            DataFrame with date keys added
        """
        # Create flight date from year, month, day
        df['flight_date'] = pd.to_datetime(
            df[['YEAR', 'MONTH', 'DAY']].rename(columns={
                'YEAR': 'year', 'MONTH': 'month', 'DAY': 'day'
            })
        )
        
        # Create date key as integer (YYYYMMDD format) to match dim_date
        df['date_key'] = df['flight_date'].dt.strftime('%Y%m%d').astype(int)
        
        return df
    
    def _add_time_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add time dimension keys to the DataFrame.
        
        Args:
            df: Flights DataFrame
            
        Returns:
            DataFrame with time keys added
        """
        # Parse scheduled departure time to string format
        df['departure_time_string'] = self._parse_time_to_key(df['SCHEDULED_DEPARTURE'])
        # Initialize departure_time_key as 0 (will be resolved later with lookup)
        df['departure_time_key'] = 0
        
        return df
    
    def _parse_time_to_key(self, time_series: pd.Series) -> pd.Series:
        """Parse time strings to create time dimension keys (mapped to closest hour).
        
        Args:
            time_series: Series containing time strings (e.g., '1430' for 2:30 PM)
            
        Returns:
            Series of time keys in HH:00 format (rounded to nearest hour)
        """
        def parse_single_time(time_str) -> str:
            if pd.isna(time_str):
                return '00:00'  # Default time
            
            try:
                # Convert to string and pad with zeros if necessary
                time_str = str(int(float(time_str))).zfill(4)
                
                # Extract hour and minute
                hour = int(time_str[:2])
                minute = int(time_str[2:])
                
                # Validate hour and minute
                if hour >= 24:
                    hour = hour % 24
                if minute >= 60:
                    minute = 0
                
                # Round to nearest hour (30+ minutes rounds up)
                if minute >= 30:
                    hour = (hour + 1) % 24
                
                # Return hourly slot format to match 24-hour dimension
                return f"{hour:02d}:00"
                
            except (ValueError, TypeError):
                return '00:00'  # Default for unparseable times
        
        return time_series.apply(parse_single_time)
    
    def _add_dimension_keys(self, df: pd.DataFrame, dim_lookups: Dict[str, Dict[str, int]]) -> pd.DataFrame:
        """Add foreign keys for dimensional tables.
        
        Args:
            df: Flights DataFrame
            dim_lookups: Dictionary containing dimension key lookups
            
        Returns:
            DataFrame with dimension keys added
        """
        # Airline keys
        if 'airlines' in dim_lookups:
            df['airline_key'] = df['AIRLINE'].map(dim_lookups['airlines'])
            unmapped_airlines = df[df['airline_key'].isna()]['AIRLINE'].nunique()
            if unmapped_airlines > 0:
                self.logger.warning(f"{unmapped_airlines} unique airlines could not be mapped to dimension")
            df['airline_key'] = df['airline_key'].fillna(0).astype(int)
        
        # Origin airport keys
        if 'airports' in dim_lookups:
            df['origin_airport_key'] = df['ORIGIN_AIRPORT'].map(dim_lookups['airports'])
            unmapped_origins = df[df['origin_airport_key'].isna()]['ORIGIN_AIRPORT'].nunique()
            if unmapped_origins > 0:
                self.logger.warning(f"{unmapped_origins} unique origin airports could not be mapped")
            
            # Destination airport keys
            df['destination_airport_key'] = df['DESTINATION_AIRPORT'].map(dim_lookups['airports'])
            unmapped_destinations = df[df['destination_airport_key'].isna()]['DESTINATION_AIRPORT'].nunique()
            if unmapped_destinations > 0:
                self.logger.warning(f"{unmapped_destinations} unique destination airports could not be mapped")
            
            # Remove flights with unmapped airports to maintain referential integrity
            initial_count = len(df)
            df = df.dropna(subset=['origin_airport_key', 'destination_airport_key'])
            removed_count = initial_count - len(df)
            if removed_count > 0:
                self.logger.warning(f"Removed {removed_count} flights with unmapped airports")
            
            # Convert to int
            df['origin_airport_key'] = df['origin_airport_key'].astype(int)
            df['destination_airport_key'] = df['destination_airport_key'].astype(int)
        
        # Time keys - convert HH:MM to HH:MM:00 format for lookup
        if 'times' in dim_lookups:
            df['departure_time_full'] = df['departure_time_string'] + ':00'
            df['departure_time_key'] = df['departure_time_full'].map(dim_lookups['times'])
            unmapped_times = df[df['departure_time_key'].isna()]['departure_time_string'].nunique()
            if unmapped_times > 0:
                self.logger.warning(f"{unmapped_times} unique departure times could not be mapped")
            df['departure_time_key'] = df['departure_time_key'].fillna(0).astype(int)
        
        return df
    
    def _calculate_delay_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate delay metrics and derived measures.
        
        Args:
            df: Flights DataFrame
            
        Returns:
            DataFrame with calculated metrics
        """
        # Standardize delay columns
        df['departure_delay_minutes'] = df['DEPARTURE_DELAY'].fillna(0)
        df['arrival_delay_minutes'] = df['ARRIVAL_DELAY'].fillna(0)
        
        # Calculate scheduled and actual elapsed time
        df['scheduled_elapsed_minutes'] = df['SCHEDULED_TIME'].fillna(0)
        df['actual_elapsed_minutes'] = df['ELAPSED_TIME'].fillna(0)
        
        # Distance in miles
        df['distance_miles'] = df['DISTANCE'].fillna(0)
        
        # Calculate derived metrics
        df['delay_indicator'] = (df['departure_delay_minutes'] > 0).astype(int)
        df['significant_delay'] = (df['departure_delay_minutes'] > 15).astype(int)
        df['severe_delay'] = (df['departure_delay_minutes'] > 60).astype(int)
        
        # Calculate delay recovery (if arrival delay < departure delay)
        df['delay_recovery'] = np.where(
            (df['departure_delay_minutes'] > 0) & (df['arrival_delay_minutes'] < df['departure_delay_minutes']),
            df['departure_delay_minutes'] - df['arrival_delay_minutes'],
            0
        )
        
        return df
    
    def _determine_delay_causes(self, df: pd.DataFrame, delay_cause_lookups: Dict[str, int]) -> pd.DataFrame:
        """Determine the primary cause of each delay.
        
        Args:
            df: Flights DataFrame
            delay_cause_lookups: Dictionary mapping delay cause codes to keys
            
        Returns:
            DataFrame with delay cause keys added
        """
        # Define the delay cause columns from the CSV
        delay_cause_columns = [
            'AIR_SYSTEM_DELAY',
            'SECURITY_DELAY', 
            'AIRLINE_DELAY',
            'LATE_AIRCRAFT_DELAY',
            'WEATHER_DELAY'
        ]
        
        # Fill NaN values with 0 for delay cause columns
        for col in delay_cause_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        def determine_primary_cause(row) -> str:
            """Determine the primary delay cause for a single flight."""
            # If no departure delay, return no delay
            if row.get('departure_delay_minutes', 0) <= 0:
                return 'NO_DELAY'
            
            # Check each delay cause type
            delay_causes = {}
            if 'AIR_SYSTEM_DELAY' in row and row['AIR_SYSTEM_DELAY'] > 0:
                delay_causes['AIR_SYSTEM'] = row['AIR_SYSTEM_DELAY']
            if 'SECURITY_DELAY' in row and row['SECURITY_DELAY'] > 0:
                delay_causes['SECURITY'] = row['SECURITY_DELAY']
            if 'AIRLINE_DELAY' in row and row['AIRLINE_DELAY'] > 0:
                delay_causes['CARRIER'] = row['AIRLINE_DELAY']
            if 'LATE_AIRCRAFT_DELAY' in row and row['LATE_AIRCRAFT_DELAY'] > 0:
                delay_causes['LATE_AIRCRAFT'] = row['LATE_AIRCRAFT_DELAY']
            if 'WEATHER_DELAY' in row and row['WEATHER_DELAY'] > 0:
                delay_causes['WEATHER'] = row['WEATHER_DELAY']
            
            # If no specific cause found, return unknown
            if not delay_causes:
                return 'UNKNOWN'
            
            # Return the cause with the maximum delay time
            return max(delay_causes.items(), key=lambda x: x[1])[0]
        
        # Apply the function to determine primary delay cause
        df['primary_delay_cause'] = df.apply(determine_primary_cause, axis=1)
        
        # Map to delay cause dimension keys
        df['delay_cause_key'] = df['primary_delay_cause'].map(delay_cause_lookups)
        df['delay_cause_key'] = df['delay_cause_key'].fillna(
            delay_cause_lookups.get('UNKNOWN', 7)  # Default to UNKNOWN
        ).astype(int)
        
        return df
    
    def _add_business_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add business flags and indicators.
        
        Args:
            df: Flights DataFrame
            
        Returns:
            DataFrame with business flags added
        """
        # Cancellation and diversion flags
        df['is_cancelled'] = df['CANCELLED'].astype(bool)
        df['is_diverted'] = df['DIVERTED'].astype(bool)
        
        # Weekend flight indicator
        df['is_weekend_flight'] = df['flight_date'].dt.dayofweek.isin([5, 6])  # Saturday = 5, Sunday = 6
        
        # Holiday period indicator (simplified)
        holiday_months = [11, 12, 6, 7]  # Nov, Dec, June, July
        df['is_holiday_period'] = df['MONTH'].isin(holiday_months)
        
        # Long-haul flight indicator (flights over 1500 miles)
        df['is_long_haul'] = df['distance_miles'] > 1500
        
        return df
    
    def _finalize_fact_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final cleanup and column selection for fact table.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Final DataFrame ready for database loading
        """
        # Select only the columns needed for the fact table
        fact_columns = [
            'date_key',
            'departure_time_key', 
            'airline_key',
            'origin_airport_key',
            'destination_airport_key',
            'delay_cause_key',
            'flight_number',
            'tail_number',
            'departure_delay_minutes',
            'arrival_delay_minutes',
            'scheduled_elapsed_minutes',
            'actual_elapsed_minutes',
            'distance_miles',
            'is_cancelled',
            'is_diverted'
        ]
        
        # Ensure all required columns exist
        missing_columns = [col for col in fact_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required fact columns: {missing_columns}")
            raise ValueError(f"Missing required fact columns: {missing_columns}")
        
        # Select columns and ensure proper data types
        df_fact = df[fact_columns].copy()
        
        # Ensure integer types for keys
        key_columns = [col for col in fact_columns if col.endswith('_key')]
        for col in key_columns:
            df_fact[col] = df_fact[col].fillna(0).astype(int)
        
        # Ensure float types for measures
        measure_columns = [col for col in fact_columns if 'minutes' in col or 'miles' in col]
        for col in measure_columns:
            df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce').fillna(0)
        
        # Ensure boolean types for flags
        flag_columns = [col for col in fact_columns if col.startswith('is_')]
        for col in flag_columns:
            df_fact[col] = df_fact[col].astype(bool)
        
        # Remove any remaining NaN values
        df_fact = df_fact.fillna(0)
        
        # Validate data ranges
        invalid_delays = df_fact[df_fact['departure_delay_minutes'] < -600]  # More than 10 hours early
        if len(invalid_delays) > 0:
            self.logger.warning(f"Found {len(invalid_delays)} flights with extreme early departures")
        
        extreme_delays = df_fact[df_fact['departure_delay_minutes'] > 1440]  # More than 24 hours late
        if len(extreme_delays) > 0:
            self.logger.warning(f"Found {len(extreme_delays)} flights with extreme delays (>24 hours)")
        
        return df_fact
    
    def get_fact_summary(self, fact_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the fact table.
        
        Args:
            fact_df: Transformed fact DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        summary = {
            'total_flights': len(fact_df),
            'cancelled_flights': fact_df['is_cancelled'].sum(),
            'diverted_flights': fact_df['is_diverted'].sum(),
            'delayed_flights': (fact_df['departure_delay_minutes'] > 0).sum(),
            'on_time_flights': (fact_df['departure_delay_minutes'] <= 0).sum(),
            'delay_statistics': {
                'avg_delay_minutes': fact_df['departure_delay_minutes'].mean(),
                'median_delay_minutes': fact_df['departure_delay_minutes'].median(),
                'max_delay_minutes': fact_df['departure_delay_minutes'].max(),
                'min_delay_minutes': fact_df['departure_delay_minutes'].min()
            },
            'distance_statistics': {
                'avg_distance_miles': fact_df['distance_miles'].mean(),
                'median_distance_miles': fact_df['distance_miles'].median(),
                'max_distance_miles': fact_df['distance_miles'].max()
            },
            'data_quality': {
                'null_counts': fact_df.isnull().sum().to_dict(),
                'zero_distance_flights': (fact_df['distance_miles'] == 0).sum(),
                'missing_keys': {
                    'airline_key_zeros': (fact_df['airline_key'] == 0).sum(),
                    'airport_key_zeros': (fact_df['origin_airport_key'] == 0).sum() + (fact_df['destination_airport_key'] == 0).sum()
                }
            }
        }
        
        return summary
