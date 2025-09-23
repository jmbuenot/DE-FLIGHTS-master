"""Data transformation module for dimensional tables in the Flight Delays ETL Pipeline."""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np

from ..config.settings import settings
from ..utils.logging_config import get_logger, ETLProgressLogger


class DimensionTransformer:
    """Handles transformation of data for dimensional tables."""
    
    def __init__(self):
        """Initialize the dimension transformer."""
        self.logger = get_logger(__name__)
        self.progress_logger = ETLProgressLogger(self.logger)
    
    def transform_airlines(self, airlines_df: pd.DataFrame) -> pd.DataFrame:
        """Transform airlines data for dim_airline table.
        
        Args:
            airlines_df: Raw airlines DataFrame from CSV
            
        Returns:
            Transformed DataFrame ready for dim_airline table
        """
        self.logger.info("Transforming airlines data for dimensional model")
        
        # Create a copy to avoid modifying the original
        df = airlines_df.copy()
        
        # Standardize column names to match dimensional model
        df = df.rename(columns={
            'IATA_CODE': 'airline_code',
            'AIRLINE': 'airline_name'
        })
        
        # Clean and standardize data
        df['airline_code'] = df['airline_code'].str.strip().str.upper()
        df['airline_name'] = df['airline_name'].str.strip()
        
        # Create airline type categorization based on airline name patterns
        df['airline_type'] = self._categorize_airline_type(df['airline_name'])
        
        # Add surrogate key (will be handled by database auto-increment)
        # Reset index to ensure clean sequential numbering
        df = df.reset_index(drop=True)
        
        # Select and order columns for dimensional model
        df = df[['airline_code', 'airline_name', 'airline_type']]
        
        self.logger.info(f"Transformed {len(df)} airline records")
        return df
    
    def transform_airports(self, airports_df: pd.DataFrame) -> pd.DataFrame:
        """Transform airports data for dim_airport table.
        
        Args:
            airports_df: Raw airports DataFrame from CSV
            
        Returns:
            Transformed DataFrame ready for dim_airport table
        """
        self.logger.info("Transforming airports data for dimensional model")
        
        # Create a copy to avoid modifying the original
        df = airports_df.copy()
        
        # Standardize column names to match dimensional model
        df = df.rename(columns={
            'IATA_CODE': 'airport_code',
            'AIRPORT': 'airport_name',
            'CITY': 'city',
            'STATE': 'state',
            'COUNTRY': 'country',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude'
        })
        
        # Clean and standardize data
        df['airport_code'] = df['airport_code'].str.strip().str.upper()
        df['airport_name'] = df['airport_name'].str.strip()
        df['city'] = df['city'].str.strip()
        df['state'] = df['state'].str.strip() if 'state' in df.columns else ''
        df['country'] = df['country'].str.strip() if 'country' in df.columns else 'USA'
        
        # Handle missing coordinates
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # Add timezone (simplified - could be enhanced with actual timezone mapping)
        df['timezone'] = self._determine_timezone(df)
        
        # Add airport size categorization (placeholder - would need flight volume data)
        df['airport_size_category'] = 'Medium'  # Default, could be enhanced later
        
        # Reset index to ensure clean sequential numbering
        df = df.reset_index(drop=True)
        
        # Select and order columns for dimensional model
        columns = [
            'airport_code', 'airport_name', 'city', 'state', 'country',
            'latitude', 'longitude', 'timezone', 'airport_size_category'
        ]
        df = df[columns]
        
        self.logger.info(f"Transformed {len(df)} airport records")
        return df
    
    def create_date_dimension(self, start_date: str = '2015-01-01', end_date: str = '2015-12-31') -> pd.DataFrame:
        """Create date dimension table with business attributes.
        
        Args:
            start_date: Start date for the dimension (YYYY-MM-DD)
            end_date: End date for the dimension (YYYY-MM-DD)
            
        Returns:
            DataFrame ready for dim_date table
        """
        self.logger.info(f"Creating date dimension from {start_date} to {end_date}")
        
        # Create date range
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        dates = pd.date_range(start=start, end=end, freq='D')
        
        # Create DataFrame
        df = pd.DataFrame({'date': dates})
        
        # Add date key as integer (YYYYMMDD format)
        df['date_key'] = df['date'].dt.strftime('%Y%m%d').astype(int)
        
        # Add date attributes
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek + 1  # 1 = Monday, 7 = Sunday
        df['day_name'] = df['date'].dt.day_name()
        df['month_name'] = df['date'].dt.month_name()
        df['quarter'] = df['date'].dt.quarter
        
        # Add additional date attributes
        df['day_of_year'] = df['date'].dt.dayofyear
        df['week_of_year'] = df['date'].dt.isocalendar().week
        df['fiscal_quarter'] = df['quarter']  # Simplified - same as calendar quarter
        
        # Business attributes
        df['is_weekend'] = df['day_of_week'].isin([6, 7])  # Saturday, Sunday
        df['is_holiday'] = self._determine_holidays(df['date'])
        df['is_business_day'] = ~(df['is_weekend'] | df['is_holiday'])
        df['season'] = self._determine_season(df['month'])
        
        # Convert date to string format for database storage
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Reorder columns to match database schema
        df = df[[
            'date_key', 'date', 'year', 'quarter', 'month', 'month_name', 
            'day', 'day_of_week', 'day_name', 'day_of_year', 'week_of_year',
            'is_weekend', 'is_holiday', 'is_business_day', 'season', 'fiscal_quarter'
        ]]
        
        self.logger.info(f"Created {len(df)} date dimension records")
        return df
    
    def create_time_dimension(self) -> pd.DataFrame:
        """Create time dimension table for departure times.
        
        Returns:
            DataFrame ready for dim_time table
        """
        self.logger.info("Creating time dimension for departure times")
        
        # Create 24-hour time entries (every minute)
        times = []
        for hour in range(24):
            for minute in range(60):
                time_str = f"{hour:02d}:{minute:02d}:00"  # Add seconds for TIME format
                times.append({
                    'time_value': time_str,
                    'hour': hour,
                    'minute': minute
                })
        
        df = pd.DataFrame(times)
        
        # Add time categorization
        df['hour_category'] = df['hour'].apply(self._categorize_hour)
        df['is_business_hours'] = df['hour'].between(8, 17)  # 8 AM to 5 PM
        df['time_period'] = df['hour'].apply(self._categorize_time_period)
        
        self.logger.info(f"Created {len(df)} time dimension records")
        return df
    
    def create_delay_cause_dimension(self) -> pd.DataFrame:
        """Create delay cause dimension table.
        
        Returns:
            DataFrame ready for dim_delay_cause table
        """
        self.logger.info("Creating delay cause dimension")
        
        # Define delay cause mappings based on the flights CSV structure
        delay_causes = [
            {'cause_code': 'NO_DELAY', 'cause_name': 'No Delay', 'cause_category': 'On Time'},
            {'cause_code': 'CARRIER', 'cause_name': 'Carrier Delay', 'cause_category': 'Carrier'},
            {'cause_code': 'WEATHER', 'cause_name': 'Weather Delay', 'cause_category': 'Weather'},
            {'cause_code': 'AIR_SYSTEM', 'cause_name': 'Air System Delay', 'cause_category': 'Air System'},
            {'cause_code': 'SECURITY', 'cause_name': 'Security Delay', 'cause_category': 'Security'},
            {'cause_code': 'LATE_AIRCRAFT', 'cause_name': 'Late Aircraft Delay', 'cause_category': 'Late Aircraft'},
            {'cause_code': 'UNKNOWN', 'cause_name': 'Unknown Delay', 'cause_category': 'Unknown'}
        ]
        
        df = pd.DataFrame(delay_causes)
        
        self.logger.info(f"Created {len(df)} delay cause dimension records")
        return df
    
    def _categorize_airline_type(self, airline_names: pd.Series) -> pd.Series:
        """Categorize airlines by type based on their names.
        
        Args:
            airline_names: Series of airline names
            
        Returns:
            Series of airline type categories
        """
        def categorize_single(name: str) -> str:
            name_upper = str(name).upper()
            
            # Major carriers
            major_carriers = ['AMERICAN', 'DELTA', 'UNITED', 'SOUTHWEST', 'JETBLUE']
            if any(carrier in name_upper for carrier in major_carriers):
                return 'Major'
            
            # Regional carriers
            regional_indicators = ['REGIONAL', 'EXPRESS', 'CONNECTION', 'EAGLE', 'SKYWEST']
            if any(indicator in name_upper for indicator in regional_indicators):
                return 'Regional'
            
            # Low-cost carriers
            lowcost_indicators = ['SOUTHWEST', 'SPIRIT', 'FRONTIER', 'ALLEGIANT']
            if any(indicator in name_upper for indicator in lowcost_indicators):
                return 'Low-Cost'
            
            return 'Other'
        
        return airline_names.apply(categorize_single)
    
    def _determine_timezone(self, airports_df: pd.DataFrame) -> pd.Series:
        """Determine timezone for airports (simplified implementation).
        
        Args:
            airports_df: DataFrame with airport location data
            
        Returns:
            Series of timezone strings
        """
        # Simplified timezone mapping based on US states/regions
        timezone_mapping = {
            'CA': 'America/Los_Angeles',
            'NY': 'America/New_York',
            'TX': 'America/Chicago',
            'FL': 'America/New_York',
            'IL': 'America/Chicago',
            'WA': 'America/Los_Angeles',
            'OR': 'America/Los_Angeles'
        }
        
        def get_timezone(state: str) -> str:
            if pd.isna(state):
                return 'America/New_York'  # Default to Eastern
            return timezone_mapping.get(str(state).strip(), 'America/New_York')
        
        return airports_df['state'].apply(get_timezone)
    
    def _determine_holidays(self, dates: pd.Series) -> pd.Series:
        """Determine if dates are holidays (simplified implementation).
        
        Args:
            dates: Series of datetime objects
            
        Returns:
            Series of boolean values indicating holidays
        """
        # Define major US holidays for 2015 (simplified)
        holidays_2015 = [
            '2015-01-01',  # New Year's Day
            '2015-05-25',  # Memorial Day
            '2015-07-04',  # Independence Day
            '2015-09-07',  # Labor Day
            '2015-11-26',  # Thanksgiving
            '2015-12-25',  # Christmas
        ]
        
        holiday_dates = pd.to_datetime(holidays_2015).date
        return dates.dt.date.isin(holiday_dates)
    
    def _determine_season(self, months: pd.Series) -> pd.Series:
        """Determine season based on month.
        
        Args:
            months: Series of month numbers (1-12)
            
        Returns:
            Series of season names
        """
        def get_season(month: int) -> str:
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:  # 9, 10, 11
                return 'Fall'
        
        return months.apply(get_season)
    
    def _categorize_hour(self, hour: int) -> str:
        """Categorize hour into time periods.
        
        Args:
            hour: Hour of day (0-23)
            
        Returns:
            Time period category
        """
        if 5 <= hour < 9:
            return 'Early Morning'
        elif 9 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 21:
            return 'Evening'
        else:
            return 'Late Night'
    
    def _categorize_time_period(self, hour: int) -> str:
        """Categorize hour into broader time periods.
        
        Args:
            hour: Hour of day (0-23)
            
        Returns:
            Time period name
        """
        if 0 <= hour < 6:
            return 'Overnight'
        elif 6 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 18:
            return 'Afternoon'
        elif 18 <= hour < 22:
            return 'Evening'
        else:
            return 'Overnight'
    
    def get_dimension_summary(self, dimensions: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, any]]:
        """Generate summary statistics for all dimension tables.
        
        Args:
            dimensions: Dictionary of dimension DataFrames
            
        Returns:
            Summary statistics for each dimension
        """
        self.logger.info("Generating dimension transformation summary")
        
        summary = {}
        
        for dim_name, dim_df in dimensions.items():
            summary[dim_name] = {
                'record_count': len(dim_df),
                'columns': list(dim_df.columns),
                'null_counts': dim_df.isnull().sum().to_dict(),
                'duplicate_count': dim_df.duplicated().sum()
            }
            
            # Add dimension-specific statistics
            if dim_name == 'airlines':
                summary[dim_name]['unique_types'] = dim_df['airline_type'].nunique()
            elif dim_name == 'airports':
                summary[dim_name]['unique_countries'] = dim_df['country'].nunique()
                summary[dim_name]['unique_states'] = dim_df['state'].nunique()
            elif dim_name == 'dates':
                summary[dim_name]['date_range'] = {
                    'min': dim_df['date'].min(),
                    'max': dim_df['date'].max()
                }
                summary[dim_name]['weekend_count'] = dim_df['is_weekend'].sum()
                summary[dim_name]['holiday_count'] = dim_df['is_holiday'].sum()
        
        return summary
