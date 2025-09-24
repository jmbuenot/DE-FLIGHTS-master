"""CSV data extraction module for the Flight Delays ETL Pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

import pandas as pd

from ..config.settings import settings
from ..utils.logging_config import ETLProgressLogger, get_logger


_DEFAULT_NA_VALUES: Tuple[str, ...] = ('', 'NULL', 'null', 'N/A', 'n/a')


@dataclass(frozen=True)
class _CSVSpec:
    """Configuration required to read and validate a CSV file."""

    path: Path
    dtype: Dict[str, str]
    required_columns: Tuple[str, ...]
    dropna_subset: Tuple[str, ...]

    def validate_columns(self, dataframe: pd.DataFrame) -> None:
        """Ensure the dataframe contains the expected columns."""
        missing_columns = [col for col in self.required_columns if col not in dataframe.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in {self.path.name}: {missing_columns}"
            )


class CSVExtractor:
    """Handles extraction of data from CSV source files."""

    def __init__(self) -> None:
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
        airlines_spec = _CSVSpec(
            path=settings.airlines_path,
            dtype={'IATA_CODE': 'string', 'AIRLINE': 'string'},
            required_columns=('IATA_CODE', 'AIRLINE'),
            dropna_subset=('IATA_CODE', 'AIRLINE'),
        )
        return self._extract_dataframe(airlines_spec)
    
    def extract_airports(self) -> pd.DataFrame:
        """Extract airport data from CSV file.
        
        Returns:
            DataFrame with airport information
            
        Raises:
            FileNotFoundError: If airports.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        airports_spec = _CSVSpec(
            path=settings.airports_path,
            dtype={
                'IATA_CODE': 'string',
                'AIRPORT': 'string',
                'CITY': 'string',
                'STATE': 'string',
                'COUNTRY': 'string',
                'LATITUDE': 'float64',
                'LONGITUDE': 'float64',
            },
            required_columns=('IATA_CODE', 'AIRPORT', 'CITY', 'STATE', 'COUNTRY'),
            dropna_subset=('IATA_CODE', 'AIRPORT', 'CITY'),
        )
        return self._extract_dataframe(airports_spec)
    
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
        flights_spec = _CSVSpec(
            path=settings.flights_path,
            dtype=self._flight_dtypes,
            required_columns=(
                'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT',
                'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE'
            ),
            dropna_subset=(
                'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT',
                'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE'
            ),
        )
        chunk_size = chunk_size or settings.BATCH_SIZE
        dataframe = self._extract_dataframe(flights_spec, chunksize=chunk_size)
        self.logger.info(f"Successfully extracted {len(dataframe):,} flight records")
        return dataframe
    
    def extract_flights_batched(self, batch_size: Optional[int] = None, start_batch: int = 1):
        """Extract flight data in batches without loading all data into memory.
        
        This generator yields individual batches of flight data, providing true
        memory-efficient processing with position tracking.
        
        Args:
            batch_size: Number of rows per batch (uses settings.BATCH_SIZE if None)
            start_batch: Batch number to start from (1-based, for resumption)
            
        Yields:
            Tuple of (batch_number, start_row, end_row, batch_dataframe)
            
        Raises:
            FileNotFoundError: If flights.csv file doesn't exist
            pd.errors.EmptyDataError: If the file is empty
            Exception: For other CSV reading errors
        """
        spec = _CSVSpec(
            path=settings.flights_path,
            dtype=self._flight_dtypes,
            required_columns=(
                'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT',
                'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE'
            ),
            dropna_subset=(
                'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'ORIGIN_AIRPORT',
                'DESTINATION_AIRPORT', 'SCHEDULED_DEPARTURE'
            ),
        )

        yield from self._batched_reader(spec, batch_size=batch_size, start_batch=start_batch)
    
    def get_total_rows(self) -> int:
        """Get the total number of rows in the flights CSV file.
        
        Returns:
            Total number of data rows (excluding header)
        """
        try:
            file_path = settings.flights_path
            with file_path.open('r', encoding='utf-8') as handler:
                total_rows = sum(1 for _ in handler) - 1  # Subtract header row
            return max(total_rows, 0)
        except Exception as exc:
            self.logger.error(f"Error counting rows in flights file: {exc}")
            return 0
    
    def validate_source_files(self) -> Dict[str, bool]:
        """Validate that all required source files exist and are readable.
        
        Returns:
            Dictionary mapping file names to their validation status
        """
        self.logger.info("Validating source CSV files")
        
        validation_results: Dict[str, bool] = {}
        files_to_check = {
            'airlines': settings.airlines_path,
            'airports': settings.airports_path,
            'flights': settings.flights_path,
        }

        for file_name, file_path in files_to_check.items():
            try:
                if not file_path.exists():
                    self.logger.error(f"{file_name}.csv not found: {file_path}")
                    validation_results[file_name] = False
                    continue

                # Try to read the first few rows to validate file structure
                df_sample = pd.read_csv(file_path, nrows=5, na_values=_DEFAULT_NA_VALUES)
                if df_sample.empty:
                    self.logger.error(f"{file_name}.csv is empty")
                    validation_results[file_name] = False
                    continue

                validation_results[file_name] = True
                self.logger.info(
                    "%s.csv validation passed (%s rows, %s columns)",
                    file_name,
                    len(df_sample),
                    len(df_sample.columns),
                )

            except Exception as exc:
                self.logger.error(f"Error validating {file_name}.csv: {exc}")
                validation_results[file_name] = False

        return validation_results
    
    def get_data_summary(self) -> Dict[str, Dict[str, any]]:
        """Get summary information about all source data files.
        
        Returns:
            Dictionary with summary statistics for each file
        """
        self.logger.info("Generating data summary")
        
        summary: Dict[str, Dict[str, any]] = {}

        try:
            airlines_df = self.extract_airlines()
            summary['airlines'] = self._summarise_dataframe(
                airlines_df,
                unique_fields={'IATA_CODE': 'unique_airlines'},
            )

            airports_df = self.extract_airports()
            summary['airports'] = self._summarise_dataframe(
                airports_df,
                unique_fields={'IATA_CODE': 'unique_airports', 'COUNTRY': 'countries'},
            )

            flights_summary = self._flight_summary()
            summary['flights'] = flights_summary

        except Exception as exc:
            self.logger.error(f"Error generating data summary: {exc}")
            summary['error'] = str(exc)

        return summary

    @property
    def _flight_dtypes(self) -> Dict[str, str]:
        """Return the dtype configuration for the flights dataset."""
        return {
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
            'WEATHER_DELAY': 'float64',
        }

    def _extract_dataframe(
        self,
        spec: _CSVSpec,
        *,
        chunksize: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read a CSV defined by ``spec`` with validation and logging."""

        self.logger.info("Extracting data from %s", spec.path)

        if not spec.path.exists():
            message = f"CSV file not found: {spec.path}"
            self.logger.error(message)
            raise FileNotFoundError(message)

        read_kwargs = {
            'dtype': spec.dtype,
            'na_values': _DEFAULT_NA_VALUES,
            'low_memory': False,
        }

        try:
            if chunksize:
                dataframe = self._read_with_progress(spec, chunksize)
            else:
                dataframe = pd.read_csv(spec.path, **read_kwargs)

            spec.validate_columns(dataframe)
            cleaned_df = dataframe.dropna(subset=spec.dropna_subset)

            dropped_rows = len(dataframe) - len(cleaned_df)
            if dropped_rows:
                self.logger.warning("Dropped %s rows with missing critical values", dropped_rows)

            return cleaned_df

        except pd.errors.EmptyDataError as exc:
            self.logger.error("%s is empty", spec.path)
            raise
        except Exception as exc:
            self.logger.error("Error reading %s: %s", spec.path, exc)
            raise

    def _read_with_progress(self, spec: _CSVSpec, chunksize: int) -> pd.DataFrame:
        """Read a CSV using chunks while reporting progress."""

        total_rows = self._count_rows(spec.path)
        chunk_list: List[pd.DataFrame] = []
        processed_rows = 0

        read_kwargs = {
            'dtype': spec.dtype,
            'na_values': _DEFAULT_NA_VALUES,
            'low_memory': False,
            'chunksize': chunksize,
        }

        for chunk_number, chunk in enumerate(pd.read_csv(spec.path, **read_kwargs), start=1):
            chunk_list.append(chunk)
            processed_rows += len(chunk)

            if total_rows:
                progress_pct = (processed_rows / total_rows) * 100
                self.logger.info(
                    "Processing chunk %s: %s records (%s/%s - %.1f%%)",
                    chunk_number,
                    len(chunk),
                    processed_rows,
                    total_rows,
                    progress_pct,
                )
            else:
                self.logger.info("Processing chunk %s: %s records", chunk_number, len(chunk))

        if not chunk_list:
            raise pd.errors.EmptyDataError(f"No data returned from {spec.path}")

        self.logger.info("Combining %s chunks into a single DataFrame", len(chunk_list))
        return pd.concat(chunk_list, ignore_index=True)

    def _batched_reader(
        self,
        spec: _CSVSpec,
        *,
        batch_size: Optional[int] = None,
        start_batch: int = 1,
    ) -> Generator[Tuple[int, int, int, pd.DataFrame], None, None]:
        """Yield batches of rows from ``spec`` with offset tracking."""

        batch_size = batch_size or settings.BATCH_SIZE
        total_rows = self._count_rows(spec.path)
        self.logger.info(
            "Starting batched extraction from %s (batch size: %s, start batch: %s)",
            spec.path,
            batch_size,
            start_batch,
        )

        skip_rows = (start_batch - 1) * batch_size + 1 if start_batch > 1 else None
        read_kwargs = {
            'dtype': spec.dtype,
            'na_values': _DEFAULT_NA_VALUES,
            'low_memory': False,
            'chunksize': batch_size,
            'skiprows': skip_rows,
        }

        current_batch = start_batch

        try:
            for batch_df in pd.read_csv(spec.path, **read_kwargs):
                if batch_df.empty:
                    self.logger.warning("Skipped empty batch %s", current_batch)
                    current_batch += 1
                    continue

                start_row = (current_batch - 1) * batch_size + 1
                end_row = start_row + len(batch_df) - 1

                spec.validate_columns(batch_df)
                cleaned_df = batch_df.dropna(subset=spec.dropna_subset)
                dropped = len(batch_df) - len(cleaned_df)
                if dropped:
                    self.logger.warning(
                        "Batch %s: dropped %s rows with missing critical values", current_batch, dropped
                    )

                self.logger.info(
                    "Yielding batch %s (%s-%s of %s)",
                    current_batch,
                    start_row,
                    end_row,
                    total_rows if total_rows > 0 else 'unknown',
                )

                yield current_batch, start_row, end_row, cleaned_df
                current_batch += 1

            self.logger.info("Completed batched extraction - processed %s batches", current_batch - start_batch)
        except pd.errors.EmptyDataError:
            self.logger.error("%s is empty", spec.path)
            raise
        except Exception as exc:
            self.logger.error("Error in batched extraction for %s: %s", spec.path, exc)
            raise

    def _count_rows(self, file_path: Path) -> int:
        """Return the number of data rows in a CSV file, excluding the header."""

        try:
            with file_path.open('r', encoding='utf-8') as handler:
                total_rows = sum(1 for _ in handler) - 1
            return max(total_rows, 0)
        except FileNotFoundError:
            self.logger.error("File not found when counting rows: %s", file_path)
            raise
        except Exception as exc:
            self.logger.error("Error counting rows in %s: %s", file_path, exc)
            return 0

    def _summarise_dataframe(
        self,
        dataframe: pd.DataFrame,
        *,
        unique_fields: Optional[Dict[str, str]] = None,
    ) -> Dict[str, any]:
        """Generate a compact summary for a dataframe."""

        summary: Dict[str, any] = {
            'record_count': len(dataframe),
            'columns': list(dataframe.columns),
        }

        if unique_fields:
            for column, alias in unique_fields.items():
                summary[alias] = dataframe[column].nunique() if column in dataframe.columns else 0

        return summary

    def _flight_summary(self) -> Dict[str, any]:
        """Return a dedicated summary for the flights dataset using a sample."""

        sample_size = min(settings.BATCH_SIZE, 10_000)
        self.logger.info("Generating flights summary using a sample of %s rows", sample_size)

        flights_sample = pd.read_csv(
            settings.flights_path,
            nrows=sample_size,
            dtype=self._flight_dtypes,
            na_values=_DEFAULT_NA_VALUES,
        )

        total_flights = self._count_rows(settings.flights_path)
        summary: Dict[str, any] = {
            'record_count': total_flights,
            'sample_size': len(flights_sample),
            'columns': list(flights_sample.columns),
        }

        if 'YEAR' in flights_sample.columns and not flights_sample['YEAR'].empty:
            summary['date_range'] = {
                'min_year': int(flights_sample['YEAR'].min()),
                'max_year': int(flights_sample['YEAR'].max()),
            }

        if 'AIRLINE' in flights_sample.columns:
            summary['unique_airlines'] = flights_sample['AIRLINE'].nunique()

        return summary
