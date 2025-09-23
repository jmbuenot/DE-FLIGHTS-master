"""Logging configuration for the Flight Delays ETL Pipeline."""

import logging
import sys
from pathlib import Path
from typing import Optional

from ..config.settings import settings


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    console_output: bool = True
) -> logging.Logger:
    """Set up logging configuration for the ETL pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (if None, uses settings.LOG_FILE)
        console_output: Whether to output logs to console
        
    Returns:
        Configured logger instance
    """
    # Use settings defaults if not provided
    if log_level is None:
        log_level = settings.LOG_LEVEL
    if log_file is None:
        log_file = settings.LOG_FILE
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger('etl_pipeline')
    logger.setLevel(numeric_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        # Create log directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a child logger for a specific module.
    
    Args:
        name: Name of the logger (typically __name__)
        
    Returns:
        Child logger instance
    """
    return logging.getLogger(f'etl_pipeline.{name}')


class ETLProgressLogger:
    """Helper class for logging ETL progress and statistics."""
    
    def __init__(self, logger: logging.Logger):
        """Initialize with a logger instance."""
        self.logger = logger
        self.step_count = 0
        self.total_steps = 0
    
    def start_process(self, process_name: str, total_steps: int = 0):
        """Log the start of an ETL process.
        
        Args:
            process_name: Name of the process starting
            total_steps: Total number of steps (if known)
        """
        self.step_count = 0
        self.total_steps = total_steps
        self.logger.info(f"Starting {process_name}")
        if total_steps > 0:
            self.logger.info(f"Total steps: {total_steps}")
    
    def log_step(self, step_description: str, records_processed: int = 0):
        """Log completion of an ETL step.
        
        Args:
            step_description: Description of the completed step
            records_processed: Number of records processed in this step
        """
        self.step_count += 1
        
        if self.total_steps > 0:
            progress = (self.step_count / self.total_steps) * 100
            step_info = f"Step {self.step_count}/{self.total_steps} ({progress:.1f}%)"
        else:
            step_info = f"Step {self.step_count}"
        
        message = f"{step_info}: {step_description}"
        if records_processed > 0:
            message += f" ({records_processed:,} records)"
        
        self.logger.info(message)
    
    def log_error(self, error_description: str, exception: Optional[Exception] = None):
        """Log an error with optional exception details.
        
        Args:
            error_description: Description of the error
            exception: Exception object (if available)
        """
        if exception:
            self.logger.error(f"{error_description}: {str(exception)}")
        else:
            self.logger.error(error_description)
    
    def log_warning(self, warning_description: str, count: int = 0):
        """Log a warning with optional count information.
        
        Args:
            warning_description: Description of the warning
            count: Number of occurrences (if applicable)
        """
        message = warning_description
        if count > 0:
            message += f" (occurred {count:,} times)"
        
        self.logger.warning(message)
    
    def complete_process(self, process_name: str, total_records: int = 0):
        """Log the completion of an ETL process.
        
        Args:
            process_name: Name of the completed process
            total_records: Total number of records processed
        """
        message = f"Completed {process_name}"
        if total_records > 0:
            message += f" - Total records processed: {total_records:,}"
        
        self.logger.info(message)
        self.logger.info("-" * 50)


# Initialize default logger
default_logger = setup_logging()
