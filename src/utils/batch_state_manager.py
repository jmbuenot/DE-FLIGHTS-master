"""Batch state management for resumable ETL processing."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional

from ..config.settings import settings
from ..utils.logging_config import get_logger


class BatchStateManager:
    """Manages ETL batch processing state for resumable operations."""
    
    def __init__(self, state_file: str = "etl_state.json"):
        """Initialize the batch state manager.
        
        Args:
            state_file: Path to the state file
        """
        self.logger = get_logger(__name__)
        self.state_file = Path(state_file)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load state from file or create new state if file doesn't exist.
        
        Returns:
            Dictionary containing the state information
        """
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.logger.info(f"Loaded existing state from {self.state_file}")
                return state
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Failed to load state file {self.state_file}: {e}")
                self.logger.info("Creating new state")
        
        # Create new state
        return self._create_new_state()
    
    def _create_new_state(self) -> Dict:
        """Create a new state dictionary.
        
        Returns:
            New state dictionary with default values
        """
        return {
            "run_id": datetime.now().strftime('%Y%m%d_%H%M%S'),
            "status": "initialized",
            "last_processed_batch": 0,
            "last_processed_row": 0,
            "batch_size": settings.BATCH_SIZE,
            "total_rows_estimated": 0,
            "timestamp": datetime.now().isoformat(),
            "records_processed": 0,
            "batches_completed": 0
        }
    
    def save_state(self):
        """Save current state to file."""
        try:
            self.state["timestamp"] = datetime.now().isoformat()
            
            # Create directory if it doesn't exist
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            
            self.logger.debug(f"State saved to {self.state_file}")
            
        except IOError as e:
            self.logger.error(f"Failed to save state to {self.state_file}: {e}")
    
    def has_incomplete_run(self) -> bool:
        """Check if there's an incomplete ETL run that can be resumed.
        
        Returns:
            True if there's an incomplete run to resume
        """
        return (
            self.state.get("status") in ["processing", "initialized"] and 
            self.state.get("last_processed_batch", 0) > 0
        )
    
    def calculate_resume_position(self) -> Tuple[int, int]:
        """Calculate the position to resume processing from.
        
        Handles batch size changes by recalculating position based on rows.
        
        Returns:
            Tuple of (next_batch_number, start_row_position)
        """
        current_batch_size = settings.BATCH_SIZE
        stored_batch_size = self.state.get("batch_size", current_batch_size)
        
        if stored_batch_size != current_batch_size:
            last_row = self.state["last_processed_row"]
            
            # Calculate which batch this row would be in with new batch size
            new_batch_number = last_row // current_batch_size
            new_start_row = new_batch_number * current_batch_size
            
            self.logger.info(
                f"Batch size changed from {stored_batch_size:,} to {current_batch_size:,}. "
                f"Recalculating position: batch {self.state['last_processed_batch']} → {new_batch_number}, "
                f"row {last_row:,} → {new_start_row:,}"
            )
            
            # Update existing properties directly
            self.state['last_processed_batch'] = new_batch_number
            self.state['last_processed_row'] = new_start_row
            self.state['batch_size'] = current_batch_size
            self.save_state()
        
        # Return next batch to process
        next_batch = self.state["last_processed_batch"] + 1
        start_row = self.state["last_processed_row"]
        
        return next_batch, start_row
    
    def start_processing(self, total_rows: int):
        """Mark the start of ETL processing.
        
        Args:
            total_rows: Total number of rows to process
        """
        self.state["status"] = "processing"
        self.state["total_rows_estimated"] = total_rows
        self.state["batch_size"] = settings.BATCH_SIZE
        self.save_state()
        
        self.logger.info(f"Started ETL processing: {total_rows:,} rows, batch size {settings.BATCH_SIZE:,}")
    
    def save_progress(self, batch_number: int, end_row: int, records_processed: int):
        """Save progress after successful batch processing.
        
        Args:
            batch_number: Completed batch number
            end_row: Last row processed in this batch
            records_processed: Number of records processed in this batch
        """
        self.state["last_processed_batch"] = batch_number
        self.state["last_processed_row"] = end_row
        self.state["records_processed"] += records_processed
        self.state["batches_completed"] += 1
        self.save_state()
        
        progress_pct = (end_row / max(self.state["total_rows_estimated"], 1)) * 100
        
        self.logger.info(
            f"Progress saved: batch {batch_number} completed "
            f"({end_row:,}/{self.state['total_rows_estimated']:,} rows - {progress_pct:.1f}%)"
        )
    
    def mark_complete(self):
        """Mark the ETL run as successfully completed."""
        self.state["status"] = "completed"
        self.state["timestamp"] = datetime.now().isoformat()
        self.save_state()
        
        self.logger.info(
            f"ETL run completed successfully: {self.state['batches_completed']} batches, "
            f"{self.state['records_processed']:,} records processed"
        )
    
    def mark_failed(self, error_message: str):
        """Mark the ETL run as failed.
        
        Args:
            error_message: Description of the failure
        """
        self.state["status"] = "failed"
        self.state["error_message"] = error_message
        self.state["timestamp"] = datetime.now().isoformat()
        self.save_state()
        
        self.logger.error(f"ETL run marked as failed: {error_message}")
    
    def reset_position(self, start_row: int = 0):
        """Reset the processing position to a specific row.
        
        Args:
            start_row: Row number to start from (0-based)
        """
        batch_size = settings.BATCH_SIZE
        start_batch = start_row // batch_size
        
        self.logger.info(
            f"Resetting position to row {start_row:,} (batch {start_batch + 1})"
        )
        
        self.state["last_processed_batch"] = start_batch
        self.state["last_processed_row"] = start_row
        self.state["batch_size"] = batch_size
        self.state["status"] = "initialized"
        self.save_state()
    
    def clear_state(self):
        """Clear all state and start fresh."""
        self.logger.info("Clearing ETL state - will start from beginning")
        
        if self.state_file.exists():
            self.state_file.unlink()
        
        self.state = self._create_new_state()
    
    def get_state_info(self) -> Dict:
        """Get current state information for display.
        
        Returns:
            Dictionary with formatted state information
        """
        state_info = {
            "Status": self.state.get("status", "unknown"),
            "Run ID": self.state.get("run_id", "unknown"),
            "Last Batch": self.state.get("last_processed_batch", 0),
            "Last Row": f"{self.state.get('last_processed_row', 0):,}",
            "Batch Size": f"{self.state.get('batch_size', 0):,}",
            "Current Batch Size": f"{settings.BATCH_SIZE:,}",
            "Total Rows": f"{self.state.get('total_rows_estimated', 0):,}",
            "Records Processed": f"{self.state.get('records_processed', 0):,}",
            "Batches Completed": self.state.get("batches_completed", 0),
            "Last Updated": self.state.get("timestamp", "unknown")
        }
        
        # Calculate progress percentage
        if self.state.get("total_rows_estimated", 0) > 0:
            progress = (self.state.get("last_processed_row", 0) / self.state["total_rows_estimated"]) * 100
            state_info["Progress"] = f"{progress:.1f}%"
        
        return state_info
    
    def needs_batch_size_recalculation(self) -> bool:
        """Check if batch size has changed and position needs recalculation.
        
        Returns:
            True if batch size has changed since last run
        """
        return self.state.get("batch_size", 0) != settings.BATCH_SIZE
