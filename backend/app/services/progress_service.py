import json
import os
import time
from typing import Dict, Any, Optional
from datetime import datetime
import threading
from loguru import logger

class ProgressService:
    """Service to track document processing progress for users"""
    
    def __init__(self, storage_dir: str = "storage/progress"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
    
    def _get_progress_file(self, user_id: str) -> str:
        """Get the progress file path for a user"""
        return os.path.join(self.storage_dir, f"{user_id}_progress.json")
    
    def start_processing(self, user_id: str, file_names: list, total_files: int) -> Dict[str, Any]:
        """Initialize progress tracking for a new processing job"""
        progress_data = {
            "user_id": user_id,
            "status": "processing",
            "start_time": datetime.now().isoformat(),
            "total_files": total_files,
            "processed_files": 0,
            "current_file": "",
            "file_names": file_names,
            "total_chunks": 0,
            "processed_chunks": 0,
            "current_chunk": 0,
            "error": None,
            "completion_time": None
        }
        
        self._save_progress(user_id, progress_data)
        logger.info(f"Started progress tracking for user {user_id} with {total_files} files")
        return progress_data
    
    def update_file_progress(self, user_id: str, current_file: str, processed_files: int, total_chunks: int = 0):
        """Update progress for file processing"""
        progress_data = self._load_progress(user_id)
        if progress_data:
            progress_data["current_file"] = current_file
            progress_data["processed_files"] = processed_files
            progress_data["total_chunks"] = total_chunks
            self._save_progress(user_id, progress_data)
    
    def update_chunk_progress(self, user_id: str, processed_chunks: int, current_chunk: int = 0):
        """Update progress for chunk processing"""
        progress_data = self._load_progress(user_id)
        if progress_data:
            progress_data["processed_chunks"] = processed_chunks
            progress_data["current_chunk"] = current_chunk
            self._save_progress(user_id, progress_data)
    
    def complete_processing(self, user_id: str, success: bool = True, error: str = None):
        """Mark processing as complete"""
        progress_data = self._load_progress(user_id)
        if progress_data:
            progress_data["status"] = "completed" if success else "failed"
            progress_data["completion_time"] = datetime.now().isoformat()
            if error:
                progress_data["error"] = error
            self._save_progress(user_id, progress_data)
            logger.info(f"Completed processing for user {user_id} - Success: {success}")
    
    def get_progress(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get current progress for a user"""
        return self._load_progress(user_id)
    
    def _load_progress(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Load progress data from file"""
        try:
            progress_file = self._get_progress_file(user_id)
            if os.path.exists(progress_file):
                with open(progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading progress for user {user_id}: {str(e)}")
        return None
    
    def _save_progress(self, user_id: str, progress_data: Dict[str, Any]):
        """Save progress data to file"""
        try:
            progress_file = self._get_progress_file(user_id)
            with open(progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving progress for user {user_id}: {str(e)}")
    
    def cleanup_progress(self, user_id: str):
        """Clean up progress file after completion"""
        try:
            progress_file = self._get_progress_file(user_id)
            if os.path.exists(progress_file):
                os.remove(progress_file)
                logger.info(f"Cleaned up progress file for user {user_id}")
        except Exception as e:
            logger.error(f"Error cleaning up progress for user {user_id}: {str(e)}")

# Global instance
progress_service = ProgressService() 