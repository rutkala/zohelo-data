"""Alias export for drive_safe_upload."""
try:
    from ingestion.drive_safe_upload import (
        escape_drive_query,
        hash_file,
        safe_drive_upload,
        safe_upload_file_to_drive,
    )
except ImportError:
    from drive_safe_upload import (  # type: ignore
        escape_drive_query,
        hash_file,
        safe_drive_upload,
        safe_upload_file_to_drive,
    )

__all__ = [
    "escape_drive_query",
    "hash_file",
    "safe_drive_upload",
    "safe_upload_file_to_drive",
]
