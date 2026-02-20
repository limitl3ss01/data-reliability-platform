class DataPlatformError(Exception):
    """Base exception for platform-level errors."""


class DataSourceError(DataPlatformError):
    """Raised for source API or connector failures."""


class StorageError(DataPlatformError):
    """Raised for storage access or write failures."""
