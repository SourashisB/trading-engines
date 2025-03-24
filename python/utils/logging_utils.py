import logging
import os
import sys
from datetime import datetime
import json
from typing import Dict, Any, Optional


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for easier parsing and analysis"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON"""
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add custom fields from LogRecord
        for key, value in record.__dict__.items():
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "id", "levelname", "levelno", "lineno", "module",
                "msecs", "message", "msg", "name", "pathname", "process",
                "processName", "relativeCreated", "stack_info", "thread", "threadName"
            }:
                try:
                    json.dumps({key: value})  # Check if serializable
                    log_data[key] = value
                except (TypeError, OverflowError):
                    log_data[key] = str(value)
        
        return json.dumps(log_data)


def configure_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[str] = None,
    log_to_console: bool = True,
    log_to_file: bool = False
) -> None:
    """
    Configure logging for the application
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format of log messages (json or text)
        log_file: Path to log file
        log_to_console: Whether to log to console
        log_to_file: Whether to log to file
    """
    # Convert log level string to logging level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    
    # Create log directory if it doesn't exist
    if log_file and log_to_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create formatter based on format type
    if log_format.lower() == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    
    # Add console handler if requested
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler if requested
    if log_to_file and log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific levels for noisy modules
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter that adds context to log messages"""
    
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})
    
    def process(self, msg, kwargs):
        """Process the log message and add context"""
        extra = kwargs.get("extra", {})
        for key, value in self.extra.items():
            if key not in extra:
                extra[key] = value
        kwargs["extra"] = extra
        return msg, kwargs
    
    def with_context(self, **kwargs) -> 'LoggerAdapter':
        """Create a new adapter with additional context"""
        new_extra = {**self.extra, **kwargs}
        return LoggerAdapter(self.logger, new_extra)


def get_logger(name: str, **context) -> LoggerAdapter:
    """
    Get a logger with context
    
    Args:
        name: Logger name
        **context: Additional context to include with all log messages
    
    Returns:
        LoggerAdapter: Logger adapter with context
    """
    logger = logging.getLogger(name)
    return LoggerAdapter(logger, context)