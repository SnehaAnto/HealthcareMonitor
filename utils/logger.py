import logging
import os
from pathlib import Path
from datetime import datetime

class LoggerSetup:
    @staticmethod
    def setup_logging():
        """Configure logging to write to log folder"""
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Clear old log files
        for old_log in log_dir.glob("*.log"):
            old_log.unlink()

        # Create handlers
        console_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(log_dir / "health_monitoring.log")
        error_handler = logging.FileHandler(log_dir / "errors.log")
        error_handler.setLevel(logging.ERROR)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Set formatters
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)

        # Configure root logger
        logging.basicConfig(
            level=logging.DEBUG,
            handlers=[console_handler, file_handler, error_handler]
        )

        # Create separate loggers for each service
        service_loggers = {
            'DataCollection': logging.getLogger('DataCollection'),
            'DataProcessing': logging.getLogger('DataProcessing'),
            'Storage': logging.getLogger('Storage'),
            'Notification': logging.getLogger('Notification'),
            'Security': logging.getLogger('Security'),
            'System': logging.getLogger('System')
        }

        # Configure service-specific log files
        for service_name, logger in service_loggers.items():
            service_handler = logging.FileHandler(log_dir / f"{service_name.lower()}.log")
            service_handler.setFormatter(formatter)
            logger.addHandler(service_handler)

        return service_loggers
