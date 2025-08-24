import asyncio
import cProfile
import pstats
import sys
import logging
from enum import Enum
from logging.handlers import QueueHandler, QueueListener
from functools import wraps
from datetime import datetime
from colorlog import ColoredFormatter
from queue import Queue

DEFAULT_LOGGING_LEVEL = logging.INFO


class LogLevel(Enum):
    """
    Enum for log levels.
    """

    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR


class AsyncLogger:
    def __init__(self):
        self.queue = Queue()
        self.logger = self._initialize_logger()
        self.listener = QueueListener(self.queue, self._create_handler())
        self.listener.start()

    def _initialize_logger(self) -> logging.Logger:
        """
        Initialize and configure the logger.

        Returns:
        logging.Logger: The configured logger.
        """
        logger = logging.getLogger()
        logger.setLevel(DEFAULT_LOGGING_LEVEL)

        # Create a queue handler
        queue_handler = QueueHandler(self.queue)
        logger.addHandler(queue_handler)

        return logger

    def _create_handler(self) -> logging.Handler:
        """
        Create a logging handler for processing log messages from the queue.

        Returns:
        logging.Handler: The configured handler.
        """
        formatter = ColoredFormatter(
            "%(log_color)s%(asctime)s - %(levelname)s - %(module)s:%(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )

        # Create a stream handler
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setLevel(DEFAULT_LOGGING_LEVEL)
        stream_handler.setFormatter(formatter)

        return stream_handler

    def get_logger(self) -> logging.Logger:
        """
        Get the configured logger.

        Returns:
        logging.Logger: The configured logger.
        """
        return self.logger


# Initialize the async logger
async_logger = AsyncLogger()
logger = async_logger.get_logger()


def log_execution_time(func):
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = await func(*args, **kwargs)
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000
            logger.info(f"Function '{func.__name__}' executed in {execution_time:.2f} milliseconds")
            return result

        return async_wrapper
    else:

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()
            execution_time = (end_time - start_time).microseconds
            logger.info(f"Function '{func.__name__}' executed in {execution_time} microseconds")
            return result

        return sync_wrapper


def profile_func(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        result = func(*args, **kwargs)
        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats("cumtime")
        stats.print_stats(10)  # Print the top 10 results
        return result

    return wrapper
