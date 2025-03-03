import asyncio
import logging
from functools import lru_cache
from sys import stdout
from time import gmtime, time
from typing import Optional


class LogMixin:
    def __init__(self):
        self.logger = get_logger()

    def log(self, msg):
        self.logger.info(f"{self.__class__.__name__} {msg}")

    def log_debug(self, msg):
        self.logger.debug(f"{self.__class__.__name__} {msg}")


@lru_cache()
def get_logger():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d: [%(process)d:%(thread)d] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s"  # noqa: 501
    )
    log_formatter.converter = gmtime
    root_logger = logging.getLogger()

    console_handler = logging.StreamHandler(stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.DEBUG)

    return logging


def log(msg: str):
    get_logger().info(msg)


def log_debug(msg: str):
    get_logger().debug(msg)


statuses = {
    100: "one",
    300: "two",
    500: "three",
    1000: "four",
    5000: "five",
    30000: "six",
    60 * 1000: "seven",
    2 * 60 * 1000: "eight",
    5 * 60 * 1000: "nine",
    10 * 60 * 1000: "ten",
}


def get_took_status(took_ms) -> str:
    """
    Returns a string describing the status of the request based on the time it took.
    """
    took_status = [status for status_ms, status in statuses.items() if took_ms > status_ms]

    return ", ".join(took_status) if took_status else ""


def get_diff_time(start_time: float, end_time: Optional[float] = None) -> str:
    if end_time is None:
        end_time = time()
    diff_ms = round((end_time - start_time) * 1000, 1)

    return f"took {diff_ms}ms, ({get_took_status(diff_ms)})"


def log_diff(name, start_time, url, extra: Optional[dict] = None):
    end_time = time()
    duration = int((end_time - start_time) * 1000)
    get_logger().info(f"{name}  took {duration}ms ({get_took_status(duration)}) {url}, {extra}")


def log_with_time_info(func):
    def timed(*args, **kwargs):
        log(f"{func.__name__} started")
        ts = time()
        result = func(*args, **kwargs)
        log(f"{func.__name__} time: {str(get_diff_time(ts))} ms")
        return result

    return timed


def log_with_time_info_async(func):
    async def process(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            log(f"sync function {func.__name__}")
            return func(*args, **kwargs)

    async def helper(*args, **kwargs):
        ts = time()
        log(f"{func.__name__} started")
        result = await process(func, *args, **kwargs)
        log(f"{func.__name__} time: {str(get_diff_time(ts))} ms")
        return result

    return helper
