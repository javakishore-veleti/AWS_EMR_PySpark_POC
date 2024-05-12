import time
import logging

LOGGER = logging.getLogger(__file__)


def log_time(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        elapsed_time = end - start
        log_msg = f"Function {func.__name__} took {elapsed_time} seconds to complete."
        print(log_msg)
        LOGGER.info(log_msg)
        return result

    return wrapper
