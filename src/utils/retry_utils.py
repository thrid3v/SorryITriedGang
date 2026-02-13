"""
RetailNexus — Retry Utilities
==============================
Provides retry decorators with exponential backoff for handling transient failures.
"""
import time
import random
import functools
from typing import Callable, Type


def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator that retries a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 1.0)
        backoff_factor: Multiplier for delay between retries (default: 2.0)
        exceptions: Tuple of exception types to catch and retry on (default: all)
    
    Example:
        @retry_with_backoff(max_attempts=3, exceptions=(IOError, OSError))
        def risky_operation():
            # ... code that might fail transiently ...
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        print(f"[RETRY] [ERROR] Failed after {max_attempts} attempts: {e}")
                        raise
                    
                    # Add jitter to avoid thundering herd
                    jitter = random.uniform(0, 0.1 * delay)
                    sleep_time = delay + jitter
                    
                    print(f"[RETRY] [WARN] Attempt {attempt}/{max_attempts} failed: {e}")
                    print(f"[RETRY] Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    delay *= backoff_factor
            
        return wrapper
    return decorator
