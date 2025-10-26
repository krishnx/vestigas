import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Awaitable, Tuple, Type, Any

from httpx import ConnectError, TimeoutException, HTTPStatusError

logger = logging.getLogger(__name__)

DEFAULT_RETRY_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    ConnectError,
    TimeoutException,
    HTTPStatusError
)

RETRY_STATUS_CODES = (500, 502, 503, 504)


def async_retry(
        max_retries: int = 3,
        base_backoff_time: float = 1.0,  # Base time in seconds
        exceptions_to_retry: Tuple[Type[Exception], ...] = DEFAULT_RETRY_EXCEPTIONS,
        jitter: bool = True
) -> Callable:
    """
    Decorator for asynchronous functions to implement exponential backoff with jitter.
    It handles transient network errors and server-side HTTP errors (5xx).

    Args:
        max_retries: Maximum number of times to retry the function.
        base_backoff_time: The starting point for exponential backoff (e.g., 1s, 2s, 4s).
        exceptions_to_retry: Tuple of exceptions that should trigger a retry.
        jitter: If True, adds random variation to the backoff time.
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:

            for attempt in range(max_retries + 1):
                try:
                    response = await func(*args, **kwargs)

                    if isinstance(response, Tuple) and len(response) == 2 and isinstance(response[0], int):
                        status_code = response[0]
                    elif hasattr(response, 'status_code'):
                        status_code = response.status_code
                    else:
                        return response

                    if status_code in RETRY_STATUS_CODES and attempt < max_retries:
                        raise HTTPStatusError(f"Retriable status code {status_code}", request=None, response=response)

                    return response

                except exceptions_to_retry as e:
                    if isinstance(e, HTTPStatusError) and e.response.status_code not in RETRY_STATUS_CODES:
                        logger.error(
                            f"Attempt {attempt + 1}/{max_retries + 1}: Fatal HTTP error"
                            f" {e.response.status_code} - not retrying.")
                        raise

                    if attempt < max_retries:
                        delay = base_backoff_time * (2 ** attempt)
                        if jitter:
                            delay = random.uniform(0, delay)

                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed with {type(e).__name__}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Final attempt failed for {func.__name__}. Giving up.")
                        raise

        return wrapper

    return decorator
