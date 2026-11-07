
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from tenacity import RetryCallState, wait_random_exponential
from tenacity.wait import wait_base


def parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None

    value = value.strip()

    if value.isascii() and value.isdigit():
        return float(value)

    try:
        retry_date = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_date.tzinfo is None:
        return None

    seconds = (retry_date - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, seconds)


class WaitRetryAfter(wait_base):
    def __init__(self):
        self.backoff = wait_random_exponential(multiplier=1, max=8)

    def __call__(self, retry_state: RetryCallState) -> float:
        backoff = self.backoff(retry_state)
        outcome = retry_state.outcome

        if outcome is None:
            return backoff

        exc = outcome.exception()

        if not isinstance(exc, httpx.HTTPStatusError):
            return backoff

        retry_after = parse_retry_after(exc.response.headers.get("Retry-After"))

        if retry_after is None:
            return backoff

        return max(backoff, retry_after)
