import logging
import httpx

from tenacity import Retrying, before_sleep_log, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_base
from trade_analytics.retry import WaitRetryAfter

logger = logging.getLogger(__name__)


BASE_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or 500 <= status < 600

    return isinstance(exc, httpx.TransportError)


class ComtradeClient:
    def __init__(self,
                 http_client: httpx.Client,
                 base_url: str = BASE_URL,
                 max_attempts: int = 3,
                 wait: wait_base | None = None):
        if max_attempts < 1:
            raise ValueError("max_attempts 必須大於 0")

        self.http_client = http_client
        self.base_url = base_url
        self.max_attempts = max_attempts
        self.wait = wait if wait is not None else WaitRetryAfter()

    def get(self, params: dict[str, str | int]) -> httpx.Response:
        retrying = Retrying(
            retry=retry_if_exception(is_retryable),
            stop=stop_after_attempt(self.max_attempts),
            wait=self.wait,
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        return retrying(self._request, params)

    def _request(self, params: dict[str, str | int]) -> httpx.Response:
        response = self.http_client.get(self.base_url, params=params)
        response.raise_for_status()
        logger.debug("HTTP 請求完成：status=%d", response.status_code)
        return response
