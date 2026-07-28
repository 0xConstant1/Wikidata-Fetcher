
import time
import logging
from typing import Callable, Dict, Any, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from validate import CsvValidationError

log = logging.getLogger(__name__)


class WikidataFetcher:
    """
    A client for making requests to the Wikidata SPARQL Query Service,
    with robust error handling and respect for rate limits.
    """
    def __init__(
        self,
        user_agent: str,
        endpoint: str = "https://query.wikidata.org/sparql",
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        max_429_retries: int = 3,
        max_validation_retries: int = 2,
        validation_backoff: float = 30.0
    ):
        """
        Initializes the WikidataClient.

        Args:
            user_agent (str): A descriptive User-Agent string that complies with
                              Wikidata's policy.
            endpoint (str): The SPARQL endpoint URL.
            max_retries (int): Max retries for transient server errors (5xx).
            backoff_factor (float): Backoff factor for retrying 5xx errors.
            max_429_retries (int): Max retries for handling 429 (Too Many Requests) errors.
            max_validation_retries (int): Max retries when the body arrives as a
                                          200 but fails validation, which is how a
                                          mid-stream query timeout presents itself.
            validation_backoff (float): Seconds to wait before the first
                                        validation retry; doubled each attempt.
        """
        if not user_agent or "python-requests" in user_agent.lower():
            raise ValueError("A descriptive User-Agent is required per Wikidata's policy. "
                             "See https://meta.wikimedia.org/wiki/User-Agent_policy")

        self.endpoint = endpoint
        # Base headers, Accept will be overridden per-request
        self.headers = {
            "User-Agent": user_agent
        }
        self.max_429_retries = max_429_retries
        self.max_validation_retries = max_validation_retries
        self.validation_backoff = validation_backoff

        # Configure retries for transient server errors (5xx)
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)

    def query(
        self,
        sparql: str,
        use_post: bool = False,
        timeout: int = 70,
        format: str = 'json',
        validator: Optional[Callable[[str], Any]] = None
    ) -> Union[Optional[Dict[str, Any]], str]:
        """
        Executes a SPARQL query and handles rate limiting.

        Args:
            sparql (str): The SPARQL query string.
            use_post (bool): If True, forces the use of POST.
            timeout (int): The request timeout in seconds.
            format (str): The desired response format ('json' or 'csv').
            validator (Optional[Callable]): Called with the response body before
                it is returned. It must raise CsvValidationError if the body is
                not a complete result set. A 200 is not evidence of success here:
                the endpoint streams results and appends its error report to a
                body whose status line was already sent.

        Returns:
            - A dictionary if format is 'json'.
            - A raw string if format is 'csv'.
            - None if all retries fail.
        """
        mime_types = {
            'json': 'application/sparql-results+json',
            'csv': 'text/csv'
        }
        if format not in mime_types:
            raise ValueError(f"Unsupported format '{format}'. Please use 'json' or 'csv'.")

        # Dynamically set the Accept header for this specific request
        request_headers = {**self.headers, "Accept": mime_types[format]}

        params = {"query": sparql}
        is_post = use_post or len(sparql) > 4000

        retries_429 = 0
        retries_validation = 0
        max_attempts = self.max_429_retries + self.max_validation_retries + 1

        for _ in range(max_attempts):
            try:
                if is_post:
                    response = self.session.post(self.endpoint, data=params, headers=request_headers, timeout=timeout)
                else:
                    response = self.session.get(self.endpoint, params=params, headers=request_headers, timeout=timeout)

                if response.ok:
                    # Return data based on the requested format
                    if format == 'json':
                        return response.json()

                    # 'csv' or other text-based formats
                    body = response.text
                    if validator is None:
                        return body

                    try:
                        validator(body)
                    except CsvValidationError as e:
                        retries_validation += 1
                        if retries_validation > self.max_validation_retries:
                            raise RuntimeError(
                                f"Response failed validation after "
                                f"{self.max_validation_retries} retries: {e}"
                            ) from e

                        wait = self.validation_backoff * (2 ** (retries_validation - 1))
                        log.warning(f"Response validation failed: {e} "
                                    f"Waiting {wait:.0f} seconds before retry "
                                    f"{retries_validation}/{self.max_validation_retries}.")
                        time.sleep(wait)
                        continue

                    return body

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    retries_429 += 1
                    if retries_429 > self.max_429_retries:
                        raise RuntimeError(f"Maximum retries ({self.max_429_retries}) exceeded for 429 responses.")

                    log.warning(f"Received HTTP 429 (Too Many Requests). "
                                f"Waiting {retry_after} seconds before retry "
                                f"{retries_429}/{self.max_429_retries}.")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()

            except requests.RequestException as e:
                raise RuntimeError(f"A network-level request failed after retries: {e}") from e

        return None