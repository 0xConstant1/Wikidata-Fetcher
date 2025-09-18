
import time
import logging
from typing import Dict, Any, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
        max_429_retries: int = 3
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
        format: str = 'json'
    ) -> Union[Optional[Dict[str, Any]], str]:
        """
        Executes a SPARQL query and handles rate limiting.

        Args:
            sparql (str): The SPARQL query string.
            use_post (bool): If True, forces the use of POST.
            timeout (int): The request timeout in seconds.
            format (str): The desired response format ('json' or 'csv').

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

        for attempt in range(self.max_429_retries + 1):
            try:
                if is_post:
                    response = self.session.post(self.endpoint, data=params, headers=request_headers, timeout=timeout)
                else:
                    response = self.session.get(self.endpoint, params=params, headers=request_headers, timeout=timeout)

                if response.ok:
                    # Return data based on the requested format
                    if format == 'json':
                        return response.json()
                    else: # 'csv' or other text-based formats
                        return response.text

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    log.warning(f"Received HTTP 429 (Too Many Requests). "
                                f"Waiting {retry_after} seconds before retry {attempt + 1}/{self.max_429_retries}.")
                    if attempt < self.max_429_retries:
                        time.sleep(retry_after)
                        continue
                    else:
                        raise RuntimeError(f"Maximum retries ({self.max_429_retries}) exceeded for 429 responses.")

                response.raise_for_status()

            except requests.RequestException as e:
                raise RuntimeError(f"A network-level request failed after retries: {e}") from e

        return None