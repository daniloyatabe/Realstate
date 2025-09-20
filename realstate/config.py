"""Configuration for the real estate scraping project."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass(frozen=True)
class Neighborhood:
    """Represents a neighborhood in São Paulo that should be scraped."""

    name: str
    query: str


DEFAULT_NEIGHBORHOODS: List[Neighborhood] = [
    Neighborhood(name="Pinheiros", query="Pinheiros"),
    Neighborhood(name="Vila Madalena", query="Vila Madalena"),
    Neighborhood(name="Moema", query="Moema"),
    Neighborhood(name="Saúde", query="Saúde"),
]

# Base directory where generated artifacts (database, plots) are stored.
BASE_DATA_DIR = Path("data")

# Default path for the SQLite database that stores the scraped data.
DEFAULT_DATABASE_PATH = BASE_DATA_DIR / "zap_rentals.sqlite"

# Base URL for the public Zap Imóveis search API.
ZAP_API_URL = "https://glue-api.zapimoveis.com.br/v3/listings"

# Amount of listings requested per page. The API accepts values up to 50.
DEFAULT_PAGE_SIZE = 50

# Default timeout (in seconds) for HTTP requests.
DEFAULT_TIMEOUT = 30

# Seconds to wait between consecutive requests to avoid overloading the API.
DEFAULT_DELAY_BETWEEN_REQUESTS = 1.5
