"""Tools for downloading and normalising rental listings from Zap Imóveis."""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional

from .config import (
    DEFAULT_DELAY_BETWEEN_REQUESTS,
    DEFAULT_NEIGHBORHOODS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TIMEOUT,
    Neighborhood,
    ZAP_API_URL,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class Listing:
    """Represents a simplified rental listing."""

    listing_id: str
    title: str
    neighborhood: str
    street: str
    city: str
    state: str
    area_m2: Optional[float]
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    parking_spaces: Optional[int]
    rent_price: Optional[float]
    condo_fee: Optional[float]
    price_per_m2: Optional[float]
    furnished: bool
    url: str
    captured_at: datetime

    @property
    def captured_date(self) -> str:
        """Returns the capture date (YYYY-MM-DD)."""

        return self.captured_at.date().isoformat()


class ZapScraper:
    """Scraper responsible for collecting rental data from Zap Imóveis."""

    def __init__(
        self,
        neighborhoods: Iterable[Neighborhood] | None = None,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        delay: float = DEFAULT_DELAY_BETWEEN_REQUESTS,
        transport=None,
    ) -> None:
        self.neighborhoods = list(neighborhoods or DEFAULT_NEIGHBORHOODS)
        self.page_size = page_size
        self.timeout = timeout
        self.delay = delay
        self.transport = transport or self._default_transport

    # ------------------------------------------------------------------
    # Networking helpers
    # ------------------------------------------------------------------
    def _default_transport(self, url: str) -> bytes:
        """Downloads the raw response from the provided URL."""

        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; RealstateBot/1.0)",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"Unexpected status code {response.status} for {url}")
            return response.read()

    # ------------------------------------------------------------------
    def _build_params(self, neighborhood: Neighborhood, page: int) -> Dict[str, str]:
        return {
            "addressCity": "Sao Paulo",
            "addressState": "SP",
            "addressNeighborhood": neighborhood.query,
            "business": "RENTAL",
            "category": "APARTMENT",
            "listingType": "USED",
            "page": str(page),
            "size": str(self.page_size),
            "order": "update",
            "direction": "desc",
        }

    def _build_url(self, neighborhood: Neighborhood, page: int) -> str:
        params = self._build_params(neighborhood, page)
        return f"{ZAP_API_URL}?{urllib.parse.urlencode(params)}"

    def _fetch_page(self, neighborhood: Neighborhood, page: int) -> Dict[str, object]:
        url = self._build_url(neighborhood, page)
        LOGGER.debug("Fetching %s", url)
        raw_response = self.transport(url)
        try:
            return json.loads(raw_response.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Failed to parse JSON from {url}: {exc}") from exc

    # ------------------------------------------------------------------
    def iterate_listings(
        self, neighborhood: Neighborhood, *, max_pages: Optional[int] = None
    ) -> Iterator[Listing]:
        """Iterates through all listings for a neighbourhood."""

        page = 1
        seen_ids: set[str] = set()
        while True:
            if max_pages is not None and page > max_pages:
                break
            payload = self._fetch_page(neighborhood, page)
            raw_listings = self._extract_raw_listings(payload)
            if not raw_listings:
                LOGGER.debug("No listings returned for %s on page %s", neighborhood.name, page)
                break
            for raw_listing in raw_listings:
                listing = self._normalise_listing(raw_listing)
                if listing is None:
                    continue
                if listing.listing_id in seen_ids:
                    continue
                seen_ids.add(listing.listing_id)
                yield listing
            if len(raw_listings) < self.page_size:
                break
            page += 1
            if self.delay:
                time.sleep(self.delay)

    def scrape(self, *, max_pages: Optional[int] = None) -> Iterator[Listing]:
        """Scrapes all configured neighbourhoods."""

        for neighborhood in self.neighborhoods:
            LOGGER.info("Scraping neighbourhood %s", neighborhood.name)
            yield from self.iterate_listings(neighborhood, max_pages=max_pages)

    # ------------------------------------------------------------------
    def _extract_raw_listings(self, payload: Dict[str, object]) -> List[Dict[str, object]]:
        listings = payload.get("listings")
        if not isinstance(listings, list):
            return []
        results: List[Dict[str, object]] = []
        for item in listings:
            if isinstance(item, dict):
                if "listing" in item and isinstance(item["listing"], dict):
                    results.append(item["listing"])
                else:
                    results.append(item)
        return results

    def _normalise_listing(self, raw: Dict[str, object]) -> Optional[Listing]:
        listing_id = self._get_listing_id(raw)
        if not listing_id:
            return None
        address = raw.get("address") or {}
        if not isinstance(address, dict):
            address = {}
        neighborhood = self._safe_str(address.get("neighborhood"))
        city = self._safe_str(address.get("city"))
        state = self._safe_str(address.get("state"))
        street = self._safe_str(address.get("street"))
        area = self._get_first_number(raw.get("usableAreas"))
        bedrooms = self._get_int(raw.get("bedrooms")) or self._get_int(raw.get("rooms"))
        bathrooms = self._get_int(raw.get("bathrooms"))
        parking = self._get_int(raw.get("parkingSpaces"))
        furnished = self._is_furnished(raw)
        pricing_info = self._select_pricing_info(raw.get("pricingInfos"))
        rent_price = self._parse_price(pricing_info)
        condo_fee = self._parse_fee(pricing_info)
        price_per_m2 = None
        if area and rent_price:
            try:
                price_per_m2 = rent_price / area
            except ZeroDivisionError:
                price_per_m2 = None
        title = self._safe_str(raw.get("title")) or self._safe_str(raw.get("advertiseTitle"))
        if not title:
            title = f"Apartamento em {neighborhood}" if neighborhood else "Apartamento"
        url = self._extract_url(raw)
        captured_at = datetime.now(timezone.utc)
        return Listing(
            listing_id=listing_id,
            title=title,
            neighborhood=neighborhood,
            street=street,
            city=city,
            state=state,
            area_m2=area,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            parking_spaces=parking,
            rent_price=rent_price,
            condo_fee=condo_fee,
            price_per_m2=price_per_m2,
            furnished=furnished,
            url=url,
            captured_at=captured_at,
        )

    # ------------------------------------------------------------------
    def _get_listing_id(self, raw: Dict[str, object]) -> Optional[str]:
        for key in ("id", "listingId", "detailId"):
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _safe_str(self, value: object) -> str:
        if isinstance(value, str):
            return value.strip()
        return ""

    def _get_first_number(self, value: object) -> Optional[float]:
        if isinstance(value, list) and value:
            for element in value:
                number = self._get_float(element)
                if number is not None:
                    return number
        return self._get_float(value)

    def _get_float(self, value: object) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace(".", "").replace(",", "")
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

    def _get_int(self, value: object) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return None
        return None

    def _is_furnished(self, raw: Dict[str, object]) -> bool:
        if isinstance(raw.get("furnished"), bool):
            return bool(raw["furnished"])
        amenities = raw.get("amenities")
        if isinstance(amenities, list):
            normalized = {str(item).strip().upper() for item in amenities}
            if "FURNISHED" in normalized or "MOBILIADO" in normalized:
                return True
        # Some listings expose furniture information in features
        features = raw.get("features")
        if isinstance(features, list):
            normalized = {str(item).strip().upper() for item in features}
            if "FURNISHED" in normalized or "MOBILIADO" in normalized:
                return True
        return False

    def _select_pricing_info(self, value: object) -> Dict[str, object]:
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    if item.get("businessType") == "RENTAL":
                        return item
            for item in value:
                if isinstance(item, dict):
                    return item
        return {}

    def _parse_price(self, info: Dict[str, object]) -> Optional[float]:
        for key in ("rentalTotalPrice", "price", "value"):
            price = self._get_float(info.get(key))
            if price is not None:
                return price
        return None

    def _parse_fee(self, info: Dict[str, object]) -> Optional[float]:
        fee = info.get("monthlyCondoFee") or info.get("condominium")
        return self._get_float(fee)

    def _extract_url(self, raw: Dict[str, object]) -> str:
        for key in ("url", "link", "detailUrl"):
            value = raw.get(key)
            if isinstance(value, str) and value.startswith("http"):
                return value
            if isinstance(value, dict):
                href = value.get("href")
                if isinstance(href, str) and href.startswith("http"):
                    return href
        listing_id = self._get_listing_id(raw)
        if listing_id:
            return f"https://www.zapimoveis.com.br/imovel/{listing_id}"
        return "https://www.zapimoveis.com.br"
