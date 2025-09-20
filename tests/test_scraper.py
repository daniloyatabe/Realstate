from __future__ import annotations

import json
import unittest
from pathlib import Path

from realstate.config import Neighborhood
from realstate.scraper import ZapScraper


def load_fixture() -> bytes:
    fixture_path = Path(__file__).resolve().parent.parent / "fixtures" / "sample_api_response.json"
    return fixture_path.read_bytes()


class ScraperTests(unittest.TestCase):
    def test_scraper_normalises_listings(self):
        responses = [load_fixture(), json.dumps({"listings": []}).encode("utf-8")]

        def fake_transport(_url: str) -> bytes:
            return responses.pop(0)

        scraper = ZapScraper(
            neighborhoods=[Neighborhood(name="Pinheiros", query="Pinheiros")],
            transport=fake_transport,
        )

        listings = list(scraper.scrape())

        self.assertEqual(len(listings), 2)

        first = listings[0]
        self.assertEqual(first.listing_id, "12345")
        self.assertEqual(first.neighborhood, "Pinheiros")
        self.assertTrue(first.furnished)
        self.assertEqual(first.rent_price, 4800.0)
        self.assertEqual(first.price_per_m2, 60.0)
        self.assertTrue(first.url.startswith("https://www.zapimoveis.com.br/imovel/12345"))

        second = listings[1]
        self.assertEqual(second.listing_id, "67890")
        self.assertEqual(second.neighborhood, "Moema")
        self.assertFalse(second.furnished)
        self.assertEqual(second.rent_price, 6000.0)
        self.assertEqual(second.price_per_m2, 60.0)
        self.assertTrue(second.url.endswith("67890"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
