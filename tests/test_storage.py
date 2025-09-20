from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from realstate.scraper import Listing
from realstate.storage import RealEstateDatabase


def make_listing(
    listing_id: str,
    *,
    captured_at: datetime,
    rent_price: float,
    price_per_m2: float,
    neighborhood: str = "Pinheiros",
    furnished: bool = True,
) -> Listing:
    return Listing(
        listing_id=listing_id,
        title="Apartamento de teste",
        neighborhood=neighborhood,
        street="Rua Teste",
        city="São Paulo",
        state="SP",
        area_m2=80.0,
        bedrooms=2,
        bathrooms=2,
        parking_spaces=1,
        rent_price=rent_price,
        condo_fee=500.0,
        price_per_m2=price_per_m2,
        furnished=furnished,
        url=f"https://www.zapimoveis.com.br/imovel/{listing_id}",
        captured_at=captured_at,
    )


class StorageTests(unittest.TestCase):
    def test_database_persists_history(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "rentals.sqlite"
            db = RealEstateDatabase(db_path)

            first_capture = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
            second_capture = first_capture + timedelta(days=1)

            listing = make_listing(
                "ABC123",
                captured_at=first_capture,
                rent_price=5000.0,
                price_per_m2=62.5,
                furnished=True,
            )
            db.persist_listing(listing)

            updated = make_listing(
                "ABC123",
                captured_at=second_capture,
                rent_price=5200.0,
                price_per_m2=65.0,
                furnished=True,
            )
            db.persist_listing(updated)

            history = db.get_listing_history("ABC123")
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0]["rent_price"], 5000.0)
            self.assertEqual(history[1]["rent_price"], 5200.0)

            averages = db.get_neighborhood_daily_average("Pinheiros", furnished=True)
            self.assertEqual(len(averages), 2)
            self.assertEqual(averages[0]["avg_price_per_m2"], 62.5)
            self.assertEqual(averages[1]["avg_price_per_m2"], 65.0)

            listings = db.list_listings("Pinheiros")
            self.assertEqual(len(listings), 1)
            self.assertEqual(listings[0]["listing_id"], "ABC123")
            self.assertEqual(listings[0]["furnished"], 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
