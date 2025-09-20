"""Persistence layer responsible for storing the scraped data."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

from .scraper import Listing


class RealEstateDatabase:
    """Small wrapper around SQLite used to persist listing information."""

    def __init__(self, database_path: Path | str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.database_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
                    listing_id TEXT PRIMARY KEY,
                    title TEXT,
                    neighborhood TEXT,
                    street TEXT,
                    city TEXT,
                    state TEXT,
                    area_m2 REAL,
                    bedrooms INTEGER,
                    bathrooms INTEGER,
                    parking_spaces INTEGER,
                    furnished INTEGER,
                    url TEXT,
                    first_seen TEXT,
                    last_seen TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    listing_id TEXT NOT NULL,
                    captured_at TEXT NOT NULL,
                    rent_price REAL,
                    price_per_m2 REAL,
                    condo_fee REAL,
                    furnished INTEGER NOT NULL,
                    UNIQUE(listing_id, captured_at),
                    FOREIGN KEY(listing_id) REFERENCES listings(listing_id) ON DELETE CASCADE
                )
                """
            )
            conn.commit()

    # ------------------------------------------------------------------
    def persist_listing(self, listing: Listing) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._persist_listing_with_cursor(cursor, listing)
            conn.commit()

    def persist_many(self, listings: Iterable[Listing]) -> int:
        count = 0
        with self._connect() as conn:
            cursor = conn.cursor()
            for listing in listings:
                self._persist_listing_with_cursor(cursor, listing)
                count += 1
            conn.commit()
        return count

    def _persist_listing_with_cursor(self, cursor: sqlite3.Cursor, listing: Listing) -> None:
        cursor.execute(
            """
            INSERT INTO listings (
                listing_id, title, neighborhood, street, city, state,
                area_m2, bedrooms, bathrooms, parking_spaces, furnished,
                url, first_seen, last_seen
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
                title=excluded.title,
                neighborhood=excluded.neighborhood,
                street=excluded.street,
                city=excluded.city,
                state=excluded.state,
                area_m2=excluded.area_m2,
                bedrooms=excluded.bedrooms,
                bathrooms=excluded.bathrooms,
                parking_spaces=excluded.parking_spaces,
                furnished=excluded.furnished,
                url=excluded.url,
                last_seen=excluded.last_seen
            """,
            (
                listing.listing_id,
                listing.title,
                listing.neighborhood,
                listing.street,
                listing.city,
                listing.state,
                listing.area_m2,
                listing.bedrooms,
                listing.bathrooms,
                listing.parking_spaces,
                int(listing.furnished),
                listing.url,
                listing.captured_at.isoformat(),
                listing.captured_at.isoformat(),
            ),
        )
        cursor.execute(
            """
            INSERT INTO price_history (
                listing_id, captured_at, rent_price, price_per_m2, condo_fee, furnished
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id, captured_at) DO UPDATE SET
                rent_price=excluded.rent_price,
                price_per_m2=excluded.price_per_m2,
                condo_fee=excluded.condo_fee,
                furnished=excluded.furnished
            """,
            (
                listing.listing_id,
                listing.captured_at.isoformat(),
                listing.rent_price,
                listing.price_per_m2,
                listing.condo_fee,
                int(listing.furnished),
            ),
        )

    # ------------------------------------------------------------------
    def get_listing_history(self, listing_id: str) -> List[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    l.listing_id,
                    l.title,
                    l.neighborhood,
                    l.street,
                    ph.captured_at,
                    ph.rent_price,
                    ph.price_per_m2,
                    ph.condo_fee,
                    ph.furnished
                FROM price_history AS ph
                JOIN listings AS l ON l.listing_id = ph.listing_id
                WHERE l.listing_id = ?
                ORDER BY ph.captured_at ASC
                """,
                (listing_id,),
            )
            return cursor.fetchall()

    def get_neighborhood_daily_average(
        self, neighborhood: str, *, furnished: Optional[bool] = None
    ) -> List[sqlite3.Row]:
        query = [
            "SELECT",
            "    date(ph.captured_at) AS captured_date,",
            "    AVG(ph.rent_price) AS avg_rent_price,",
            "    AVG(ph.price_per_m2) AS avg_price_per_m2,",
            "    COUNT(*) AS listings",
            "FROM price_history AS ph",
            "JOIN listings AS l ON l.listing_id = ph.listing_id",
            "WHERE l.neighborhood = ?",
        ]
        params: List[object] = [neighborhood]
        if furnished is not None:
            query.append("AND ph.furnished = ?")
            params.append(1 if furnished else 0)
        query.append("GROUP BY captured_date")
        query.append("ORDER BY captured_date ASC")
        sql = "\n".join(query)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()

    def list_listings(self, neighborhood: Optional[str] = None) -> List[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.cursor()
            if neighborhood:
                cursor.execute(
                    "SELECT * FROM listings WHERE neighborhood = ? ORDER BY title",
                    (neighborhood,),
                )
            else:
                cursor.execute("SELECT * FROM listings ORDER BY neighborhood, title")
            return cursor.fetchall()
