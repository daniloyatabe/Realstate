"""Utility functions for generating plots and analytical summaries."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from .storage import RealEstateDatabase


class PlottingError(RuntimeError):
    """Raised when generating a plot fails due to missing data."""


def _ensure_matplotlib():
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise ModuleNotFoundError(
            "matplotlib is required to generate charts. Install it with 'pip install matplotlib'."
        ) from exc
    return plt


def plot_listing_price_history(
    database_path: Path | str,
    listing_id: str,
    *,
    output_path: Optional[Path | str] = None,
    show: bool = False,
):
    """Generates a chart with the historic rental price for a single listing."""

    db = RealEstateDatabase(database_path)
    rows = db.get_listing_history(listing_id)
    if not rows:
        raise PlottingError(
            f"Nenhum registro encontrado para o imóvel {listing_id}. Execute a coleta primeiro."
        )

    plt = _ensure_matplotlib()

    dates = [_parse_datetime(row["captured_at"]) for row in rows]
    rent_prices = [row["rent_price"] for row in rows]
    ppm_values = [row["price_per_m2"] for row in rows]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(dates, rent_prices, marker="o", color="tab:blue", label="Aluguel total (R$)")
    ax1.set_xlabel("Data de captura")
    ax1.set_ylabel("Aluguel total (R$)")
    ax1.grid(True, axis="y", linestyle="--", alpha=0.3)

    handles, labels = ax1.get_legend_handles_labels()

    if any(value is not None for value in ppm_values):
        valid_dates = [d for d, v in zip(dates, ppm_values) if v is not None]
        valid_values = [v for v in ppm_values if v is not None]
        if valid_values:
            ax2 = ax1.twinx()
            ax2.plot(
                valid_dates,
                valid_values,
                marker="s",
                color="tab:orange",
                label="Preço por m² (R$)",
            )
            ax2.set_ylabel("Preço por m² (R$)")
            h2, l2 = ax2.get_legend_handles_labels()
            handles.extend(h2)
            labels.extend(l2)

    title = rows[0]["title"] if rows[0]["title"] else f"Imóvel {listing_id}"
    subtitle = rows[0]["neighborhood"] if rows[0]["neighborhood"] else "São Paulo"
    ax1.set_title(f"{title}\n{subtitle}")

    if handles:
        fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.1, 0.95))

    _save_or_show(fig, output_path, show)


def plot_neighborhood_average_price(
    database_path: Path | str,
    neighborhood: str,
    *,
    output_path: Optional[Path | str] = None,
    show: bool = False,
):
    """Generates a chart with the daily average price per m² in a neighbourhood."""

    db = RealEstateDatabase(database_path)
    furnished_rows = db.get_neighborhood_daily_average(neighborhood, furnished=True)
    unfurnished_rows = db.get_neighborhood_daily_average(neighborhood, furnished=False)

    if not furnished_rows and not unfurnished_rows:
        raise PlottingError(
            f"Nenhum dado histórico encontrado para o bairro {neighborhood}. Execute a coleta primeiro."
        )

    plt = _ensure_matplotlib()
    fig, ax = plt.subplots(figsize=(10, 6))

    def add_series(rows, label, color):
        if not rows:
            return
        parsed = [
            (_parse_date(row["captured_date"]), row["avg_price_per_m2"])
            for row in rows
            if row["avg_price_per_m2"] is not None
        ]
        if not parsed:
            return
        dates, values = zip(*parsed)
        ax.plot(dates, values, marker="o", label=label, color=color)

    add_series(furnished_rows, "Mobiliados", "tab:green")
    add_series(unfurnished_rows, "Não mobiliados", "tab:red")

    ax.set_title(f"Preço médio por m² - {neighborhood}")
    ax.set_xlabel("Data")
    ax.set_ylabel("Preço médio por m² (R$)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    ax.legend(loc="best")

    _save_or_show(fig, output_path, show)


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:  # pragma: no cover - defensive path
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _parse_date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _save_or_show(fig, output_path: Optional[Path | str], show: bool):
    plt = _ensure_matplotlib()
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.tight_layout()
        fig.savefig(output_path, bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)
