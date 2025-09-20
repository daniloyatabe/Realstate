"""Command line interface for the Zap Imóveis scraping toolkit."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Iterable, List

from .config import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_DELAY_BETWEEN_REQUESTS,
    DEFAULT_NEIGHBORHOODS,
    Neighborhood,
)
from .reporting import plot_listing_price_history, plot_neighborhood_average_price
from .scraper import ZapScraper
from .storage import RealEstateDatabase


def parse_arguments(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ferramenta para coletar diariamente preços de aluguel por m² em São Paulo usando o Zap Imóveis."
        )
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE_PATH,
        help="Caminho do banco SQLite onde os dados serão armazenados.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Nível de verbosidade do log.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser(
        "collect", help="Executa o processo de coleta de dados a partir do Zap Imóveis."
    )
    collect_parser.add_argument(
        "--neighborhood",
        dest="neighborhoods",
        action="append",
        help="Lista de bairros a coletar. Pode ser utilizado múltiplas vezes.",
    )
    collect_parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Número máximo de páginas por bairro (útil para testes).",
    )
    collect_parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_BETWEEN_REQUESTS,
        help="Intervalo em segundos entre requisições consecutivas ao servidor.",
    )
    collect_parser.add_argument(
        "--daily",
        action="store_true",
        help="Mantém a coleta ativa diariamente (executa novamente a cada 24h).",
    )
    collect_parser.add_argument(
        "--interval-hours",
        type=float,
        default=24.0,
        help="Intervalo em horas entre execuções quando --daily é usado.",
    )

    report_parser = subparsers.add_parser(
        "report", help="Gera relatórios e gráficos a partir dos dados coletados."
    )
    report_subparsers = report_parser.add_subparsers(dest="report_type", required=True)

    listing_report = report_subparsers.add_parser(
        "listing", help="Gera gráfico de variação de preço para um imóvel específico."
    )
    listing_report.add_argument("listing_id", help="Identificador do imóvel no Zap Imóveis.")
    listing_report.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Arquivo onde o gráfico será salvo. Se omitido, será exibido na tela.",
    )
    listing_report.add_argument(
        "--show",
        action="store_true",
        help="Exibe o gráfico na tela (requer ambiente gráfico).",
    )

    neighborhood_report = report_subparsers.add_parser(
        "neighborhood", help="Gera gráfico do preço médio por m² para um bairro."
    )
    neighborhood_report.add_argument("neighborhood", help="Nome do bairro.")
    neighborhood_report.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Arquivo onde o gráfico será salvo. Se omitido, será exibido na tela.",
    )
    neighborhood_report.add_argument(
        "--show",
        action="store_true",
        help="Exibe o gráfico na tela (requer ambiente gráfico).",
    )

    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_arguments(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    logging.debug("Argumentos recebidos: %s", args)

    database_path: Path = args.database
    database_path.parent.mkdir(parents=True, exist_ok=True)

    if args.command == "collect":
        run_collection(database_path, args)
    elif args.command == "report":
        run_report(database_path, args)
    else:  # pragma: no cover - defensive programming
        raise ValueError(f"Comando desconhecido: {args.command}")

    return 0


def run_collection(database_path: Path, args: argparse.Namespace) -> None:
    neighborhoods = _resolve_neighborhoods(args.neighborhoods)
    interval_seconds = max(0.0, float(args.interval_hours) * 3600.0)

    def single_run() -> None:
        logging.info("Iniciando coleta de dados para %s bairros", len(neighborhoods))
        scraper = ZapScraper(neighborhoods=neighborhoods, delay=args.delay)
        db = RealEstateDatabase(database_path)
        total = db.persist_many(scraper.scrape(max_pages=args.max_pages))
        logging.info("Coleta finalizada. %s registros processados.", total)

    single_run()

    if args.daily:
        while True:
            logging.info("Aguardando %s horas para a próxima execução.", args.interval_hours)
            time.sleep(interval_seconds)
            try:
                single_run()
            except Exception:  # pragma: no cover - runtime safeguard
                logging.exception("Erro durante a coleta diária. Retentando na próxima janela.")


def run_report(database_path: Path, args: argparse.Namespace) -> None:
    if args.report_type == "listing":
        plot_listing_price_history(
            database_path,
            args.listing_id,
            output_path=args.output,
            show=args.show,
        )
    elif args.report_type == "neighborhood":
        plot_neighborhood_average_price(
            database_path,
            args.neighborhood,
            output_path=args.output,
            show=args.show,
        )
    else:  # pragma: no cover
        raise ValueError(f"Tipo de relatório desconhecido: {args.report_type}")


def _resolve_neighborhoods(custom: List[str] | None) -> List[Neighborhood]:
    if not custom:
        return DEFAULT_NEIGHBORHOODS
    return [Neighborhood(name=name, query=name) for name in custom]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
