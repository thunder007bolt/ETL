"""
Relance une ou plusieurs tables DTM.

Usage :
    python run_dtm.py --dtm cotisations                     # une seule table
    python run_dtm.py --dtm cotisations recouvrement        # plusieurs tables
    python run_dtm.py                                        # toutes les tables DTM
    python run_dtm.py --date 20260301                        # CLICHE basé sur une date
"""

import argparse
import logging
import sys
from datetime import datetime

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("cnss_etl.run_dtm")

    from domaines.prestation_recouvrement.dtm.dtm_config import DTM_CONFIG

    available = [cfg["name"] for cfg in DTM_CONFIG]

    parser = argparse.ArgumentParser(description="Charge les tables DTM agrégées")
    parser.add_argument(
        "--dtm",
        nargs="*",
        choices=available,
        metavar="DTM",
        default=None,
        help=(
            "Nom(s) court(s) des tables DTM à charger "
            "(défaut : toutes). Ex : cotisations recouvrement"
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date de référence YYYYMMDD pour le CLICHE (défaut : aujourd'hui)",
    )
    args = parser.parse_args()

    batch_date = None
    if args.date:
        try:
            batch_date = datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            logger.critical(f"--date invalide : {args.date!r}. Format attendu : YYYYMMDD")
            sys.exit(1)

    names = args.dtm or None   # None = toutes les tables

    display_date = args.date or "aujourd'hui"
    logger.info(
        f"DTM run — tables={names or 'toutes'} "
        f"date={display_date}"
    )

    try:
        from domaines.prestation_recouvrement.dtm.pipeline_dtm import DtmPipeline

        DtmPipeline().run(names=names, batch_date=batch_date)
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Erreur DTM pipeline : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
