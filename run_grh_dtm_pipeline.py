"""
Point d'entrée Jenkins — pipeline DTM GRH.

Usage :
    python run_grh_dtm_pipeline.py
    python run_grh_dtm_pipeline.py --dtm grh_ratio grh_formation
"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_grh_dtm_pipeline")

    parser = argparse.ArgumentParser(
        description="Lance le pipeline DTM GRH"
    )
    parser.add_argument(
        "--dtm",
        nargs="*",
        metavar="NAME",
        default=None,
        help="Noms courts des tables DTM GRH à charger (défaut : toutes)",
    )
    args = parser.parse_args()

    logger.info(
        f"Démarrage pipeline DTM GRH"
        + (f" — dtm={args.dtm}" if args.dtm else " — toutes les tables")
    )

    try:
        from domaines.grh.dtm.pipeline_dtm import DtmPipeline

        DtmPipeline().run(
            names=args.dtm or None,
        )

        logger.info("Pipeline DTM GRH terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Pipeline DTM GRH échoué : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
