"""
Point d'entrée Jenkins — pipeline DTM Prestation & Recouvrement.

Usage :
    python run_prestation_recouvrement_dtm_pipeline.py
    python run_prestation_recouvrement_dtm_pipeline.py --exclude-heavy
    python run_prestation_recouvrement_dtm_pipeline.py --dtm cotisations recouvrement
"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_prestation_recouvrement_dtm_pipeline")

    parser = argparse.ArgumentParser(
        description="Lance le pipeline DTM Prestation & Recouvrement"
    )
    parser.add_argument(
        "--exclude-heavy",
        action="store_true",
        help="Exclut les tables marquées heavy=True (ex. DTM_DOSSIER, DTM_SALAIRE)",
    )
    parser.add_argument(
        "--dtm",
        nargs="*",
        metavar="NAME",
        default=None,
        help="Noms courts des tables DTM à charger (défaut : toutes)",
    )
    args = parser.parse_args()

    logger.info(
        f"Démarrage pipeline DTM PR — exclude_heavy={args.exclude_heavy}"
        + (f" — dtm={args.dtm}" if args.dtm else "")
    )

    try:
        from domaines.prestation_recouvrement.dtm.pipeline_dtm import DtmPipeline

        DtmPipeline().run(
            names=args.dtm or None,
            exclude_heavy=args.exclude_heavy,
        )

        logger.info("Pipeline DTM PR terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Pipeline DTM PR échoué : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
