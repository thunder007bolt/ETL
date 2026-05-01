"""
Point d'entrée Jenkins — pipeline des faits Prestation & Recouvrement.

Usage :
    python run_prestation_recouvrement_facts_pipeline.py
    python run_prestation_recouvrement_facts_pipeline.py --exclude-heavy
    python run_prestation_recouvrement_facts_pipeline.py --table FAIT_DEBOURS
"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_prestation_recouvrement_facts_pipeline")

    parser = argparse.ArgumentParser(
        description="Lance le pipeline des faits Prestation & Recouvrement"
    )
    parser.add_argument(
        "--exclude-heavy",
        action="store_true",
        help="Exclut les tables marquées heavy=True (ex. FAIT_DEBOURS, FAIT_DOSSIER)",
    )
    parser.add_argument(
        "--table",
        default=None,
        metavar="TABLE",
        help="Lance uniquement la table cible indiquée (ex. FAIT_DEBOURS)",
    )
    args = parser.parse_args()

    logger.info(
        f"Démarrage — exclude_heavy={args.exclude_heavy}"
        + (f" — table={args.table}" if args.table else "")
    )

    try:
        from domaines.prestation_recouvrement.facts.pipeline_facts import FactsPipeline

        FactsPipeline().run(
            exclude_heavy=args.exclude_heavy,
            table_filter=args.table,
        )

        logger.info("FactsPipeline terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"FactsPipeline échoué : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
