"""
Point d'entrée Jenkins — pipeline des faits GRH.

Usage :
    python run_grh_facts_pipeline.py
    python run_grh_facts_pipeline.py --exclude-heavy
    python run_grh_facts_pipeline.py --table FAIT_GRH_ABSENCE
"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_grh_facts_pipeline")

    parser = argparse.ArgumentParser(
        description="Lance le pipeline des faits GRH"
    )
    parser.add_argument(
        "--exclude-heavy",
        action="store_true",
        help="Exclut les tables marquées heavy=True",
    )
    parser.add_argument(
        "--table",
        default=None,
        metavar="TABLE",
        help="Lance uniquement la table cible indiquée",
    )
    args = parser.parse_args()

    logger.info(
        f"Démarrage pipeline faits GRH — exclude_heavy={args.exclude_heavy}"
        + (f" — table={args.table}" if args.table else "")
    )

    try:
        from domaines.grh.facts.pipeline_facts import FactsPipeline

        FactsPipeline().run(
            exclude_heavy=args.exclude_heavy,
            table_filter=args.table,
        )

        logger.info("FactsPipeline GRH terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"FactsPipeline GRH échoué : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
