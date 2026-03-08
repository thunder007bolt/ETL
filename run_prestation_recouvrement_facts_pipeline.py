"""

"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_prestation_recouvrement_facts_pipeline")

    logger.info(f"Démarrage")

    try:
        from domaines.prestation_recouvrement.facts.pipeline_facts import FactsPipeline
        FactsPipeline().run()

        logger.info("FactsPipeline terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"FactsPipeline échoué : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
