"""
Lance le pipeline DTM complet (toutes les tables).
"""

import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_prestation_recouvrement_dtm_pipeline")

    logger.info("Démarrage pipeline DTM")

    try:
        from domaines.prestation_recouvrement.dtm.pipeline_dtm import DtmPipeline

        DtmPipeline().run()

        logger.info("Pipeline DTM terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Pipeline DTM échoué : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
