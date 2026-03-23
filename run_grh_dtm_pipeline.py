"""
Lance le pipeline DTM GRH complet (toutes les tables).
"""

import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_grh_dtm_pipeline")

    logger.info("Démarrage pipeline DTM GRH")

    try:
        from domaines.grh.dtm.pipeline_dtm import DtmPipeline

        DtmPipeline().run()

        logger.info("Pipeline DTM GRH terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Pipeline DTM GRH échoué : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
