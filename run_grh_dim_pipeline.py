"""

"""

import argparse
import logging
import sys

from shared.configs.log_setup import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger("run_grh_dim_pipeline")

    logger.info(f"Démarrage")

    try:
        from domaines.grh.dims.pipeline_dims import DimsPipeline
        DimsPipeline().run()

        logger.info("DimsPipeline terminé avec succès.")
        sys.exit(0)

    except Exception as e:
        logger.critical(f"DimsPipeline échoué : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
