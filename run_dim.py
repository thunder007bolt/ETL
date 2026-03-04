"""
Relance une seule dimension avec granularité E / T / L.

Usage :
    python run_dim.py --dim branche       --step E            # Extract  → staging/raw/

"""

import argparse
import logging
import sys
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from shared.configs.log_setup import setup_logging



_SQL_DIR = str(
    Path(__file__).parent
    / "domaines" / "prestation_recouvrement" / "dims" / "sql"
)


def main():
    setup_logging()
    logger = logging.getLogger("cnss_etl.run_dim")

    from domaines.prestation_recouvrement.dims.dim_config import DIM_CONFIG
    available = [cfg["sql_file"].removesuffix(".sql") for cfg in DIM_CONFIG]

    parser = argparse.ArgumentParser(description="Relance une seule dimension ETL")
    parser.add_argument(
        "--dim",  
        required=True, 
        choices=available,
        metavar="DIM",
        help="Nom court de la dimension "
    )
    parser.add_argument(
        "--step",
        choices=["E", "T", "L"],
        default=None,
        help="E=extract, T=transform, L=load "
    )
   
    args = parser.parse_args()

    cfg      = next(c for c in DIM_CONFIG if c["sql_file"] == f"{args.dim}.sql")
    target   = cfg["target"]
    run_date = date.today().strftime("%Y%m%d")

    logger.info(f"[{target}] step={args.step or 'ETL'}  date={run_date}")

    src_conn = dw_conn = None
    try:
        from shared.configs import settings
        from shared.utils import staging
        from shared.utils.sql_loader import load_sql

        # ── E : extraction → staging/raw/ ───────────────────────────────────
        if args.step == "E":
            from shared.utils.db_utils import get_source_connection, get_dw_connection

            src_conn = get_source_connection()
            dw_conn  = get_dw_connection()

            sql    = load_sql(_SQL_DIR, cfg["sql_file"])
            cursor = src_conn.cursor()
            cursor.execute(sql)

            cols = [col[0].upper() for col in cursor.description]
            df   = pd.DataFrame(cursor.fetchall(), columns=cols)
            path = staging.write_raw(settings.STAGING_DIR, args.dim, run_date, df)
            logger.info(f"[E] {target} — {len(df)} lignes extraites → {path}")

        # ── T : transformation ───────────────────────
        elif args.step == "T":
            df   = staging.read_raw(settings.STAGING_DIR, args.dim, run_date)
            path = staging.write_transformed(settings.STAGING_DIR, args.dim, run_date, df)
            logger.info(f"[T] {target} — {len(df)} lignes → {path}")
            
        # ── L : chargement ───────────────────────────
        elif args.step == "L":
            from shared.utils.db_utils import get_dw_connection
            from domaines.prestation_recouvrement.dims.pipeline_dims import _GenericDimLoader

            dw_conn = get_dw_connection()
            df      = staging.read_transformed(settings.STAGING_DIR, args.dim, run_date)
            if df.empty:
                logger.info(f"[L] {target} — aucune donnée dans le staging")
            else:
                loader = _GenericDimLoader(dw_conn)
                count  = loader.merge_via_gtt(target, df, cfg["key_cols"])
                logger.info(f"[L] {target} — {count} lignes chargées")


        # ── ETL complet en mémoire (sans --step) ────────────────────────────
        else:
            from shared.utils.db_utils import get_source_connection, get_dw_connection
            from domaines.prestation_recouvrement.dims.pipeline_dims import (
                DimsPipeline, _GenericDimLoader,
            )
            src_conn = get_source_connection()
            dw_conn  = get_dw_connection()
            loader   = _GenericDimLoader(dw_conn)
            count    = DimsPipeline()._load_one(src_conn, loader, cfg)
            logger.info(f"[ETL] {target} — {count} lignes chargées")
        
        sys.exit(0)

    except Exception as e:
        logger.critical(f"Erreur : {e}", exc_info=True)
        sys.exit(1)

    finally:
        if src_conn:
            try: src_conn.close()
            except Exception: pass
        if dw_conn:
            try: dw_conn.close()
            except Exception: pass


if __name__ == "__main__":
    main()
