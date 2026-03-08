"""
Relance un seul fait avec granularité E / T / L.

Usage :
    python run_fact.py --fact contrainte                    # ETL complet (mémoire)
    python run_fact.py --fact contrainte --step E           # Extract  → staging/raw/
    python run_fact.py --fact contrainte --step T           # Transform → staging/transformed/
    python run_fact.py --fact contrainte --step L           # Load     ← staging/transformed/
    python run_fact.py --fact contrainte --step L --date 20260301
"""

import argparse
import logging
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from pathlib import Path

from shared.configs.log_setup import setup_logging

_SQL_DIR = str(
    Path(__file__).parent
    / "domaines" / "prestation_recouvrement" / "facts" / "sql"
)


def main():
    setup_logging()
    logger = logging.getLogger("cnss_etl.run_fact")

    from domaines.prestation_recouvrement.facts.fact_config import FACT_CONFIG
    available = [cfg["sql_file"].removesuffix(".sql") for cfg in FACT_CONFIG]

    parser = argparse.ArgumentParser(description="Relance un seul fait ETL")
    parser.add_argument("--fact", required=True, choices=available, metavar="FACT",
                        help="Nom court du fait (ex: contrainte, emploi)")
    parser.add_argument("--step", choices=["E", "T", "L"], default=None,
                        help="E=extract, T=transform, L=load  (défaut: ETL complet en mémoire)")
    parser.add_argument("--date", default=date.today().strftime("%Y%m%d"),
                        help="Date du fichier staging YYYYMMDD (défaut: aujourd'hui)")
    args = parser.parse_args()

    cfg      = next(c for c in FACT_CONFIG if c["sql_file"] == f"{args.fact}.sql")
    target   = cfg["target"]
    run_date = args.date

    logger.info(f"[{target}] step={args.step or 'ETL'} date={run_date}")

    src_conn = dw_conn = None
    try:
        from shared.configs import settings
        from shared.utils import staging
        from shared.utils.sql_loader import load_sql

        # ── E : extraction → staging/raw/ ───────────────────────────────────
        if args.step == "E":
            from shared.utils.db_utils import get_source_connection

            src_conn = get_source_connection()
            sql      = load_sql(_SQL_DIR, cfg["sql_file"])
            cursor   = src_conn.cursor()
            cursor.execute(sql)

            cols          = [col[0].upper() for col in cursor.description]
            path          = staging.raw_path(settings.STAGING_DIR, args.fact, run_date)
            writer        = None
            writer_schema = None
            total         = 0
            try:
                while True:
                    rows = cursor.fetchmany(50_000)
                    if not rows:
                        break
                    chunk = pd.DataFrame(rows, columns=cols)
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writer is None:
                        writer_schema = table.schema
                        writer = pq.ParquetWriter(str(path), writer_schema)
                    else:
                        table = table.cast(writer_schema)
                    writer.write_table(table)
                    total += len(chunk)
            finally:
                if writer:
                    writer.close()
            logger.info(f"[E] {target} — {total} lignes extraites → {path}")

        # ── T : transformation → staging/transformed/ ───────────────────────
        elif args.step == "T":
            df = staging.read_raw(settings.STAGING_DIR, args.fact, run_date)

            col_map = cfg.get("col_map", {})
            if col_map:
                df = df.rename(columns={k.upper(): v.upper() for k, v in col_map.items()})

            now = datetime.now()
            # new naming convention for period columns
            df["L_ANNEE"] = now.year
            df["L_MOIS"]  = now.month

            print(df.info())
            transform_fn = cfg.get("transform_fn")
            if transform_fn is not None:
                df = transform_fn(df)

            path = staging.write_transformed(settings.STAGING_DIR, args.fact, run_date, df)
            logger.info(f"[T] {target} — {len(df)} lignes → {path}")

        # ── L : chargement ← staging/transformed/ ───────────────────────────
        elif args.step == "L":
            from shared.utils.db_utils import get_dw_connection
            from domaines.prestation_recouvrement.facts.pipeline_facts import _GenericFactLoader

            dw_conn = get_dw_connection()
            df      = staging.read_transformed(settings.STAGING_DIR, args.fact, run_date)
            if df.empty:
                logger.info(f"[L] {target} — aucune donnée dans le staging")
            else:
                loader = _GenericFactLoader(dw_conn)
                # use renamed columns when determining period
                l_annee  = int(df["L_ANNEE"].iloc[0])
                l_mois   = int(df["L_MOIS"].iloc[0])
                loader.delete_period(target, l_annee, l_mois)
                count = loader.insert_chunk(target, df)
                logger.info(f"[L] {target} — {count} lignes chargées")

        # ── ETL complet en mémoire (sans --step) ────────────────────────────
        else:
            from shared.utils.db_utils import get_source_connection, get_dw_connection
            from domaines.prestation_recouvrement.facts.pipeline_facts import (
                FactsPipeline, _GenericFactLoader,
            )

            src_conn = get_source_connection()
            dw_conn  = get_dw_connection()
            loader   = _GenericFactLoader(dw_conn)
            count    = FactsPipeline()._load_one(src_conn, loader, cfg)
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
