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
import pyarrow as pa
import pyarrow.parquet as pq
from shared.configs.log_setup import setup_logging

# pd.set_option('display.max_columns', None)  # Afficher toutes les colonnes
# pd.set_option('display.width', None)        # Largeur automatique
# pd.set_option('display.max_colwidth', None) # Ne pas tronquer le contenu


_SQL_DIR = str(
    Path(__file__).parent
    / "domaines" / "prestation_recouvrement" / "dims" / "sql"
)


def main():
    import gc
    gc.collect()
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
            cols  = [col[0].upper() for col in cursor.description]
            path  = staging.raw_path(settings.STAGING_DIR, args.dim, run_date)
            # Lecture en streaming fetchmany → écriture Parquet incrémentale.
            # Évite le double pic mémoire de fetchall() + pd.DataFrame() global.
            _FETCH = 50_000
            writer       = None
            writer_schema = None
            total        = 0
            try:
                while True:
                    rows = cursor.fetchmany(_FETCH)
                    if not rows:
                        break
                    chunk = pd.DataFrame(rows, columns=cols)
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    if writer is None:
                        # Schéma de référence fixé sur le premier batch
                        writer_schema = table.schema
                        writer = pq.ParquetWriter(str(path), writer_schema)
                    else:
                        # Cast vers le schéma de référence : corrige les dérives de type
                        # entre batches (ex: int64 → double quand des NULL apparaissent)
                        table = table.cast(writer_schema)
                    writer.write_table(table)
                    total += len(chunk)
            finally:
                if writer:
                    writer.close()
            logger.info(f"[E] {target} — {total} lignes extraites → {path}")

        # ── T : transformation ───────────────────────
        elif args.step == "T":
            df   = staging.read_raw(settings.STAGING_DIR, args.dim, run_date)
            print(df.columns.tolist())
            print(df.shape)

            print(df.head(10))
            #return
            # Renommage source → cible
            col_map = cfg.get("col_map", {})
            if col_map:
                df = df.rename(columns={k.upper(): v.upper() for k, v in col_map.items()})

            # Colonnes cible absentes de la source (métadonnées ETL, valeurs fixes…)
            for col, val in cfg.get("extra_cols", {}).items():
                df[col.upper()] = val() if callable(val) else val

            # Transformation métier spécifique à la dimension
            transform_fn = cfg.get("transform_fn")
            if transform_fn is not None:
                df = transform_fn(df)
            
            path = staging.write_transformed(settings.STAGING_DIR, args.dim, run_date, df)
            logger.info(f"[T] {target} — {len(df)} lignes → {path}")
            
        # ── L : chargement ───────────────────────────
        elif args.step == "L":
            from shared.utils.db_utils import get_dw_connection
            from domaines.prestation_recouvrement.dims.pipeline_dims import _GenericDimLoader

            dw_conn = get_dw_connection()
            df      = staging.read_transformed(settings.STAGING_DIR, args.dim, run_date)
             # Résolution des FK DWH (miroir de pipeline_dims._load_one)
            for fk in cfg.get("fk_lookups", []):
                cur = dw_conn.cursor()
                cur.execute(
                    f"SELECT {fk['join_col']}, {fk['fk_col']} FROM {fk['ref_table']}"
                )
                import pandas as _pd
                fk_map = _pd.DataFrame(
                    cur.fetchall(), columns=[fk["join_col"], fk["fk_col"]]
                )
                df = df.merge(fk_map, on=fk["join_col"], how="inner")
            seq_cols = cfg.get("seq_cols") or {}
            #return
            if df.empty:
                logger.info(f"[L] {target} — aucune donnée dans le staging")
            else:
                loader = _GenericDimLoader(dw_conn)
                count  = loader.merge_via_gtt(target, df, cfg["key_cols"], seq_cols=seq_cols)
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
