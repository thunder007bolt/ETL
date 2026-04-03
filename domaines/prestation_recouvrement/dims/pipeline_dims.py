"""
Pipeline dimensions 
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from shared.configs import settings
from domaines.prestation_recouvrement.dims.dim_config import DIM_CONFIG
from shared.utils.db_utils import get_source_connection, get_dw_connection
from shared.utils.sql_loader import load_sql
from shared.base.base_loader import BaseLoader

logger = logging.getLogger("cnss_etl.pipeline_dims")

_SQL_DIR = str(Path(__file__).parent / "sql")


class DimsPipeline:

    def run(self, batch_date: datetime = None) -> None:
        batch_date = batch_date or datetime.now()
        src_conn   = get_source_connection()
        dw_conn    = get_dw_connection()

        rows_loaded = 0

        try:
            loader = _GenericDimLoader(dw_conn)
            for cfg in DIM_CONFIG:
                rows_loaded += self._load_one(src_conn, loader, cfg)

            logger.info(f"Pipeline DIMS terminé — {rows_loaded} lignes chargées")

        except Exception as e:
            logger.exception("Erreur pipeline DIMS")
            raise
        finally:
            src_conn.close()
            dw_conn.close()

    def _load_one(self, src_conn, loader, cfg: dict) -> int:
        target   = cfg["target"]
        sql_file = cfg["sql_file"]
        key_cols = cfg["key_cols"]
        import gc
        gc.collect()
        
        try:
             
            #### Extraction
            sql = load_sql(_SQL_DIR, sql_file)
            with src_conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [col[0].upper() for col in cursor.description]
                rows    = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)

            if df.empty:
                logger.info(f"[{target}] aucune donnée")
                return 0

            #### Transformation
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
                
                
            #### Chargement
            strategy = cfg.get("strategy", "merge")
            seq_cols = cfg.get("seq_cols") or {}

            if strategy == "gtt":
                count = loader.merge_via_gtt(target, df, key_cols, seq_cols=seq_cols)
            else:
                count = loader.merge(target, df, key_cols, seq_cols=seq_cols)


            logger.info(f"[{target}] {count} lignes chargées")
            return count

        except Exception as e:
            logger.error(f"[{target}] erreur : {e}")
            raise


class _GenericDimLoader(BaseLoader):
    def load(self, df) -> int:
        raise NotImplementedError

    def merge(self, table: str, df: pd.DataFrame, key_cols: list, seq_cols: dict | None = None) -> int:
        return self._merge(table, df, key_cols, seq_cols=seq_cols)

    def full_reload(self, table: str, df: pd.DataFrame, seq_cols: dict | None = None) -> int:
        return self._full_reload(table, df, seq_cols=seq_cols)

    def merge_via_gtt(self, table: str, df: pd.DataFrame, key_cols: list, seq_cols: dict | None = None) -> int:
        return self._merge_via_gtt(table, df, key_cols, seq_cols=seq_cols)
