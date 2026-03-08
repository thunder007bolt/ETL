"""
Pipeline faits — charge les tables de faits via fact_config.
"""

import logging
from datetime import datetime
from pathlib import Path

import oracledb
import pandas as pd

from shared.configs import settings
from domaines.prestation_recouvrement.facts.fact_config import FACT_CONFIG
from shared.utils.db_utils import get_source_connection, get_dw_connection
from shared.utils.sql_loader import load_sql
from shared.base.base_loader import BaseLoader

logger = logging.getLogger("cnss_etl.pipeline_facts")

_SQL_DIR = str(Path(__file__).parent / "sql")


def _cast_oracle_types(df: pd.DataFrame, description) -> pd.DataFrame:
    for col_info in description:
        col_name = col_info[0].upper()
        col_type = col_info[1]
        scale    = col_info[5]
        if col_name not in df.columns:
            continue
        if col_type == oracledb.DB_TYPE_NUMBER:
            numeric = pd.to_numeric(df[col_name], errors="coerce")
            df[col_name] = numeric.astype("Int64") if scale == 0 else numeric.astype("float64")
    return df


class FactsPipeline:

    def run(self, batch_date: datetime = None) -> None:
        batch_date  = batch_date or datetime.now()
        src_conn    = get_source_connection()
        dw_conn     = get_dw_connection()
        rows_loaded = 0

        try:
            loader = _GenericFactLoader(dw_conn)
            for cfg in FACT_CONFIG:
                rows_loaded += self._load_one(src_conn, loader, cfg)

            logger.info(f"Pipeline FACTS terminé — {rows_loaded} lignes chargées")

        except Exception as e:
            logger.exception("Erreur pipeline FACTS")
            raise
        finally:
            src_conn.close()
            dw_conn.close()

    def _load_one(self, src_conn, loader, cfg: dict) -> int:
        target = cfg["target"]

        try:
            sql    = load_sql(_SQL_DIR, cfg["sql_file"])
            cursor = src_conn.cursor()
            cursor.execute(sql)

            description = cursor.description
            columns     = [col[0].upper() for col in description]
            df          = pd.DataFrame(cursor.fetchall(), columns=columns)
            df          = _cast_oracle_types(df, description)

            now = datetime.now()
            df["ANNEE"] = now.year
            df["MOIS"]  = now.month

            transform_fn = cfg.get("transform_fn")
            if transform_fn is not None:
                df = transform_fn(df)

            if df.empty:
                logger.info(f"[{target}] aucune donnée")
                return 0

            count = loader.delete_insert_period(target, df)
            logger.info(f"[{target}] {count} lignes chargées")
            return count

        except Exception as e:
            logger.error(f"[{target}] erreur : {e}")
            raise


class _GenericFactLoader(BaseLoader):
    def load(self, df) -> int:
        raise NotImplementedError

    def delete_insert_period(self, table: str, df: pd.DataFrame) -> int:
        return self._delete_insert_period(table, df)
