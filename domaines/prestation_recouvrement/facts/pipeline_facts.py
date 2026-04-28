"""
Pipeline faits — charge les tables de faits via fact_config.
"""

import logging
import argparse
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

    def run(self, batch_date: datetime = None,
             table_filter: str = None,
             exclude_heavy: bool = False) -> None:
        batch_date  = batch_date or datetime.now()
        src_conn    = get_source_connection()
        dw_conn     = get_dw_connection()
        rows_loaded = 0

        # --- Filtrage de la config ---
        configs = FACT_CONFIG
        if table_filter:
            configs = [c for c in configs if c["target"] == table_filter]
            if not configs:
                raise ValueError(f"Table '{table_filter}' introuvable dans FACT_CONFIG.")
        elif exclude_heavy:
            configs = [c for c in configs if not c.get("heavy", False)]

        try:
            loader = _GenericFactLoader(dw_conn)
            for cfg in configs:
                rows_loaded += self._load_one(src_conn, loader, cfg)

            logger.info(f"Pipeline FACTS terminé — {rows_loaded} lignes chargées")

        except Exception as e:
            logger.exception("Erreur pipeline FACTS")
            raise
        finally:
            src_conn.close()
            dw_conn.close()

    _FETCH = 100_000

    @staticmethod
    def _compute_rowid_chunks(cursor, table: str, chunk_size: int) -> list:
        """
        Scan unique O(n) sur les ROWID de la table source pour calculer
        les bornes (min_rowid, max_rowid) de chaque chunk de chunk_size lignes.
        Très rapide : Oracle ne lit que les ROWID (8 octets/ligne), pas les données.
        """
        cursor.execute(f"""
            SELECT MIN(RID), MAX(RID)
            FROM (
                SELECT RID,
                       CEIL(ROWNUM / :1) AS chunk_num
                FROM (
                    SELECT ROWID AS RID
                    FROM {table}
                    ORDER BY ROWID
                )
            )
            GROUP BY chunk_num
            ORDER BY MIN(RID)
        """, [chunk_size])
        return cursor.fetchall()

    def _load_one(self, src_conn, loader, cfg: dict) -> int:
        target = cfg["target"]
        import gc
        gc.collect()

        try:
            sql          = load_sql(_SQL_DIR, cfg["sql_file"])
            transform_fn = cfg.get("transform_fn")
            rowid_table  = cfg.get("rowid_chunk_table")

            now    = datetime.now()
            cliche = f"{now.month:02d}{now.year}"   # MMYYYY ex. "042026"

            ods = settings.ODS_SCHEMA
            if ods:
                loader.archive_and_truncate(target, ods, cliche)
            else:
                loader.delete_cliche(target, cliche)

            description = None
            columns     = None
            total       = 0

            loader.disable_indexes(target)
            try:
                if rowid_table:
                    # --- Chunking ROWID (grandes tables) ---
                    # Calcul des bornes en une seule passe sur la table source.
                    # Chaque chunk interroge exactement chunk_size lignes via ROWID
                    # → curseur de quelques secondes, pas de risque ORA-01555.
                    pre    = src_conn.cursor()
                    chunks = self._compute_rowid_chunks(pre, rowid_table, self._FETCH)
                    pre.close()
                    logger.info(f"[{target}] {len(chunks)} chunks ROWID à traiter")

                    for min_rid, max_rid in chunks:
                        cursor = src_conn.cursor()
                        cursor.execute(sql, {"min_rid": min_rid, "max_rid": max_rid})
                        if description is None:
                            description = cursor.description
                            columns     = [col[0].upper() for col in description]
                        rows = cursor.fetchall()
                        cursor.close()
                        if not rows:
                            continue
                        df = pd.DataFrame(rows, columns=columns)
                        df = _cast_oracle_types(df, description)
                        df["CLICHE"] = cliche
                        if transform_fn is not None:
                            df = transform_fn(df)
                        total += loader.insert_chunk(target, df)
                        logger.info(f"[{target}] {total} lignes insérées")

                else:
                    # --- fetchmany (petites/moyennes tables) ---
                    with src_conn.cursor() as cursor:
                        cursor.arraysize = self._FETCH
                        cursor.execute(sql)
                        description = cursor.description
                        columns     = [col[0].upper() for col in description]

                        while True:
                            rows = cursor.fetchmany(self._FETCH)
                            if not rows:
                                break
                            df = pd.DataFrame(rows, columns=columns)
                            df = _cast_oracle_types(df, description)
                            df["CLICHE"] = cliche
                            if transform_fn is not None:
                                df = transform_fn(df)
                            total += loader.insert_chunk(target, df)
                            del df, rows
                            gc.collect()

            finally:
                loader.rebuild_indexes(target)

            logger.info(f"[{target}] {total} lignes chargées")
            return total

        except Exception as e:
            logger.error(f"[{target}] erreur : {e}")
            raise


class _GenericFactLoader(BaseLoader):
    def load(self, df) -> int:
        raise NotImplementedError

    def archive_and_truncate(self, table: str, ods_schema: str, cliche: str) -> int:
        return self._archive_to_ods_and_truncate(table, ods_schema, cliche)

    def delete_cliche(self, table: str, cliche: str) -> None:
        self._delete_cliche(table, cliche)

    def insert_chunk(self, table: str, df: pd.DataFrame) -> int:
        return self._bulk_insert(table, df)

    def disable_indexes(self, table: str) -> None:
        self._disable_indexes(table)

    def rebuild_indexes(self, table: str) -> None:
        self._rebuild_indexes(table)
