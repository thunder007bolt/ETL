"""
Classe de base pour tous les loaders.
"""

import decimal
import logging
import math
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd

logger = logging.getLogger("cnss_etl.loader")


def _to_python(val):
    """Convertit une valeur pandas/numpy en type Python natif compatible Oracle."""
    if val is None:
        return None
    if val is pd.NA:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, pd.NaT.__class__) or val is pd.NaT:
        return None
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return None if math.isnan(val) else float(val)
    if isinstance(val, np.bool_):
        return bool(val)
    if isinstance(val, np.str_):
        return str(val)
    if isinstance(val, decimal.Decimal):
        return None if not val.is_finite() else float(val)
    return val


def _prepare(df: pd.DataFrame) -> list[tuple]:
    """Convertit toutes les lignes d'un DataFrame en tuples Oracle-compatibles."""
    return [tuple(_to_python(v) for v in row) for row in df.itertuples(index=False)]


class BaseLoader(ABC):
    """
    Chargement des données transformées dans le Data Warehouse Oracle.

    """
    _MERGE_BATCH = 5_000   

    def __init__(self, dw_conn):
        self.conn = dw_conn

    @abstractmethod
    def load(self, df) -> int:
        """Charge le DataFrame dans la table cible. Retourne le nombre de lignes chargées."""

    # ------------------------------------------------------------------
    # INSERT bulk — tables de faits
    # ------------------------------------------------------------------
    def _bulk_insert(self, table: str, df: pd.DataFrame) -> int:
        """Insert en masse avec executemany (mode append)."""
        if df.empty:
            return 0
        cols         = list(df.columns)
        placeholders = ", ".join(f":{i+1}" for i in range(len(cols)))
        sql          = f"INSERT /*+ APPEND */ INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        cursor       = self.conn.cursor()
        total        = len(df)

        for start in range(0, total, self._MERGE_BATCH):
            cursor.executemany(sql, _prepare(df.iloc[start: start + self._MERGE_BATCH]))
        self.conn.commit()
        logger.info(f"[{table}] {total} lignes insérées")
        return total


    # ------------------------------------------------------------------
    # MERGE bulk — tables de dimensions 
    # ------------------------------------------------------------------


    def _merge(self, table: str, df: pd.DataFrame, key_cols: list) -> int:
        """
        MERGE Oracle en masse via executemany, par lots de _MERGE_BATCH lignes.
        """
        if df.empty:
            return 0

        all_cols     = list(df.columns)
        non_key_cols = [c for c in all_cols if c not in key_cols]

        using_select = ", ".join(
            f":{i+1} AS {c}" for i, c in enumerate(all_cols)
        )
        on_clause  = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in non_key_cols)
        ins_cols   = ", ".join(all_cols)
        ins_vals   = ", ".join(f"s.{c}" for c in all_cols)

        sql = f"""
            MERGE INTO {table} t
            USING (SELECT {using_select} FROM DUAL) s
            ON ({on_clause})
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({ins_cols}) VALUES ({ins_vals})
        """

        cursor = self.conn.cursor()
        total  = len(df)

        for start in range(0, total, self._MERGE_BATCH):
            chunk = df.iloc[start: start + self._MERGE_BATCH]
            cursor.executemany(sql, _prepare(chunk))
            self.conn.commit()
            logger.debug(f"[{table}] MERGE {min(start + self._MERGE_BATCH, total)}/{total}")

        logger.info(f"[{table}] MERGE {total} lignes")
        return total


    def _ensure_gtt(self, table: str) -> str:
        """

        """
        gtt = f"GTT_{table}"

        cursor = self.conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM user_tables WHERE table_name = :1",
            [gtt],
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                """
                SELECT column_name, data_type, data_length,
                       data_precision, data_scale
                FROM   user_tab_columns
                WHERE  table_name = :1
                ORDER  BY column_id
                """,
                [table],
            )
            rows = cursor.fetchall()
            if not rows:
                raise RuntimeError(
                    f"Table {table} introuvable dans user_tab_columns — "
                    "vérifiez le schéma DW."
                )

            col_defs = []
            for col_name, dtype, length, precision, scale in rows:
                if dtype == "NUMBER":
                    if precision and scale is not None:
                        col_defs.append(f"{col_name} NUMBER({precision},{scale})")
                    elif precision:
                        col_defs.append(f"{col_name} NUMBER({precision})")
                    else:
                        col_defs.append(f"{col_name} NUMBER")
                elif dtype in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
                    col_defs.append(f"{col_name} {dtype}({length})")
                elif dtype == "DATE":
                    col_defs.append(f"{col_name} DATE")
                elif dtype.startswith("TIMESTAMP"):
                    col_defs.append(f"{col_name} TIMESTAMP")
                else:
                    col_defs.append(f"{col_name} {dtype}")

            ddl = (
                f"CREATE GLOBAL TEMPORARY TABLE {gtt} "
                f"({', '.join(col_defs)}) ON COMMIT DELETE ROWS"
            )
            cursor.execute(ddl)
            logger.info(f"[GTT] {gtt} créée automatiquement.")

        return gtt

    def _merge_via_gtt(self, table: str, df: pd.DataFrame, key_cols: list) -> int:
        """
    
        """
        if df.empty:
            return 0

        gtt          = self._ensure_gtt(table)
        all_cols     = list(df.columns)
        non_key_cols = [c for c in all_cols if c not in key_cols]

        cols_sql     = ", ".join(all_cols)
        placeholders = ", ".join(f":{i+1}" for i in range(len(all_cols)))
        on_clause    = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
        set_clause   = ", ".join(f"t.{c} = s.{c}" for c in non_key_cols)
        ins_vals     = ", ".join(f"s.{c}" for c in all_cols)

        insert_sql = f"INSERT INTO {gtt} ({cols_sql}) VALUES ({placeholders})"
        merge_sql  = f"""
            MERGE INTO {table} t
            USING {gtt} s
            ON ({on_clause})
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({cols_sql}) VALUES ({ins_vals})
        """      
        cursor = self.conn.cursor()
        total  = len(df)

        for start in range(0, total, self._MERGE_BATCH):
            chunk = df.iloc[start: start + self._MERGE_BATCH]
            cursor.executemany(insert_sql, _prepare(chunk))
        logger.debug(f"[{table}] GTT chargée ({total} lignes)")

        cursor.execute(merge_sql)
        self.conn.commit()

        cursor.execute(f"DROP TABLE {gtt}")
        logger.debug(f"[GTT] {gtt} supprimée.")

        logger.info(f"[{table}] MERGE via GTT {total} lignes")
        return total

    def _full_reload(self, table: str, df: pd.DataFrame) -> int:
        """
        Vide la table cible puis insère toutes les lignes en une passe.
        """
        if df.empty:
            return 0

        cursor = self.conn.cursor()
        cursor.execute(f"DELETE FROM {table}")
        self.conn.commit()
        logger.debug(f"[{table}] DELETE — table vidée")

        return self._bulk_insert(table, df)
