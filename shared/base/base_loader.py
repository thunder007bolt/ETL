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
    def _bulk_insert(self, table: str, df: pd.DataFrame,
                     seq_cols: dict | None = None) -> int:
        """
        Insert en masse avec executemany (mode append).

        seq_cols : dict {col_cible: nom_sequence} — colonnes dont la valeur
                   est fournie par SEQ.NEXTVAL (absent du DataFrame).
                   Ex : {"ID_DOSSIER": "SEQ_DIM_DOSSIER"}
        """
        if df.empty:
            return 0
        seq_cols     = seq_cols or {}
        df_cols      = list(df.columns)

        # Colonnes séquence en tête, puis colonnes DataFrame
        ins_cols  = list(seq_cols.keys()) + df_cols
        ins_vals  = [f"{s}.NEXTVAL" for s in seq_cols.values()] \
                  + [f":{i+1}" for i in range(len(df_cols))]

        sql    = (f"INSERT /*+ APPEND */ INTO {table}"
                  f" ({', '.join(ins_cols)}) VALUES ({', '.join(ins_vals)})")
        cursor = self.conn.cursor()
        total  = len(df)

        for start in range(0, total, self._MERGE_BATCH):
            cursor.executemany(sql, _prepare(df.iloc[start: start + self._MERGE_BATCH]))
        self.conn.commit()
        logger.info(f"[{table}] {total} lignes insérées")
        return total


    # ------------------------------------------------------------------
    # MERGE bulk — tables de dimensions 
    # ------------------------------------------------------------------


    def _merge(self, table: str, df: pd.DataFrame, key_cols: list, seq_cols: dict | None = None) -> int:
        """
        MERGE Oracle en masse via executemany, par lots de _MERGE_BATCH lignes.
        """
        if df.empty:
            return 0

        seq_cols     = seq_cols or {}
        all_cols     = list(df.columns)
        non_key_cols = [c for c in all_cols if c not in key_cols]

        using_select = ", ".join(
            f":{i+1} AS {c}" for i, c in enumerate(all_cols)
        )
        on_clause  = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in non_key_cols)

        # seq_cols : ajoutés à l'INSERT uniquement (NEXTVAL), jamais à l'UPDATE
        _ins_cols = list(seq_cols.keys()) + all_cols
        _ins_vals = [f"{s}.NEXTVAL" for s in seq_cols.values()] \
                  + [f"s.{c}" for c in all_cols]

        sql = f"""
            MERGE INTO {table} t
            USING (SELECT {using_select} FROM DUAL) s
            ON ({on_clause})
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(_ins_cols)}) VALUES ({', '.join(_ins_vals)})
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

    def _merge_via_gtt(self, table: str, df: pd.DataFrame, key_cols: list, seq_cols: dict | None = None) -> int:
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
        
        _ins_cols = list(seq_cols.keys()) + all_cols
        _ins_vals = [f"{s}.NEXTVAL" for s in seq_cols.values()] \
                  + [f"s.{c}" for c in all_cols]

        insert_sql = f"INSERT INTO {gtt} ({cols_sql}) VALUES ({placeholders})"
        merge_sql  = f"""
            MERGE INTO {table} t
            USING {gtt} s
            ON ({on_clause})
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(_ins_cols)}) VALUES ({', '.join(_ins_vals)})

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
    
      # ------------------------------------------------------------------
    # DELETE période — utilisé avant un INSERT en streaming
    # ------------------------------------------------------------------
    def _delete_cliche(self, table: str, cliche: str) -> None:
        """
        Supprime par lots de _DELETE_BATCH lignes pour éviter ORA-30036
        (UNDO tablespace full) sur les grandes tables de faits.

        Chaque lot sélectionne au plus _DELETE_BATCH ROWID correspondant
        au cliché MMYYYY, les supprime, puis committe. L'opération se répète
        jusqu'à ce qu'il ne reste plus rien à supprimer.

        Avec un index sur CLICHE la sélection des ROWID est un index range
        scan — rapide même sur 40 M lignes.
        """
        cursor = self.conn.cursor()
        total  = 0
        sql    = (
            f"DELETE FROM {table} WHERE ROWID IN ("
            f"  SELECT ROWID FROM {table}"
            f"  WHERE CLICHE = :1"
            f"  AND ROWNUM <= :2"
            f")"
        )
        while True:
            cursor.execute(sql, [cliche, self._DELETE_BATCH])
            deleted = cursor.rowcount
            self.conn.commit()
            total += deleted
            logger.debug(f"[{table}] DELETE batch {total} lignes (CLICHE={cliche})")
            if deleted < self._DELETE_BATCH:
                break
        logger.info(f"[{table}] DELETE CLICHE={cliche} — {total} lignes")

    # ------------------------------------------------------------------
    # ARCHIVE ODS + TRUNCATE DWH — remplacement de delete_period
    # ------------------------------------------------------------------
    def _archive_to_ods_and_truncate(self, table: str, ods_schema: str,
                                      cliche: str) -> int:
        """
        1. Nettoyage idempotent : DELETE ODS WHERE CLICHE = cliche — élimine tout
           snapshot partiel d'un run précédent interrompu (rapide si index sur CLICHE).
        2. Archive DWH → ODS via INSERT SELECT server-side (direct path, O(n) Oracle).
        3. TRUNCATE DWH (DDL, 0 UNDO, instantané).
        Retourne le nombre de lignes archivées.

        cliche : identifiant de période au format MMYYYY (ex. "032026").
        Le user DWH doit avoir DELETE + INSERT ON <ods_schema>.<table> (DBA).
        """
        cursor = self.conn.cursor()

        # Étape 0 : idempotence — nettoie un éventuel snapshot partiel
        cursor.execute(
            f"DELETE FROM {ods_schema}.{table} WHERE CLICHE = :1",
            [cliche],
        )
        deleted = cursor.rowcount
        self.conn.commit()
        if deleted:
            logger.warning(
                f"[ODS] {ods_schema}.{table} — {deleted} lignes partielles "
                f"(CLICHE={cliche}) nettoyées avant réarchivage"
            )

        # Étape 1 : archive server-side via colonnes explicites (évite ORA-00932
        # causé par un SELECT * qui mappe par position et non par nom lorsque
        # les structures DWH/ODS ont divergé, ex. après ajout de CLICHE).
        cursor.execute(
            """
            SELECT c.column_name
            FROM   user_tab_columns c
            JOIN   all_tab_columns  o
                   ON  o.owner       = :ods
                   AND o.table_name  = c.table_name
                   AND o.column_name = c.column_name
            WHERE  c.table_name = :tbl
            ORDER BY c.column_id
            """,
            {"ods": ods_schema.upper(), "tbl": table.upper()},
        )
        common_cols = ", ".join(row[0] for row in cursor.fetchall())
        if not common_cols:
            raise RuntimeError(
                f"Aucune colonne commune entre {table} (DWH) et "
                f"{ods_schema}.{table} (ODS) — vérifier la structure des tables."
            )
        archive_sql = (
            f"INSERT /*+ APPEND */ INTO {ods_schema}.{table} ({common_cols})"
            f" SELECT {common_cols} FROM {table}"
        )
        logger.debug(f"[ODS] {archive_sql}")
        cursor.execute(archive_sql)
        archived = cursor.rowcount
        self.conn.commit()
        logger.info(f"[ODS] {table} → {ods_schema}.{table} : {archived} lignes archivées")

        # Étape 2 : vidage DWH — DDL = auto-commit Oracle, 0 undo
        cursor.execute(f"TRUNCATE TABLE {table}")
        logger.info(f"[DWH] {table} TRUNCATE OK")

        return archived
   # ------------------------------------------------------------------
    def _delete_insert_period(self, table: str, df: pd.DataFrame,
                               period_cols: list | None = None,
                               seq_cols: dict | None = None) -> int:
        """
        Recharge les faits pour la période du run courant.

        L_ANNEE et L_MOIS sont ajoutés au DataFrame avant l'appel (via extra_cols)
        à partir de la date d'exécution — toutes les lignes du batch partagent
        donc la même valeur. Un seul DELETE suffit.

            DELETE FROM table WHERE L_ANNEE = :1 AND L_MOIS = :2
            INSERT /*+ APPEND */ toutes les lignes

        Cela permet de rejouer un mois sans toucher aux autres périodes.
        """
        if df.empty:
            return 0

        period_cols = period_cols or ["L_ANNEE", "L_MOIS"]
        # Toutes les lignes ont la même période (calculée à l'exécution)
        period_vals = [_to_python(df[c].iloc[0]) for c in period_cols]
        where       = " AND ".join(f"{c} = :{i+1}" for i, c in enumerate(period_cols))

        cursor = self.conn.cursor()
        cursor.execute(f"DELETE FROM {table} WHERE {where}", period_vals)
        self.conn.commit()
        logger.debug(f"[{table}] DELETE période {dict(zip(period_cols, period_vals))}")

        return self._bulk_insert(table, df, seq_cols=seq_cols)

    def _full_reload(self, table: str, df: pd.DataFrame,
                     seq_cols: dict | None = None) -> int:
        """
        Vide la table cible puis insère toutes les lignes en une passe.
        """
        if df.empty:
            return 0

        cursor = self.conn.cursor()
        cursor.execute(f"DELETE FROM {table}")
        self.conn.commit()
        logger.debug(f"[{table}] DELETE — table vidée")

        return self._bulk_insert(table, df, seq_cols=seq_cols)
