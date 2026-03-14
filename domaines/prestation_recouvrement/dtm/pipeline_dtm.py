"""
Pipeline DTM — alimente les tables d'agrégats DTM.

Stratégie : DELETE WHERE CLICHE = :cliche  puis  INSERT (streaming).
CLICHE format : YYYYMM  (ex. '202603')  — différent des faits DWH (MMYYYY).

Source ET cible utilisent la même connexion get_dw_connection()
(DWH.* et DTM.* sont dans le même Data Warehouse Oracle).
"""

import logging
from datetime import datetime
from pathlib import Path

import oracledb
import pandas as pd

from domaines.prestation_recouvrement.dtm.dtm_config import DTM_CONFIG
from shared.base.base_loader import BaseLoader
from shared.utils.db_utils import get_dw_connection
from shared.utils.sql_loader import load_sql

logger = logging.getLogger("cnss_etl.pipeline_dtm")

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


class DtmPipeline:

    def __init__(self, fetch_size=100_000):
        self._FETCH = fetch_size

    def run(self, names: list[str] = None, batch_date: datetime = None) -> None:
        """
        Charge les tables DTM spécifiées (toutes si names=None).

        names     : liste de noms courts déclarés dans DTM_CONFIG
                    ex. ['cotisations', 'recouvrement']
        batch_date: date de référence pour le CLICHE (défaut = maintenant)
        """
        batch_date = batch_date or datetime.now()
        cliche     = f"{batch_date.month:02d}{batch_date.year}"   # MMYYYY — uniforme FAIT + DTM
        dw_conn    = get_dw_connection()

        configs = [
            c for c in DTM_CONFIG
            if names is None or c["name"] in names
        ]
        if not configs:
            logger.warning(f"Aucune table DTM correspondant à : {names}")
            return

        rows_loaded = 0
        try:
            loader = _DtmLoader(dw_conn)
            for cfg in configs:
                rows_loaded += self._load_one(dw_conn, loader, cfg, cliche)

            logger.info(f"Pipeline DTM terminé — {rows_loaded} lignes chargées")

        except Exception:
            logger.exception("Erreur pipeline DTM")
            raise
        finally:
            dw_conn.close()

    def _load_one(self, conn, loader: "_DtmLoader", cfg: dict, cliche: str) -> int:
        target = cfg["target"]

        try:
            loader.delete_cliche(target, cliche)

            logger.info(f"[{target}] Cliche {cliche} supprimé, début chargement...")
            
            total = 0
            for sql_entry in cfg["sql_files"]:
                sql    = load_sql(_SQL_DIR, sql_entry["file"])
                logger.info(f"[{target}][{sql_entry['label']}] Exécution de la requête SQL...")
                cursor = conn.cursor()
                cursor.arraysize = self._FETCH
                cursor.execute(sql, [cliche])   # :1 = CLICHE MMYYYY (uniforme FAIT + DTM)
                logger.info(f"[{target}][{sql_entry['label']}] Requête exécutée, début récupération des données...")

                description = cursor.description
                columns     = [col[0].upper() for col in description]

                while True:
                    rows = cursor.fetchmany(self._FETCH)
                    if not rows:
                        break
                    df    = pd.DataFrame(rows, columns=columns)
                    df    = _cast_oracle_types(df, description)
                    try:
                        chunk_size = loader.insert_chunk(target, df)
                    except Exception:
                        logger.error(f"[{target}] Chunk {total}–{total+len(df)} — valeurs max par colonne :")
                        for col in df.columns:
                            try:
                                logger.error(f"  {col}: min={df[col].min()!r}  max={df[col].max()!r}")
                            except Exception:
                                pass
                        raise
                    total += chunk_size
                    logger.info(f"[{target}][{sql_entry['label']}] Chunk traité : {chunk_size} lignes (total: {total})")

                logger.info(f"[{target}][{sql_entry['label']}] {total} lignes chargées")

            return total

        except Exception as e:
            logger.error(f"[{target}] erreur : {e}")
            raise


class _DtmLoader(BaseLoader):
    """Loader DTM — wrapping BaseLoader pour les tables d'agrégats DTM."""

    _DELETE_BATCH = 10_000

    def load(self, df) -> int:
        raise NotImplementedError

    def delete_cliche(self, table: str, cliche: str) -> None:
        self._delete_cliche(table, cliche)

    def insert_chunk(self, table: str, df: pd.DataFrame) -> int:
        return self._bulk_insert(table, df)
