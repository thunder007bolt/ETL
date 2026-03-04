"""
Classe de base pour tous les extracteurs.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from shared.configs import settings
from shared.utils.sql_loader import load_sql

logger = logging.getLogger("cnss_etl.extractor")

# Date de début pour le mode FULL (équivalent "tout extraire")
_FULL_MODE_START = datetime(2000, 1, 1)


class BaseExtractor(ABC):
    """
    Extraction depuis Oracle SOURCE.
    """

    PIPELINE_NAME: str = ""
    SQL_DIR:       str = ""     # défini dans chaque sous-classe via __file__
    SOURCES:       dict = {}    # défini dans chaque sous-classe

    def __init__(self, src_conn):
        self.conn    = src_conn
        self.staging = Path(settings.STAGING_DIR)
        self.staging.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self, mode: str, batch_date: datetime) -> dict[str, pd.DataFrame]:
        """
        Extrait les données et retourne un dict de DataFrames par table source.
        """

    # ------------------------------------------------------------------
    # API interne
    # ------------------------------------------------------------------

    def _fetch_source(self, name: str, watermark: datetime) -> pd.DataFrame:
        """
        Charge le SQL depuis son fichier
        dynamiquement, puis exécute la requête.
        """
        
        config        = self.SOURCES[name]
        sql           = load_sql(self.SQL_DIR, config["file"])

        return self._fetch(sql)


    def _fetch(self, sql: str, params: list = None) -> pd.DataFrame:
        """Exécute une requête et retourne un DataFrame."""
        cursor = self.conn.cursor()
        cursor.execute(sql, params or [])
        columns = [col[0].lower() for col in cursor.description]
        rows    = cursor.fetchall()
        logger.info(f"[{self.PIPELINE_NAME}] {len(rows)} lignes extraites")
        return pd.DataFrame(rows, columns=columns)

    def _save_staging(self, df: pd.DataFrame, name: str) -> Path:
        """Sauvegarde le DataFrame en Parquet dans le répertoire staging."""
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.staging / f"{name}_{ts}.parquet"
        df.to_parquet(path, index=False)
        logger.info(f"Staging écrit : {path} ({len(df)} lignes)")
        return path
