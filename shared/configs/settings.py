"""
Lecture de la configuration depuis les variables d'environnement Jenkins.
Toutes les valeurs sensibles sont injectées par Jenkins (aucune valeur en dur).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Charge le .env situé à la racine du projet (sans écraser les vraies vars Jenkins)
load_dotenv(Path(__file__).parents[2] / ".env", override=False)

from shared.configs.plateform_defaults import DEFAULT_LOG_FILE, DEFAULT_STAGING_DIR, DEFAULT_ORA_CLIENT


def _require(var: str) -> str:
    value = os.environ.get(var)
    if not value:
        raise EnvironmentError(f"Variable d'environnement obligatoire manquante : {var}")
    return value


# ---------------------------------------------------------------------------
# Connexion Oracle SOURCE 
# ---------------------------------------------------------------------------
SRC_DB = {
    "host":     _require("SRC_ORACLE_HOST"),
    "port":     int(os.environ.get("SRC_ORACLE_PORT", "1521")),
    "service":  _require("SRC_ORACLE_SERVICE"),
    "user":     _require("SRC_ORACLE_USER"),
    "password": _require("SRC_ORACLE_PASSWORD"),
}

# ---------------------------------------------------------------------------
# Connexion Oracle CIBLE (Data Warehouse)
# ---------------------------------------------------------------------------
DW_DB = {
    "host":     _require("DW_ORACLE_HOST"),
    "port":     int(os.environ.get("DW_ORACLE_PORT", "1521")),
    "service":  _require("DW_ORACLE_SERVICE"),
    "user":     _require("DW_ORACLE_USER"),
    "password": _require("DW_ORACLE_PASSWORD"),
}

ETL_LOG_FILE    = os.environ.get("ETL_LOG_FILE", str(DEFAULT_LOG_FILE))
STAGING_DIR     = os.environ.get("STAGING_DIR",  str(DEFAULT_STAGING_DIR))

# ---------------------------------------------------------------------------
# Oracle client (thick mode)
# ORA_THICK_MODE : true  → mode thick (requis pour Oracle < 12.1)
#                  false → mode thin (Oracle ≥ 12.1, pas de client requis)
# ORA_CLIENT_DIR : chemin vers Oracle Instant Client (vide = cherche dans PATH)
# ---------------------------------------------------------------------------
ORA_THICK_MODE = os.environ.get("ORA_THICK_MODE", "true").lower() == "true"
ORA_CLIENT_DIR = os.environ.get("ORA_CLIENT_DIR", str(DEFAULT_ORA_CLIENT))

# ---------------------------------------------------------------------------
# Archivage ODS (Operational Data Store)
# ODS_SCHEMA : schéma Oracle cible pour l'archivage server-side avant TRUNCATE
#              vide = désactive l'archivage (comportement historique delete_period)
# ---------------------------------------------------------------------------
ODS_SCHEMA: str = os.getenv("ODS_SCHEMA", "")  # ex. "CNSS_ODS"
