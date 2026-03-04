"""
Utilitaires de connexion Oracle (source et cible).
"""

import logging
import oracledb
from shared.configs import settings

logger = logging.getLogger("cnss_etl.db")

_thick_initialized = False


def _init_thick_mode() -> None:
    """Active le mode thick"""
    global _thick_initialized
    if _thick_initialized or not settings.ORA_THICK_MODE:
        return
    lib_dir = settings.ORA_CLIENT_DIR or None
    oracledb.init_oracle_client(lib_dir=lib_dir)
    _thick_initialized = True
    logger.debug(f"Oracle thick mode activé (lib_dir={lib_dir or 'PATH'})")

def get_source_connection():
    """Retourne une connexion à la base Oracle SOURCE (OLTP)."""
    _init_thick_mode()
    cfg = settings.SRC_DB
    dsn = f"{cfg['host']}:{cfg['port']}/{cfg['service']}"
    conn = oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=dsn)
    logger.debug(f"Connexion SOURCE établie : {cfg['user']}@{dsn}")
    return conn


def get_dw_connection():
    """Retourne une connexion à la base Oracle CIBLE (Data Warehouse)."""
    _init_thick_mode()
    cfg = settings.DW_DB
    dsn = f"{cfg['host']}:{cfg['port']}/{cfg['service']}"
    conn = oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=dsn)
    logger.debug(f"Connexion DW établie : {cfg['user']}@{dsn}")
    return conn
