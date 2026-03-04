"""
uniquemnt pour le dev 
"""

import platform
from pathlib import Path

_SYSTEM = platform.system()   # "Linux" | "Windows" | "Darwin"
_ROOT_DIR = Path(__file__).parents[2]                  
_ORA_DIR  = _ROOT_DIR / "shared" / "utils" / "instantclient_19_30"  

# ── Linux ──────────────────────────────────────────────────────────────────
if _SYSTEM == "Linux":
    DEFAULT_LOG_FILE    = Path("/var/log/cnss_etl/etl.log")
    DEFAULT_STAGING_DIR = Path("/tmp/cnss_etl/staging")
    DEFAULT_ORA_CLIENT  = _ORA_DIR

# ── Windows ────────────────────────────────────────────────────────────────
elif _SYSTEM == "Windows":
   DEFAULT_LOG_FILE    = Path("logs") / "etl.log"
   DEFAULT_STAGING_DIR = Path("staging")
   DEFAULT_ORA_CLIENT  = _ORA_DIR
