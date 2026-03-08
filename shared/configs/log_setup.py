"""
Configuration centralisée du logging CNSS ETL.

Deux handlers :
  - Console : colorlog.StreamHandler — couleurs par niveau, streaming temps réel
  - Fichier : RotatingFileHandler   — texte brut (codes ANSI filtrés), 10 MB x 5

Appelé une seule fois depuis le point d'entrée (main.py, run_dim.py, run_fact.py).
La configuration est lue depuis les variables d'environnement :
  ETL_LOG_LEVEL : DEBUG | INFO | WARNING | ERROR  (défaut: INFO)
  ETL_LOG_FILE  : chemin du fichier de log         (défaut: voir platform_defaults.py)
"""

import logging
import logging.handlers
import os
import re
from pathlib import Path

import colorlog

from shared.configs.plateform_defaults import DEFAULT_LOG_FILE


class AnsiEscapeCodeFilter(logging.Filter):
    """Supprime les codes ANSI du message avant écriture dans le fichier de log."""

    _PATTERN = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = self._PATTERN.sub('', record.msg)
        return True


def setup_logging() -> None:
    """
    Configure le logging global de l'application.
    Doit être appelé une seule fois, au tout début du point d'entrée,
    avant tout import de module métier.
    """
    level_name = os.environ.get("ETL_LOG_LEVEL", "INFO").upper()
    log_file   = os.environ.get("ETL_LOG_FILE", str(DEFAULT_LOG_FILE))
    level      = getattr(logging, level_name, logging.INFO)

    # ── Console handler — colorlog ────────────────────────────────────────
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(colorlog.ColoredFormatter(
        fmt="%(log_color)s%(asctime)s [%(levelname)-8s] %(name)s — %(message)s%(reset)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red,bg_white",
        },
    ))

    # ── File handler — RotatingFileHandler, texte brut ───────────────────
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file,
        maxBytes=0.5 * 1024 * 1024,  # 0.5 MB par fichier
        backupCount=20,              # etl.log, etl.log.1, ..., etl.log.5
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    file_handler.addFilter(AnsiEscapeCodeFilter())

    # ── Root logger ───────────────────────────────────────────────────────
    root = logging.getLogger()
    if root.handlers:
        # Évite la double configuration si appelé plusieurs fois
        return
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)
