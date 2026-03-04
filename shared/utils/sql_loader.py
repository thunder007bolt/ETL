"""
Chargement des fichiers .sql
"""

from pathlib import Path


def load_sql(base: str, filename: str) -> str:
    """
    Charge le contenu d'un fichier .sql.
    """
    path = Path(base)
    sql_path = (path / "sql" / filename) if path.is_file() else (path / filename)

    if not sql_path.exists():
        raise FileNotFoundError(f"Fichier SQL introuvable : {sql_path}")

    return sql_path.read_text(encoding="utf-8")
