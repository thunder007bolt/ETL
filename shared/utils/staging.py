"""
Persistance intermédiaire entre les steps E / T / L.

Convention de nommage :
    staging/raw/{name}_{YYYYMMDD}.parquet         ← sortie du step E
    staging/transformed/{name}_{YYYYMMDD}.parquet ← sortie du step T
"""

from pathlib import Path

import pandas as pd


def _raw_path(staging_dir: str, name: str, run_date: str) -> Path:
    return Path(staging_dir) / "raw" / f"{name}_{run_date}.parquet"


def _transformed_path(staging_dir: str, name: str, run_date: str) -> Path:
    return Path(staging_dir) / "transformed" / f"{name}_{run_date}.parquet"


def write_raw(staging_dir: str, name: str, run_date: str, df: pd.DataFrame) -> Path:
    path = _raw_path(staging_dir, name, run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_raw(staging_dir: str, name: str, run_date: str) -> pd.DataFrame:
    path = _raw_path(staging_dir, name, run_date)
    if not path.exists():
        raise FileNotFoundError(f"Fichier staging raw introuvable : {path}")
    return pd.read_parquet(path)


def write_transformed(staging_dir: str, name: str, run_date: str, df: pd.DataFrame) -> Path:
    path = _transformed_path(staging_dir, name, run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_transformed(staging_dir: str, name: str, run_date: str) -> pd.DataFrame:
    path = _transformed_path(staging_dir, name, run_date)
    if not path.exists():
        raise FileNotFoundError(f"Fichier staging transformed introuvable : {path}")
    return pd.read_parquet(path)
