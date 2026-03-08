import math
import decimal
import time
import tracemalloc
import pandas as pd
import numpy as np


# ── méthode originale ──────────────────────────────────────────────────────

def _to_python_original(val):
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

def _prepare_original(df: pd.DataFrame) -> list[tuple]:
    return [tuple(_to_python_original(v) for v in row) for row in df.itertuples(index=False)]


# ── méthode optimisée v1 (vectorisé + générateur) ─────────────────────────

def _to_python(val):
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

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    for col in clean.columns:
        dtype = clean[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            clean[col] = clean[col].astype(object).where(clean[col].notna(), None)
        elif pd.api.types.is_float_dtype(dtype):
            clean[col] = clean[col].astype(object).where(clean[col].notna(), None)
        elif pd.api.types.is_bool_dtype(dtype):
            clean[col] = clean[col].astype(object).where(clean[col].notna(), None)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            clean[col] = clean[col].astype(object).where(clean[col].notna(), None)
        elif dtype == object:
            clean[col] = clean[col].where(clean[col].notna(), None)
    return clean

def _prepare_optimized(df: pd.DataFrame, chunksize: int = 5000):
    for start in range(0, len(df), chunksize):
        chunk = _clean_dataframe(df.iloc[start:start + chunksize])
        for row in chunk.itertuples(index=False):
            yield tuple(_to_python(v) for v in row)


# ── méthode v2 (vectorisé + to_records) ───────────────────────────────────

def _prepare_v2(df: pd.DataFrame, chunksize: int = 5000):
    for start in range(0, len(df), chunksize):
        chunk = df.iloc[start:start + chunksize].copy()

        for col in chunk.columns:
            dtype = chunk[col].dtype

            if pd.api.types.is_integer_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)
            elif pd.api.types.is_float_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)
            elif pd.api.types.is_bool_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)
            elif dtype == object:
                chunk[col] = chunk[col].apply(
                    lambda v: None if v is None or v is pd.NA
                    else float(v) if isinstance(v, decimal.Decimal) and v.is_finite()
                    else None if isinstance(v, decimal.Decimal)
                    else v
                )

        yield from chunk.to_records(index=False).tolist()


# ── méthode v3 (vectorisé + to_records + détection Decimal optimisée) ─────

def _prepare_v3(df: pd.DataFrame, chunksize: int = 5000):
    """
    Amélioration de V2 :
    - Détection des colonnes Decimal une seule fois avant les chunks
    - .apply() uniquement sur les colonnes qui contiennent réellement des Decimal
    - Colonnes object sans Decimal → vectorisé pur (where + notna)
    """
    # Détection Decimal une seule fois sur un échantillon de 100 lignes
    sample = df.iloc[:100]
    decimal_cols = {
        col for col in df.columns
        if df[col].dtype == object
        and sample[col].dropna().apply(
            lambda v: isinstance(v, decimal.Decimal)
        ).any()
    }

    for start in range(0, len(df), chunksize):
        chunk = df.iloc[start:start + chunksize].copy()

        for col in chunk.columns:
            dtype = chunk[col].dtype

            if pd.api.types.is_integer_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)

            elif pd.api.types.is_float_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)

            elif pd.api.types.is_bool_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)

            elif pd.api.types.is_datetime64_any_dtype(dtype):
                chunk[col] = chunk[col].astype(object).where(chunk[col].notna(), None)

            elif dtype == object:
                if col in decimal_cols:
                    # .apply() uniquement si la colonne contient des Decimal
                    chunk[col] = chunk[col].apply(
                        lambda v: None if v is None or v is pd.NA
                        else float(v) if isinstance(v, decimal.Decimal) and v.is_finite()
                        else None if isinstance(v, decimal.Decimal)
                        else v
                    )
                else:
                    # Vectorisé pur — aucun .apply()
                    chunk[col] = chunk[col].where(chunk[col].notna(), None)

        yield from chunk.to_records(index=False).tolist()


# ── benchmark ──────────────────────────────────────────────────────────────

def benchmark(label: str, fn, *args, runs: int = 3):
    times = []
    peaks = []
    for _ in range(runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        result = list(fn(*args))
        t1 = time.perf_counter()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(t1 - t0)
        peaks.append(peak / 1024 / 1024)
    avg_time = sum(times) / runs
    avg_peak = sum(peaks) / runs
    print(f"\n{'─'*45}")
    print(f"  {label}")
    print(f"{'─'*45}")
    print(f"  Temps moyen  : {avg_time:.3f}s  (sur {runs} runs)")
    print(f"  Mémoire pic  : {avg_peak:.1f} MB")
    return avg_time, avg_peak


def compare(df: pd.DataFrame, chunksize: int = 5000, runs: int = 3):
    print(f"\n{'═'*45}")
    print(f"  BENCHMARK  —  {len(df):,} lignes × {len(df.columns)} colonnes")
    print(f"{'═'*45}")

    t_orig, m_orig = benchmark("ORIGINAL   (liste + _to_python)",                  _prepare_original,  df,            runs=runs)
    t_opti, m_opti = benchmark("OPTIMISÉ   (vectorisé + générateur)",              _prepare_optimized, df, chunksize, runs=runs)
    t_v2,   m_v2   = benchmark("V2         (vectorisé + to_records)",              _prepare_v2,        df, chunksize, runs=runs)
    t_v3,   m_v3   = benchmark("V3         (vectorisé + to_records + Decimal opt)",_prepare_v3,        df, chunksize, runs=runs)

    print(f"\n{'═'*45}")
    print(f"  RÉSULTATS vs ORIGINAL")
    print(f"{'═'*45}")

    for label, t, m in [
        ("OPTIMISÉ", t_opti, m_opti),
        ("V2      ", t_v2,   m_v2),
        ("V3      ", t_v3,   m_v3),
    ]:
        gt = (t_orig - t) / t_orig * 100
        gm = (m_orig - m) / m_orig * 100
        sign_t = "✅" if gt > 5 else "⚠️ " if gt < -5 else "➡️ "
        sign_m = "✅" if gm > 5 else "⚠️ " if gm < -5 else "➡️ "
        print(f"\n  {label}")
        print(f"  {sign_t} Temps   : {t_orig:.3f}s  →  {t:.3f}s   ({gt:+.1f}%)")
        print(f"  {sign_m} Mémoire : {m_orig:.1f} MB  →  {m:.1f} MB   ({gm:+.1f}%)")

    print(f"\n{'═'*45}\n")


# ── DataFrame de test ──────────────────────────────────────────────────────

if __name__ == "__main__":
    N = 100_000  # ← change : 100_000 / 300_000 / 500_000

    rng = np.random.default_rng(42)

    df_test = pd.DataFrame({
        "id": pd.array(
            rng.integers(0, N, N), dtype=pd.Int64Dtype()
        ),
        "montant": np.where(
            rng.random(N) < 0.1, np.nan, rng.random(N) * 1000
        ),
        "actif": pd.array(
            np.where(rng.random(N) < 0.1, pd.NA, rng.integers(0, 2, N).astype(bool)),
            dtype=pd.BooleanDtype()
        ),
        "label": pd.array(
            np.where(rng.random(N) < 0.1, None, rng.choice(["foo", "bar", "baz"], N))
        ),
        "created": pd.to_datetime(
            np.where(
                rng.random(N) < 0.1,
                pd.NaT,
                pd.date_range("2020-01-01", periods=N, freq="s")
            )
        ),
        "coeff": [
            None if rng.random() < 0.1
            else decimal.Decimal(str(round(float(x), 4)))
            for x in rng.random(N)
        ],
    })

    compare(df_test, chunksize=5000, runs=3)