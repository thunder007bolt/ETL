"""
Génère un fichier SQL de création des tables miroirs ODS pour toutes les tables DTM.

Pour chaque table DTM.XXX déclarée dans DTM_CONFIG (prestation_recouvrement + grh),
ce script se connecte au DWH en lecture seule via ALL_TAB_COLUMNS pour lire la structure,
puis écrit un fichier SQL prêt à être exécuté par le DBA avec le user ODS.

Usage :
    python scripts/create_ods_dtm_mirrors.py
    python scripts/create_ods_dtm_mirrors.py --out /tmp/create_ods_dtm.sql
    python scripts/create_ods_dtm_mirrors.py --dtm cotisations dossier

Prérequis :
  - Variables d'environnement DWH configurées (même .env que le pipeline).
  - ODS_SCHEMA doit être défini dans l'environnement (ex. export ODS_SCHEMA=ODS).
  - Le fichier .sql généré doit être exécuté par le user ODS (ou SYSDBA).
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# --- Permet d'importer les modules du projet depuis scripts/ ---
sys.path.insert(0, str(Path(__file__).parents[1]))

from shared.configs.log_setup import setup_logging

# ---------------------------------------------------------------------------
# Tables à traiter (fusion des deux domaines)
# ---------------------------------------------------------------------------
from domaines.prestation_recouvrement.dtm.dtm_config import DTM_CONFIG as _PR
from domaines.grh.dtm.dtm_config                     import DTM_CONFIG as _GRH

DTM_ALL = _PR + _GRH


# ---------------------------------------------------------------------------
# Helpers DDL
# ---------------------------------------------------------------------------

def _ora_type(data_type: str, data_length: int, data_precision, data_scale) -> str:
    """Reconstruit la définition de type Oracle depuis ALL_TAB_COLUMNS."""
    if data_type == "NUMBER":
        if data_precision and data_scale is not None:
            return f"NUMBER({data_precision},{data_scale})"
        elif data_precision:
            return f"NUMBER({data_precision})"
        return "NUMBER"
    if data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
        return f"{data_type}({data_length} CHAR)"
    if data_type.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    return data_type   # DATE, CLOB, BLOB, ...


def _build_ddl(ods_schema: str, dwh_schema: str, table_name: str,
               columns: list, dwh_user: str) -> str:
    """
    Génère le bloc DDL SQL pour une table ODS miroir.

    - Aucune contrainte (ni NOT NULL, ni PK, ni FK, ni UNIQUE).
    - Un seul index sur CLICHE pour les purges du pipeline.
    - GRANT au user DWH.
    """
    ods_full  = f"{ods_schema}.{table_name}"
    dwh_full  = f"{dwh_schema}.{table_name}"
    idx_short = table_name.replace("DTM_", "")[:22]

    col_defs = [
        f"    {col[0]:<35} {_ora_type(col[1], col[2], col[3], col[4])}"
        for col in columns
    ]

    lines = [
        f"-- {'='*68}",
        f"-- Miroir ODS de {dwh_full}",
        f"-- Table de staging/archive — aucune contrainte",
        f"-- {'='*68}",
        f"CREATE TABLE {ods_full}",
        f"(",
        ",\n".join(col_defs),
        f");",
        f"",
    ]

    # Index sur CLICHE
    col_names = [c[0].upper() for c in columns]
    if "CLICHE" in col_names:
        idx_name = f"IDX_ODS_{idx_short}_CLICHE"[:30]
        lines.append(
            f"CREATE INDEX {ods_schema}.{idx_name}\n"
            f"    ON {ods_full} (CLICHE)\n"
            f"    TABLESPACE TBS_DWH_INDEX;"
        )
        lines.append("")

    # Grant au user DWH
    lines.append(f"GRANT SELECT, INSERT, DELETE ON {ods_full} TO {dwh_user};")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_logging()
    logger = logging.getLogger("cnss_etl.create_ods_dtm_mirrors")

    parser = argparse.ArgumentParser(
        description="Génère le SQL de création des tables miroirs ODS pour les tables DTM"
    )
    parser.add_argument(
        "--out",
        default=None,
        metavar="FILE",
        help="Fichier SQL de sortie (défaut : scripts/create_ods_dtm_mirrors.sql)",
    )
    parser.add_argument(
        "--dtm", nargs="*", metavar="NAME",
        help="Noms courts des tables DTM à traiter (défaut : toutes)",
    )
    args = parser.parse_args()

    out_path = Path(args.out) if args.out else Path(__file__).parent / "create_ods_dtm_mirrors.sql"

    from shared.configs import settings
    from shared.utils.db_utils import get_dw_connection

    ods_schema = settings.ODS_SCHEMA
    if not ods_schema:
        logger.critical("ODS_SCHEMA non configuré — définissez la variable d'environnement ODS_SCHEMA.")
        sys.exit(1)

    dwh_user = settings.DW_DB["user"].upper()

    configs = DTM_ALL
    if args.dtm:
        configs = [c for c in configs if c["name"] in args.dtm]
        if not configs:
            logger.error(f"Aucune table DTM trouvée pour : {args.dtm}")
            sys.exit(1)

    conn = get_dw_connection()

    try:
        blocks   = []
        skipped  = []
        errors   = []

        for cfg in configs:
            target     = cfg["target"]           # ex. "DTM.DTM_COTISATIONS"
            parts      = target.split(".", 1)
            dwh_schema = parts[0] if len(parts) == 2 else dwh_user
            table_name = parts[1] if len(parts) == 2 else parts[0]

            # Lecture structure depuis ALL_TAB_COLUMNS (lecture seule — pas de SELECT sur DTM)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name, data_type, data_length,
                           data_precision, data_scale, nullable
                    FROM   all_tab_columns
                    WHERE  owner      = :1
                    AND    table_name = :2
                    ORDER BY column_id
                    """,
                    [dwh_schema.upper(), table_name.upper()],
                )
                columns = cursor.fetchall()

            if not columns:
                msg = (
                    f"[{target}] introuvable dans ALL_TAB_COLUMNS "
                    f"(schéma={dwh_schema}, table={table_name}) — ignorée"
                )
                logger.error(msg)
                errors.append(table_name)
                continue

            logger.info(f"[{target}] {len(columns)} colonnes lues")
            blocks.append(_build_ddl(ods_schema, dwh_schema, table_name, columns, dwh_user))

    finally:
        conn.close()

    if not blocks:
        logger.error("Aucun DDL généré.")
        sys.exit(1)

    # Écriture du fichier SQL
    header = (
        f"-- ================================================================\n"
        f"-- Tables miroirs ODS pour les tables DTM\n"
        f"-- Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"-- ODS_SCHEMA : {ods_schema}\n"
        f"-- À exécuter avec le user ODS (ou SYSDBA)\n"
        f"-- ================================================================\n\n"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(header + "\n".join(blocks), encoding="utf-8")

    logger.info(
        f"\n{'='*50}\n"
        f"  Tables générées : {len(blocks)}\n"
        f"  Erreurs         : {len(errors)}\n"
        f"  Fichier SQL     : {out_path}\n"
        f"{'='*50}"
    )
    if errors:
        logger.warning(f"Tables introuvables : {errors}")
    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
