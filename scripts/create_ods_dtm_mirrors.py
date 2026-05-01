"""
Génère et exécute les tables miroirs ODS pour toutes les tables DTM.

Pour chaque table DTM.XXX déclarée dans DTM_CONFIG (prestation_recouvrement + grh),
ce script :
  1. Lit la structure réelle depuis ALL_TAB_COLUMNS (connexion DWH live).
  2. Génère le DDL CREATE TABLE ODS.<nom_table>.
  3. Crée un index sur CLICHE (utilisé par le pipeline archive_to_ods_and_truncate).
  4. Accorde SELECT, INSERT, DELETE au user DWH sur la table ODS.

Usage :
    python scripts/create_ods_dtm_mirrors.py              # crée toutes les tables manquantes
    python scripts/create_ods_dtm_mirrors.py --dry-run    # affiche les DDL sans exécuter
    python scripts/create_ods_dtm_mirrors.py --force      # recrée même si la table existe déjà

Prérequis :
  - Variables d'environnement DWH configurées (même .env que le pipeline).
  - Le user DWH doit avoir CREATE TABLE et CREATE ANY INDEX sur le schéma ODS.
  - ODS_SCHEMA doit être défini dans l'environnement (ex. export ODS_SCHEMA=CNSS_ODS).
"""

import argparse
import logging
import sys
from pathlib import Path

# --- Permet d'importer les modules du projet depuis scripts/ ---
sys.path.insert(0, str(Path(__file__).parents[1]))

from shared.configs.log_setup import setup_logging

# ---------------------------------------------------------------------------
# Tables à créer  (fusion des deux domaines)
# ---------------------------------------------------------------------------
from domaines.prestation_recouvrement.dtm.dtm_config import DTM_CONFIG as _PR
from domaines.grh.dtm.dtm_config                     import DTM_CONFIG as _GRH

DTM_ALL = _PR + _GRH   # liste complète des entrées DTM


# ---------------------------------------------------------------------------
# Helpers DDL
# ---------------------------------------------------------------------------

def _ora_col_def(col_name: str, data_type: str, data_length: int,
                 data_precision, data_scale) -> str:
    """
    Reconstruit une définition de colonne Oracle depuis ALL_TAB_COLUMNS.

    Les tables ODS sont des tables de staging/archive :
    AUCUNE contrainte n'est reprise (ni NOT NULL, ni PK, ni FK, ni UNIQUE).
    Toutes les colonnes sont nullable pour permettre l'insertion sans blocage.
    """
    if data_type == "NUMBER":
        if data_precision and data_scale is not None:
            type_str = f"NUMBER({data_precision},{data_scale})"
        elif data_precision:
            type_str = f"NUMBER({data_precision})"
        else:
            type_str = "NUMBER"
    elif data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
        type_str = f"{data_type}({data_length} CHAR)"
    elif data_type == "DATE":
        type_str = "DATE"
    elif data_type.startswith("TIMESTAMP"):
        type_str = "TIMESTAMP"
    elif data_type == "CLOB":
        type_str = "CLOB"
    else:
        type_str = data_type

    # Pas de NOT NULL — table ODS sans contraintes
    return f"    {col_name:<35} {type_str}"


def _build_ddl(ods_schema: str, dwh_schema: str, table_name: str,
               columns: list, dwh_user: str) -> list[str]:
    """
    Retourne la liste des instructions DDL SQL pour créer la table ODS miroir.
    columns : liste de tuples (col_name, data_type, data_length,
                               data_precision, data_scale, nullable)

    Règles ODS (table de staging/archive) :
      - Aucune contrainte reprise (ni NOT NULL, ni PK, ni FK, ni UNIQUE, ni CHECK).
      - Toutes les colonnes sont nullable.
      - Un seul index sur CLICHE pour les purges du pipeline.
    """
    ods_full  = f"{ods_schema}.{table_name}"
    dwh_full  = f"{dwh_schema}.{table_name}"

    # On passe seulement les 5 premiers éléments (on ignore 'nullable')
    col_defs  = [_ora_col_def(c[0], c[1], c[2], c[3], c[4]) for c in columns]
    # Nom court pour l'index (max 30 car. Oracle 12c)
    idx_short = table_name.replace("DTM_", "")[:22]

    stmts = []

    # 1. CREATE TABLE ODS — sans aucune contrainte
    stmts.append(
        f"-- Miroir ODS de {dwh_full} (sans contraintes — table de staging/archive)\n"
        f"CREATE TABLE {ods_full}\n"
        f"(\n"
        + ",\n".join(col_defs) + "\n"
        f");"
    )

    # 2. Index sur CLICHE uniquement (purge par cliché dans le pipeline)
    col_names = [c[0] for c in columns]
    if "CLICHE" in col_names:
        idx_name = f"IDX_ODS_{idx_short}_CLICHE"[:30]
        stmts.append(
            f"CREATE INDEX {ods_schema}.{idx_name} ON {ods_full} (CLICHE);"
        )
    else:
        stmts.append(
            f"-- ATTENTION : colonne CLICHE absente de {dwh_full} — index non créé"
        )

    # 3. Grant au user DWH
    stmts.append(
        f"GRANT SELECT, INSERT, DELETE ON {ods_full} TO {dwh_user};"
    )

    return stmts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_logging()
    logger = logging.getLogger("cnss_etl.create_ods_dtm_mirrors")

    parser = argparse.ArgumentParser(
        description="Crée les tables miroirs ODS pour toutes les tables DTM"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Affiche les DDL sans les exécuter"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Recrée les tables même si elles existent déjà (DROP IF EXISTS + CREATE)"
    )
    parser.add_argument(
        "--dtm", nargs="*", metavar="NAME",
        help="Noms courts des tables DTM à traiter (défaut : toutes)"
    )
    args = parser.parse_args()

    from shared.configs import settings
    from shared.utils.db_utils import get_dw_connection

    ods_schema = settings.ODS_SCHEMA
    if not ods_schema:
        logger.critical("ODS_SCHEMA non configuré — définissez la variable d'environnement ODS_SCHEMA.")
        sys.exit(1)

    # User DWH (celui qui a besoin du GRANT)
    dwh_user = settings.DW_DB["user"].upper()

    # Filtrage optionnel
    configs = DTM_ALL
    if args.dtm:
        configs = [c for c in configs if c["name"] in args.dtm]
        if not configs:
            logger.error(f"Aucune table DTM trouvée pour : {args.dtm}")
            sys.exit(1)

    conn = get_dw_connection()

    try:
        created = 0
        skipped = 0
        errors  = 0

        for cfg in configs:
            target      = cfg["target"]           # ex. "DTM.DTM_COTISATIONS"
            parts       = target.split(".", 1)
            dwh_schema  = parts[0] if len(parts) == 2 else dwh_user
            table_name  = parts[1] if len(parts) == 2 else parts[0]

            # --- Lecture de la structure DWH ---
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
                logger.error(
                    f"[{target}] Table introuvable dans ALL_TAB_COLUMNS "
                    f"(schéma={dwh_schema}, table={table_name}). Ignorée."
                )
                errors += 1
                continue

            # --- Vérification existence ODS ---
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM all_tables
                    WHERE owner = :1 AND table_name = :2
                    """,
                    [ods_schema.upper(), table_name.upper()],
                )
                ods_exists = cursor.fetchone()[0] > 0

            if ods_exists and not args.force:
                logger.info(f"[{ods_schema}.{table_name}] Déjà existante — ignorée (--force pour recréer)")
                skipped += 1
                continue

            # --- Génération DDL ---
            stmts = _build_ddl(ods_schema, dwh_schema, table_name, columns, dwh_user)

            if args.dry_run:
                print(f"\n{'='*70}")
                print(f"-- {cfg['name']}  →  {ods_schema}.{table_name}")
                print(f"{'='*70}")
                for s in stmts:
                    print(s)
                created += 1
                continue

            # --- Exécution ---
            if ods_exists and args.force:
                logger.warning(f"[{ods_schema}.{table_name}] DROP + recréation (--force)")
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE {ods_schema}.{table_name} CASCADE CONSTRAINTS")
                conn.commit()

            try:
                with conn.cursor() as cursor:
                    for stmt in stmts:
                        if stmt.startswith("--"):
                            logger.warning(stmt)
                            continue
                        logger.debug(f"Exécution : {stmt[:80]}...")
                        cursor.execute(stmt)
                        # DDL Oracle auto-commite, mais on commit explicitement
                        # après les DML (GRANT)
                conn.commit()
                logger.info(f"[{ods_schema}.{table_name}] ✓ Créée ({len(columns)} colonnes)")
                created += 1
            except Exception as e:
                logger.error(f"[{ods_schema}.{table_name}] Erreur : {e}")
                errors += 1

        # --- Résumé ---
        mode = "DRY-RUN" if args.dry_run else "EXÉCUTÉ"
        logger.info(
            f"\n{'='*50}\n"
            f"  Mode    : {mode}\n"
            f"  Créées  : {created}\n"
            f"  Ignorées: {skipped}\n"
            f"  Erreurs : {errors}\n"
            f"{'='*50}"
        )
        sys.exit(0 if errors == 0 else 1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
