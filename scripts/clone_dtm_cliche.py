"""
Duplique les données DTM d'un cliché source vers un ou plusieurs clichés cibles.

Utile pour les tests multi-clichés : copier un cliché existant sous plusieurs
labels MMYYYY sans relancer tout le pipeline ETL.

Usage :
    python scripts/clone_dtm_cliche.py --from 052026 --to 012026 022026 032026
    python scripts/clone_dtm_cliche.py --from 052026 --to 012026 --dtm cotisations recouvrement

Comportement :
  - Pour chaque table DTM × chaque cliché cible :
      1. DELETE WHERE CLICHE = :to  (idempotence)
      2. INSERT SELECT ... :to FROM table WHERE CLICHE = :from
  - Les autres clichés déjà présents dans la table sont intacts.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from shared.configs.log_setup import setup_logging
from shared.utils.db_utils import get_dw_connection

logger = logging.getLogger("cnss_etl.clone_dtm_cliche")


def _get_columns(cursor, schema: str, table_name: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name FROM all_tab_columns
        WHERE owner = :1 AND table_name = :2
        ORDER BY column_id
        """,
        [schema.upper(), table_name.upper()],
    )
    return [row[0] for row in cursor.fetchall()]


def clone_table(conn, table: str, from_cliche: str, to_cliches: list[str]) -> None:
    schema, table_name = table.split(".", 1) if "." in table else ("DTM", table)
    with conn.cursor() as cursor:
        cols = _get_columns(cursor, schema, table_name)
        if not cols:
            raise RuntimeError(f"Table {table} introuvable dans ALL_TAB_COLUMNS.")

        non_cliche_cols = [c for c in cols if c != "CLICHE"]
        select_cols     = ", ".join(non_cliche_cols)
        insert_cols     = ", ".join(non_cliche_cols + ["CLICHE"])

        for to_cliche in to_cliches:
            # Idempotence
            cursor.execute(
                f"DELETE FROM {table} WHERE CLICHE = :1", [to_cliche]
            )
            deleted = cursor.rowcount
            conn.commit()
            if deleted:
                logger.info(f"[{table}] {deleted} lignes existantes supprimées (CLICHE={to_cliche})")

            # Clone
            cursor.execute(
                f"INSERT /*+ APPEND */ INTO {table} ({insert_cols})"
                f" SELECT {select_cols}, :1 FROM {table} WHERE CLICHE = :2",
                [to_cliche, from_cliche],
            )
            inserted = cursor.rowcount
            conn.commit()
            logger.info(f"[{table}] {inserted} lignes clonées {from_cliche} → {to_cliche}")


def main():
    setup_logging()

    from domaines.prestation_recouvrement.dtm.dtm_config import DTM_CONFIG as _PR
    from domaines.grh.dtm.dtm_config import DTM_CONFIG as _GRH
    DTM_CONFIG = _PR + _GRH
    available  = [c["name"] for c in DTM_CONFIG]

    parser = argparse.ArgumentParser(description="Clone un cliché DTM vers d'autres clichés (tests)")
    parser.add_argument("--from", dest="from_cliche", required=True,
                        metavar="MMYYYY", help="Cliché source (ex. 052026)")
    parser.add_argument("--to", dest="to_cliches", required=True, nargs="+",
                        metavar="MMYYYY", help="Cliché(s) cible(s) (ex. 012026 022026)")
    parser.add_argument("--dtm", nargs="*", choices=available, metavar="DTM",
                        default=None, help="Tables DTM à cloner (défaut : toutes)")
    args = parser.parse_args()

    configs = DTM_CONFIG if args.dtm is None else [c for c in DTM_CONFIG if c["name"] in args.dtm]
    if not configs:
        logger.error("Aucune table DTM correspondante.")
        sys.exit(1)

    logger.info(
        f"Clone DTM : {args.from_cliche} → {args.to_cliches} "
        f"({len(configs)} table(s))"
    )

    conn = get_dw_connection()
    try:
        for cfg in configs:
            clone_table(conn, cfg["target"], args.from_cliche, args.to_cliches)
        logger.info("Clone terminé.")
    except Exception:
        logger.exception("Erreur lors du clone DTM")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
