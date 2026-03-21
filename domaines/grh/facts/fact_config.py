"""
Configuration centralisée des tables de faits — domaine GRH.

Chaque entrée déclare :
    sql_file : fichier SQL dans domaines/grh/facts/sql/
    target   : table cible dans le DWH

Chargement : DELETE WHERE CLICHE = :1 (ou archive ODS + TRUNCATE) + INSERT /*+ APPEND */
CLICHE est injecté automatiquement par le pipeline à partir de la date du run.

Champ optionnel :
    col_map      : dict {col_source: col_cible} — renommage de colonnes
    transform_fn : callable (df: DataFrame) -> DataFrame
"""

FACT_CONFIG = [

    {
        "sql_file": "grh_situation.sql",
        "target":   "FAIT_GRH_SITUATION",
    },
    {
        "sql_file": "grh_section.sql",
        "target":   "FAIT_GRH_SECTION",
    },
    {
        "sql_file": "grh_sanction.sql",
        "target":   "FAIT_GRH_SANCTION",
    },
    {
        "sql_file": "grh_poste_formations.sql",
        "target":   "FAIT_GRH_POSTE_FORMATIONS",
    },
    {
        "sql_file": "grh_personne.sql",
        "target":   "FAIT_GRH_PERSONNE",
    },
    {
        "sql_file": "grh_mouvement.sql",
        "target":   "FAIT_GRH_MOUVEMENT",
    },
    {
        "sql_file": "grh_facture.sql",
        "target":   "FAIT_GRH_FACTURE",
    },
    {
        "sql_file": "grh_demande.sql",
        "target":   "FAIT_GRH_DEMANDE",
    },
    {
        "sql_file": "grh_decision_personne.sql",
        "target":   "FAIT_GRH_DECISION_PERSONNE",
    },
    {
        "sql_file": "grh_decision_carriere.sql",
        "target":   "FAIT_GRH_DECISION_CARRIERE",
    },
    {
        "sql_file": "grh_decision.sql",
        "target":   "FAIT_GRH_DECISION",
    },
    {
        "sql_file": "grh_absence.sql",
        "target":   "FAIT_GRH_ABSENCE",
    },
    {
        "sql_file": "grh_abs_lieu.sql",
        "target":   "FAIT_GRH_ABS_LIEU",
    },

]
