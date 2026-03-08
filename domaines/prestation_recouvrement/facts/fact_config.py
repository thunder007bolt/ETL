"""
Configuration centralisée des tables de faits.

Chaque entrée déclare :
    sql_file : fichier SQL dans domains/.../facts/sql/
    target   : table cible dans le DWH

Chargement : DELETE WHERE L_ANNEE = :1 AND L_MOIS = :2 + INSERT /*+ APPEND */
L_ANNEE et L_MOIS sont injectés automatiquement par le pipeline à partir de la date du run.

Champ optionnel :
    col_map      : dict {col_source: col_cible} — renommage de colonnes
    transform_fn : callable (df: DataFrame) -> DataFrame
"""

FACT_CONFIG = [

    # ----------------------------------------------------------------
    # Stratégie "period" (défaut) :
    #   1. L_ANNEE et L_MOIS injectés automatiquement par le pipeline
    #   2. DELETE FROM table WHERE L_ANNEE = :1 AND L_MOIS = :2
    #   3. INSERT /*+ APPEND */ toutes les lignes du batch
    # ----------------------------------------------------------------

    {
        "sql_file":   "categorie_employeur.sql",
        "target":     "FAIT_CATEGORIE_EMPLOYEUR",
    },
    {
        "sql_file":   "contrainte.sql",
        "target":     "FAIT_CONTRAINTE",
    },
    {
        "sql_file":   "controle.sql",
        "target":     "FAIT_CONTROLE",
    },
    {
        "sql_file":   "declaration_nominative.sql",
        "target":     "FAIT_DECLARATION_NOMINATIVE",
    },
    {
        "sql_file":   "depot.sql",
        "target":     "FAIT_DEPOT",
    },
    {
        "sql_file":   "dossier_immatriculation.sql",
        "target":     "FAIT_DOSSIER_IMMATRICULATION",
    },
    {
        "sql_file":   "emploi.sql",
        "target":     "FAIT_EMPLOI",
    },
    {
        "sql_file":   "employeur.sql",
        "target":     "FAIT_EMPLOYEUR",
    },
    {
        "sql_file":   "individu.sql",
        "target":     "FAIT_INDIVIDU",
    },
    {
        "sql_file":   "mise_en_demeure.sql",
        "target":     "FAIT_MISE_EN_DEMEURE",
    },
    {
        "sql_file":   "notification_prest.sql",
        "target":     "FAIT_NOTIFICATION_PREST",
    },
    {
        "sql_file":   "partenaire_assurance.sql",
        "target":     "FAIT_PARTENAIRE_ASSURANCE",
    },
    {
        "sql_file":   "penalite_retard.sql",
        "target":     "FAIT_PENALITE_RETARD",
    },
    {
        "sql_file":   "prestation_esp.sql",
        "target":     "FAIT_PRESTATION_ESP",
    },
    {
        "sql_file":   "reception_dossier.sql",
        "target":     "FAIT_RECEPTION_DOSSIER",
    },
    {
        "sql_file":   "transaction_cotisation.sql",
        "target":     "FAIT_TRANSACTION_COTISATION",
    },
    {
        "sql_file":   "transaction_declaration.sql",
        "target":     "FAIT_TRANSACTION_DECLARATION",
    },
    {
        "sql_file":   "transaction_reglement.sql",
        "target":     "FAIT_TRANSACTION_REGLEMENT",
    },
    {
        "sql_file":   "travailleur.sql",
        "target":     "FAIT_TRAVAILLEUR",
    },
    {
        "sql_file":   "tsinistre.sql",
        "target":     "FAIT_TSINISTRE",
    },

]
