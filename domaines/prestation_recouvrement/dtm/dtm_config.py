"""
Configuration des tables DTM agrégées.

Chaque entrée déclare :
  name      : identifiant court utilisé comme clé CLI (--dtm cotisations)
  target    : table cible (schéma.table)
  sql_files : liste ordonnée de { file, label }
               – file  : nom du fichier SQL dans le répertoire sql/
               – label : libellé pour les logs

Les SQL sélectionnent CLICHE directement depuis la table de fait source (format MMYYYY uniforme).
  - Chaque SQL reçoit :1 = CLICHE (MMYYYY) comme bind variable pour filtrer le snapshot.
  - DATE_CHARGEMENT est géré par DEFAULT SYSDATE sur la table cible.
"""

DTM_CONFIG = [
    # ------------------------------------------------------------------
    # Domaine Employeur
    # ------------------------------------------------------------------
    {
        "name": "employeur",
        "target": "DTM.DTM_EMPLOYEUR",
        "sql_files": [
            {"file": "employeur.sql", "label": "EMPLOYEUR"},
        ],
    },
    # ------------------------------------------------------------------
    # Domaine Recouvrement
    # ------------------------------------------------------------------
    {
        "name": "cotisations",
        "target": "DTM.DTM_COTISATIONS",
        "sql_files": [
            # {"file": "cotisations_pf.sql", "label": "PF"},
            {"file": "cotisations_at.sql", "label": "AT"},
            {"file": "cotisations_av.sql", "label": "AV"},
        ],
    },
    {
        "name": "recouvrement",
        "target": "DTM.DTM_RECOUVREMENT",
        "sql_files": [
            {"file": "recouvrement.sql", "label": "RECOUVREMENT"},
        ],
    },
    {
        "name": "controles",
        "target": "DTM.DTM_CONTROLE",
        "sql_files": [
            {"file": "controles.sql", "label": "CONTROLES"},
        ],
    },
    {
        "name": "salaires",
        "target": "DTM.DTM_SALAIRE",
        "sql_files": [
            {"file": "salaires.sql", "label": "SALAIRES"},
        ],
    },
    {
        "name": "mise_en_demeure",
        "target": "DTM.DTM_MISE_EN_DEMEURE",
        "sql_files": [
            {"file": "mise_en_demeure.sql", "label": "MED"},
        ],
    },
    #{
    #    "name": "med_employeur",
    #    "target": "DTM.DTM_MED_EMPLOYEUR",
    #    "sql_files": [
    #        {"file": "med_employeur.sql", "label": "MED_EMP"},
    #    ],
    #},
    {
        "name": "travailleur",
        "target": "DTM.DTM_TRAVAILLEUR",
        "sql_files": [
            {"file": "travailleur.sql", "label": "TRAVAILLEUR"},
        ],
    },
    {
        "name": "immatriculation_emp",
        "target": "DTM.DTM_IMM_EMPLOYEUR",
        "sql_files": [
            {"file": "immatriculation_emp.sql", "label": "EMP"},
        ],
    },
    {
        "name": "immatriculation_tr",
        "target": "DTM.DTM_IMM_TRAVAILLEUR",
        "sql_files": [
            {"file": "immatriculation_tr.sql", "label": "TR"},
        ],
    },
    # ------------------------------------------------------------------
    # Domaine Prestations
    # ------------------------------------------------------------------
    {
        "name": "prestation",
        "target": "DTM.DTM_PRESTATION",
        "sql_files": [
            {"file": "prestation.sql", "label": "PRESTATION"},
        ],
    },
    {
        "name": "prestation_indue",
        "target": "DTM.DTM_PRESTATION_INDUE",
        "sql_files": [
            {"file": "prestation_indue.sql", "label": "PRESTATION_INDUE"},
        ],
    },
    {
        "name": "accident_travail",
        "target": "DTM.DTM_ACCIDENT_TRAVAIL",
        "sql_files": [
            {"file": "accident_travail.sql", "label": "AT"},
        ],
    },
    {
        "name": "dossier",
        "target": "DTM.DTM_DOSSIER",
        "sql_files": [
            {"file": "dossier.sql", "label": "DOSSIER"},
        ],
    },
]
