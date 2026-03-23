"""
Configuration des tables DTM agrégées — domaine GRH.

Chaque entrée déclare :
  name      : identifiant court utilisé comme clé CLI (--dtm grh_ratio)
  target    : table cible (schéma.table)
  sql_files : liste ordonnée de { file, label }
               – file  : nom du fichier SQL dans le répertoire sql/
               – label : libellé pour les logs

Les SQL sélectionnent :1 = CLICHE (MMYYYY) comme bind variable pour filtrer
le snapshot source et étiqueter les lignes insérées.
DATE_CHARGEMENT est géré par DEFAULT SYSDATE sur la table cible.
"""

DTM_CONFIG = [
    {
        "name": "grh_ratio",
        "target": "DTM.DTM_GRH_RATIO",
        "sql_files": [
            {"file": "grh_ratio.sql", "label": "GRH_RATIO"},
        ],
    },
    {
        "name": "grh_formation",
        "target": "DTM.DTM_GRH_FORMATION",
        "sql_files": [
            {"file": "grh_formation.sql", "label": "GRH_FORMATION"},
        ],
    },
    {
        "name": "grh_mouvement",
        "target": "DTM.DTM_GRH_MOUVEMENT",
        "sql_files": [
            {"file": "grh_mouvement.sql", "label": "GRH_MOUVEMENT"},
        ],
    },
    {
        "name": "grh_effectif",
        "target": "DTM.DTM_GRH_EFFECTIF",
        "sql_files": [
            {"file": "grh_effectif.sql", "label": "GRH_EFFECTIF"},
        ],
    },
]
