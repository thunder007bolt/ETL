# Suivi des modifications à faire sur le DWH — domaine GRH

Ce fichier liste les changements de structure et de déploiement à faire dans la base, dans l'ordre. Chaque ligne passe de « À faire » à « Fait », avec la date et le nom de la personne qui l'a faite.

## Modifications

| N° | Ajouté le | Modification | Où | Comment | Statut | Fait le, par |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 25/10/2026 | Ajouter les colonnes `QUALITE_EMPLOYE` et `NATURE_EMPLOI` à `DTM_GRH_EFFECTIF` et `DTM_GRH_FORMATION` | Schéma DTM | [ddl/20261025_ajout_type_agent.sql](ddl/20261025_ajout_type_agent.sql), étapes 1 et 2 | À faire | |
| 2 | 25/10/2026 | Ajouter les mêmes colonnes aux tables miroirs de l'ODS | Schéma ODS | Même script, étape 3 ; contrôle à l'étape 4 (8 lignes) | À faire | |
| 3 | 25/10/2026 | Déployer les 4 requêtes GRH de la branche `fix/grh-requetes-dtm` (effectif, ratio, indicateurs, formation) | Dépôt et Jenkins | Seulement après 1 et 2, et après le visa de la DRH | À faire | |
| 4 | 25/10/2026 | Recharger le DTM GRH du mois | Jenkins | `python run_grh_dtm_pipeline.py`, après le job des faits du même mois | À faire | |
| 5 | 25/10/2026 | Contrôler les chiffres du chargement | SQL Developer | [analyses/grh/lot15_test_type_agent.sql](../../../analyses/grh/lot15_test_type_agent.sql) | À faire | |
| 6 | 25/10/2026 | Prévenir Jules des nouvelles colonnes, pour les filtres OBIEE | OBIEE | Voir « Pour le tableau de bord » | À faire | |
| 7 | 25/10/2026 | Mettre à jour l'onglet « Texte des requêtes » du document d'exploitation | Doc Claude | Recopier les deux requêtes et ajouter les deux colonnes aux tableaux de colonnes | À faire | |
| 8 | 25/10/2026 | Remplacer les tranches d'âge de `DIM_GRH_TRANCHE_AGE` par celles de l'Annuaire CIPRES (T58) : moins de 20, 20-24 … 55-59, 60 et plus | Schéma DTM | [ddl/20261025_tranches_cipres.sql](ddl/20261025_tranches_cipres.sql), étapes 0 à 2 et 4. Commencer par l'étape 0 (structure), à faire avant l'étape 4 (rechargement) | À faire | |
| 9 | 25/10/2026 | Remplacer les tranches d'ancienneté de `DIM_GRH_TRANCHE_ANCIENNETE` par celles de l'Annuaire CIPRES (T61) : moins de 1, 1-4 … 35-39, 40 et plus | Schéma DTM | Même script, étapes 1, 3 et 4 ; avant l'étape 4 (rechargement) | À faire | |

## Pour le tableau de bord

- **`QUALITE_EMPLOYE`** : `A` agent CNSS, `B` agent de la fonction publique, `C` agent affecté.
- **`NATURE_EMPLOI`** : `PERMANENT` ou `TEMPORAIRE`. Dans `DTM_GRH_EFFECTIF`, c'est la nature de la situation au 31/12. Dans `DTM_GRH_FORMATION`, c'est la situation au 31/12 de l'exercice de la formation.
- **Sans filtre, les totaux ne changent pas** : chaque agent reste compté une fois par exercice, seulement sur une ligne plus détaillée.
- **Le filtre à appliquer dépend des réponses de la DRH** aux questions 2 et 13 du questionnaire CIPRES (temporaires et agents de la fonction publique).
- **Clichés déjà chargés** : leurs deux colonnes restent vides.
- **Taux d'encadrement** : `DTM_GRH_RATIO` et `DTM_INDICATEURS_FINANCES_GRH` n'ont pas cet axe, car ils donnent une ligne par année. Pour un taux filtré par type d'agent, OBIEE peut le calculer à partir de `DTM_GRH_EFFECTIF` (`QUAL_CODE`, `NB_AGENTS_ACTIFS`).

## Autres points en attente

- **Ligne `NOM` de `DIM_GRH_TYPE_MOUVEMENT`** : lire la ligne existante avant toute nouvelle version, pour ne pas l'écraser au MERGE.
- **Vue `V_BI_DECISION_AGENT`** : demandée à la DSI, dans la spécification des vues du domaine 4.
- **Requête des mouvements (T60)** : à réécrire après les réponses de la DRH.
- **Domaines CIPRES (T59)** : après la réponse de la DRH à la question 11, ranger les codes de domaine de la GRH dans les 7 domaines de l'Annuaire. Cela peut se faire par une colonne de regroupement dans `DIM_GRH_DOMAINE_ACTIVITE`, ou dans OBIEE.

## Tranches CIPRES

Après les modifications 8 et 9, les codes de tranche changent : `<20`, `20-24` … `55-59`, `60+` pour l'âge, et `<1`, `1-4` … `35-39`, `40+` pour l'ancienneté. Les clichés déjà chargés gardent les anciens codes (`<25`, `55+`, `30+`). Il faut prévenir Jules en même temps que pour la modification 6. Référence : [analyses/grh/reference_cipres_T58_T65.md](../../../analyses/grh/reference_cipres_T58_T65.md).
