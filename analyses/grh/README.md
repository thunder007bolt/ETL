# Analyses de données — domaine GRH

Ce dossier garde l'historique des requêtes de profilage lancées sur la base source GRH (schéma `USER_GRH`). Elles servent à vérifier les tableaux CIPRES du domaine 4 (T58 à T65). Chaque requête indique son objectif, le tableau visé et le résultat obtenu.

Rapport de synthèse : [Analyse des mouvements du personnel](https://claude.ai/artifact/RagXh2KPVP1GrpPd6hRDeC) (doc privé, à partager depuis son menu Partager).

## Conventions

- Un fichier par lot, numéroté dans l'ordre d'exécution. Les requêtes sont numérotées Q01, Q02… sans jamais réutiliser un numéro.
- L'en-tête de chaque requête donne l'objectif, les tableaux visés, le résultat et le constat.
- Les requêtes sont en lecture seule et ne renvoient que des comptages, sans aucune donnée nominative.
- La base est antérieure à Oracle 12c : pas de `FETCH FIRST`, on limite avec `ROWNUM`.
- Exécution en mode script (F5) dans SQL Developer.
- Une fois un lot exécuté, ses résultats sont reportés dans les en-têtes « Résultat ».

## Lots

| Lot | Fichier | Date | Objectif | État |
| --- | --- | --- | --- | --- |
| 1 | [lot1_profilage_grh_mouvement.sql](lot1_profilage_grh_mouvement.sql) | 21/10/2026 | Contenu et fiabilité de GRH_MOUVEMENT (Q01–Q07) | Exécuté |
| 2 | [lot2_codes_et_sources.sql](lot2_codes_et_sources.sql) | 21/10/2026 | Sens des codes, sources des entrées et sorties (Q08–Q12) | Exécuté |
| 3 | [lot3_validite_situations_decisions.sql](lot3_validite_situations_decisions.sql) | 21/10/2026 | Sens de VALIDE = E, date de sortie, catégories de décisions (Q13–Q15) | Exécuté |
| 4 | [lot4_profilage_decisions.sql](lot4_profilage_decisions.sql) | 21/10/2026 | Validité des décisions, lien avec l'agent, volumes par an (Q16–Q18) | Exécuté |
| 5 | [lot5_recoupements.sql](lot5_recoupements.sql) | 21/10/2026 | Recoupements entre décisions, embauches et état des agents (Q19–Q22) | Exécuté |
| 6 | [lot6_effectif_et_zones_ombre.sql](lot6_effectif_et_zones_ombre.sql) | 21/10/2026 | Effectif au 31/12, sorties et zones d'ombre (Q23–Q39) | Exécuté (Q38 remplacée par Q46 et Q48 du lot 8) |
| 7 | [lot7_dates_de_sortie_et_effectif_reel.sql](lot7_dates_de_sortie_et_effectif_reel.sql) | 21/10/2026 | Dater les sorties, encadrer l'effectif réel (Q40–Q45) | Exécuté |
| 8 | [lot8_effectif_detail_formation_finances.sql](lot8_effectif_detail_formation_finances.sql) | 21/10/2026 | Détail de l'effectif, formation, frais de personnel, indicateurs publiés, défaut des tranches (Q46–Q61) | Exécuté (Q51, Q52, Q54, Q57 reprises au lot 9) |
| 9 | [lot9_formation_et_comptabilite.sql](lot9_formation_et_comptabilite.sql) | 21/10/2026 | Formation sans filtre de statut, piste comptable pour T63 et T64 (Q62–Q69) | Exécuté, sauf Q62 et Q63 (résultats à recoller) |
| 10 | [lot10_test_requetes_dtm_corrigees.sql](lot10_test_requetes_dtm_corrigees.sql) | 22/10/2026 | Test des requêtes DTM corrigées avant chargement (Q70–Q74) | Exécuté : formation conforme ; effectif et ratio vides (lot 11) ; Q74 à refaire après chargement |
| 11 | [lot11_diagnostic_lot10.sql](lot11_diagnostic_lot10.sql) | 22/10/2026 | Clichés présents dans les faits du DWH, étapes de l'effectif, budget des formations (Q75–Q79) | Exécuté : le DWH contient bien 102026 ; BUDGET est une année, pas un montant |
| 12 | [lot12_test_requetes_dtm_cliche_en_clair.sql](lot12_test_requetes_dtm_cliche_en_clair.sql) | 22/10/2026 | Nouveau test de l'effectif, du ratio et de la formation avec le cliché écrit en clair ; comportement de SQL Developer avec :1 (Q80–Q84) | Exécuté : effectif (885 agents en 2025), ratio (41,7 %) et formation conformes ; les résultats vides du lot 10 venaient de la valeur saisie |

## Requêtes DTM corrigées (22/10/2026, commit e822039 sur la branche fix/grh-requetes-dtm)

Quatre requêtes de `domaines/grh/dtm/sql` ont été corrigées, avec les mêmes colonnes en sortie pour ne rien casser dans OBIEE. L'en-tête de chaque fichier ne décrit que ses règles. Le détail des corrections, avec les références E1 à E18, est dans le rapport et dans le tableau ci-dessous. Les règles de gestion restent à faire viser par la DRH avant la mise en production.

Les requêtes ont été testées sur la base le 22/10/2026 (lot 12) et donnent les résultats attendus. DTM_INDICATEURS_FINANCES_GRH ne pourra être contrôlée qu'après un premier chargement, car elle lit DTM_GRH_EFFECTIF.

| Fichier | Tableaux | Corrections |
| --- | --- | --- |
| grh_effectif.sql | T58, T59, T61 | Agents présents au 31/12 (actifs, ou sortis après cette date) ; dernière situation commencée avant le 31/12 ; actifs hors positions d'absence ; tranches en « <= » ; exercices clos (E1 à E5) |
| grh_ratio.sql | T65 | Même règle de présence que l'effectif ; exercices clos (E1, E2, E5) |
| indicateurs_finances_grh.sql | T65 | Lecture de l'effectif du cliché chargé, plus de tri de texte (E11) |
| grh_formation.sql | T62, T64 | Plus de filtre de statut ; frais vides comptés à zéro (E12, E13) ; budget laissé vide, car le champ BUDGET contient une année budgétaire (lot 11, Q78, Q79) |

Ne sont pas corrigés : T60 (grh_mouvement.sql), qui attend les réponses de la DRH ; la définition de l'encadrant (E10) ; les unités de durée (E15) ; la lecture directe des inscriptions (E17) ; T63 et T64 côté finances (E18).

## Tableaux CIPRES et requêtes

| Tableau | Contenu | Requêtes |
| --- | --- | --- |
| T58 | Effectif au 31/12 par catégorie, âge et sexe | Q23–Q29, Q36, Q38–Q40, Q45, Q46, Q58, Q60 |
| T59 | Effectif au 31/12 par domaine d'activité et sexe | Q23–Q29, Q37, Q39, Q45, Q47, Q58, Q60 |
| T60 | Mouvement du personnel (« T3 » dans la doc DataMart) | Q01–Q35, Q41, Q42, Q44, Q45, Q59, Q61 |
| T61 | Effectif au 31/12 par ancienneté et sexe | Q23–Q29, Q38–Q40, Q45, Q48, Q58, Q60 |
| T62 | Agents formés par catégorie, sexe et finalité | Q49, Q50, Q54, Q56, Q57, Q59, Q64, Q65, Q67–Q69 |
| T63 | Frais de personnel / charges de fonctionnement | Q55, Q61, Q66 |
| T64 | Dépenses de formation / masse salariale et budget | Q50–Q53, Q55, Q56, Q61–Q63, Q65, Q66 |
| T65 | Taux d'encadrement | Q23–Q29, Q36, Q39, Q43, Q45, Q56 |

## Constats à ce jour

- GRH_MOUVEMENT ne contient presque que des mouvements internes : aucun recrutement ni départ (Q03).
- Les recrutements se lisent dans GRH_PERSONNE (date d'embauche), et les départs dans GRH_DECISION (catégories 02, 36, 52, 54) (Q15 à Q22).
- Plusieurs codes sont trompeurs : dans les situations, REC veut dire reclassement et RET rétrogradation ; DIS veut dire mise à disposition (Q08).
- La situation d'un agent qui part n'est jamais clôturée, et son statut n'est pas toujours mis à jour (Q14, Q26). La règle de l'effectif du DTM compte donc 2 845 agents au 31/12/2025, pour 879 actifs réels (Q23). Le DTM publie 2 118 agents et 1 104 actifs, et 1 636 actifs dans le ratio (Q39).
- Environ 1 380 agents partis n'ont ni décision de sortie ni date de décès (Q24). Le lot 7 en date une partie : l'indemnité de fin de contrat (35) pour la moitié des temporaires, la liquidation des droits (34) pour les décès. Restent 817 départs sans aucune date (Q44). L'effectif des années passées n'est donc pas reconstituable : entre 621 et 1 613 agents en 2019 (Q45).
- Aucune date de décès n'est saisie après 2010 (Q42).
- Le DTM perd des agents à cause des bornes de ses tranches d'âge et d'ancienneté, dont tous les recrutés de l'année : 2 822 agents comptés, moins 704 perdus, donne exactement les 2 118 publiés (Q40, Q58).
- Sur les seuls actifs, T58, T59 et T61 se calculent sans difficulté (Q46 à Q48).
- Toutes les formations ont le statut « N » alors que le DTM ne garde que le statut « T » : T62 et T64 ne publient rien (Q50, Q56). La table GRH_CAT_FORMATION, lue par l'ETL, n'existe pas (Q49).
- La paie du personnel n'est pas dans la copie : les 22 vues de paie de USER_GRH sont invalides (Q55). T63 et T64 devront passer par la comptabilité (lot 9).
- Pour T65 en 2025, le DTM publie 31,1 % dans une table et 13,6 % dans l'autre (Q39, Q56).
- Le type de fonction R désigne les postes pourvus par nomination, pas l'encadrement. Les postes de commandement comptent 179 actifs, soit 20,4 % (Q43).
- La base consultée est une copie figée au 18/02/2026 (Q35).
- Les codes S et M de la doc et les codes HC, 04, 05, 06 du DTM désignent les mêmes cadres : 366 actifs sur 878, soit 41,7 % (Q36).
- Les alias Oracle sont limités à 30 caractères (ORA-00972 sur Q32 et Q38).
