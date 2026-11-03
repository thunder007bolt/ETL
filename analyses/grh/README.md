# Analyses de données — domaine GRH

Ce dossier garde l'historique des requêtes de profilage lancées sur la base source GRH (schéma `USER_GRH`). Elles servent à vérifier les tableaux CIPRES du domaine 4 (T58 à T65). Chaque requête indique son objectif, le tableau visé et le résultat obtenu.

Rapport de synthèse : [Analyse des mouvements du personnel](https://claude.ai/artifact/RagXh2KPVP1GrpPd6hRDeC) (doc privé, à partager depuis son menu Partager).

Plan de travail, sur le modèle des prestations (contrôle par l'annuaire de la CNSS, règles, requêtes, fiches de validation, schéma des règles, fiches de maîtrise) : [TODO.md](TODO.md).

Fiches de validation (Excalidraw), une par famille de tableaux, sur le gabarit des prestations : [T58 effectif au 31/12](<../../excalidraw/schemas/grh/tableaux cipres/T58_effectif_31_12.excalidraw>) (pilote de T58, T59, T61, T65), [T60 mouvement du personnel](<../../excalidraw/schemas/grh/tableaux cipres/T60_mouvement_personnel.excalidraw>), [T62 personnel formé](<../../excalidraw/schemas/grh/tableaux cipres/T62_personnel_forme.excalidraw>). T63 et T64 attendent les totaux du grand livre (lot 22) : la paie manque, la comptabilité est là.

Schéma des règles (Excalidraw) : [regles_grh](../../excalidraw/schemas/grh/regles_grh.excalidraw), à régénérer à chaque nouvelle règle (`python3 excalidraw/sources/grh/regles_grh.py --force`, après avoir vérifié qu'il n'a pas été retouché dans la visionneuse).

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
| 13 | [lot13_temporaires_et_fonction_publique.sql](lot13_temporaires_et_fonction_publique.sql) | 24/10/2026 | Agents temporaires et agents de la fonction publique : codes, effectifs, contrats, engagements par année (Q85–Q89) | Exécuté : 846 temporaires dont 6 actifs, renouvellements fréquents ; fonction publique entrée par détachement ou affectation |
| 14 | [lot14_types_agents.sql](lot14_types_agents.sql) | 25/10/2026 | Requête de présentation : nombre d'agents par type (CNSS permanent, CNSS temporaire, fonction publique, affecté), en activité et au total (Q90) | À exécuter |
| 15 | [lot15_test_type_agent.sql](lot15_test_type_agent.sql) | 25/10/2026 | Test de l'axe type d'agent ajouté à l'effectif et à la formation : totaux inchangés, détail par type (Q91–Q94) | À exécuter, Q94 après chargement |
| 16 | [lot16_domaines_activite.sql](lot16_domaines_activite.sql) | 26/10/2026 | Domaines d'activité : libellés des codes et lien avec la situation et l'unité (Q95–Q96) | À exécuter |
| 17 | [lot17_comparaison_annuaire_cnss_2024.sql](lot17_comparaison_annuaire_cnss_2024.sql) | 01/11/2026 | Le personnel au 31/12/2024 face à l'annuaire de la CNSS (tableaux 119 à 124) : par type d'agent et catégorie (Q97), permanents par âge et sexe (Q98, T58), mouvements de 2020 à 2024 (Q99, T60), ancienneté (Q100, T61), formés en 2024 (Q101, T62), taux d'encadrement de 2020 à 2024 (Q102, T65). Les chiffres publiés sont dans l'en-tête | Exécuté le 01/11/2026 (Q99 réécrite après ORA-00904, puis relancée) : 742 permanents pour 758, âges, ancienneté, recrutements, retraites et démissions retrouvés ; catégories, contractuels, formés, décès et disponibilités à expliquer (lot 18) |
| 18 | [lot18_ecarts_annuaire_cnss_2024.sql](lot18_ecarts_annuaire_cnss_2024.sql) | 01/11/2026 | Expliquer les écarts du lot 17 : champ de catégorie de la CNSS (Q103, Q104), les 84 engagés de 2019 (Q105), agents non comptés en 2024 et contractuels (Q106), date d'état de la fiche pour dater les départs (Q107), formés de 2024 en agents ou en inscriptions (Q108), mouvements ligne à ligne comme le tableau 121 (Q109) | Exécuté le 01/11/2026 : la catégorie de la CNSS est GRH_SITUATION.CATEGORIE ; les 84 engagés de 2019 confirmés ; la date d'état ne date pas les départs ; disponibilités et décès absents des statuts de situation (Q109) |
| 19 | [lot19_categorie_cnss.sql](lot19_categorie_cnss.sql) | 01/11/2026 | La catégorie de la CNSS : libellés (Q110), T58 (Q111), T65 de 2020 à 2024 (Q112), T62 (Q113) ; les 46 contractuels (Q114) et les 16 permanents manquants (Q115) ; retraites mois par mois (Q116) | Exécuté le 01/11/2026 (Q110 réécrite après ORA-01722, puis relancée) : T58 et T65 retrouvés avec la catégorie de la situation ; contractuels = engagés de l'année ; T62 ne retombe pas ; retraites gardées à la date de la décision 02 (Q116) |
| 20 | [lot20_mouvements_t60.sql](lot20_mouvements_t60.sql) | 01/11/2026 | Mouvements de T60 que les sorties ne donnent pas : décisions 07, 18, 14, 37 et statuts R, X, Y des situations, de 2019 à 2024 (Q117) | Exécuté le 01/11/2026 : disponibilités (07) et remises en activité (18) retrouvées ; statuts R, X, Y presque jamais saisis |
| 21 | [lot21_classement_formation.sql](lot21_classement_formation.sql) | 02/11/2026 | Le classement avant et après une formation (T62, avec ou sans promotion) : type des champs (Q118), valeurs de CLA_AVANT et CLA_APRES (Q119), promotions lues dans la situation et les décisions 60 et 61 de 2019 à 2025 (Q120) | Q119 exécutée le 02/11/2026 : CLA_AVANT (catégorie, échelle, échelon, ex. 1C04BB12) rempli partout ; CLA_APRES rempli 10 fois en 45 ans, formations diplômantes, montées de catégorie. Q118 et Q120 exécutées le 02/11/2026 : CLA_AVANT = catégorie de la situation (4 345 sur 4 356) ; aucune décision 60 ou 61 ; montées de catégorie après formation seulement depuis 2023 (5 à 6 % des formés, en perfectionnement : la carrière, pas la formation) ; 2024 : 2 promotions liées à une formation diplômante pour 6 publiées |
| 22 | [lot22_grand_livre_t63_t64.sql](lot22_grand_livre_t63_t64.sql) | 02/11/2026 | Le grand livre pour T63 et T64, partie A : tables de STG_GL (Q121), leurs colonnes avec statistiques et commentaires (Q122), comptabilité et budget dans les autres schémas (Q123). Dictionnaire Oracle seulement, aucune écriture lue. La partie B (totaux 2024 par compte, gestion G ou 7) sera écrite d'après ces résultats | À exécuter |

## Requêtes DTM corrigées (22/10/2026, commit 1b44a0c sur la branche fix/grh-requetes-dtm)

Quatre requêtes de `domaines/grh/dtm/sql` ont été corrigées, avec les mêmes colonnes en sortie pour ne rien casser dans OBIEE. L'en-tête de chaque fichier ne décrit que ses règles. Le détail des corrections, avec les références E1 à E18, est dans le rapport et dans le tableau ci-dessous. Les règles de gestion restent à faire viser par la DRH avant la mise en production.

Les requêtes ont été testées sur la base le 22/10/2026 (lot 12) et donnent les résultats attendus. DTM_INDICATEURS_FINANCES_GRH ne pourra être contrôlée qu'après un premier chargement, car elle lit DTM_GRH_EFFECTIF.

| Fichier | Tableaux | Corrections |
| --- | --- | --- |
| grh_effectif.sql | T58, T59, T61 | Agents présents au 31/12 (actifs, ou sortis après cette date) ; dernière situation commencée avant le 31/12 ; actifs hors positions d'absence ; tranches en « <= » ; exercices clos (E1 à E5) |
| grh_ratio.sql | T65 | Même règle de présence que l'effectif ; exercices clos (E1, E2, E5) |
| indicateurs_finances_grh.sql | T65 | Lecture de l'effectif du cliché chargé, plus de tri de texte (E11) |
| grh_formation.sql | T62, T64 | Plus de filtre de statut ; frais vides comptés à zéro (E12, E13) ; budget laissé vide, car le champ BUDGET contient une année budgétaire (lot 11, Q78, Q79) |

Ne sont pas corrigés : T60 (grh_mouvement.sql), qui attend les réponses de la DRH ; la définition de l'encadrant (E10) ; les unités de durée (E15) ; la lecture directe des inscriptions (E17) ; T63 et T64 côté finances (E18).

## Requêtes proposées (à valider)

Écrites le 01/11/2026 d'après les règles G1 à G11, dans la forme des maquettes de l'Annuaire, sur les faits du DWH (`:1` = cliché, saisi sans guillemets ; `:2` = exercice). Les questions en attente sont des réglages en tête de requête (`parametres`) : `categorie_05` (QG1), `duree_contrat` (QG5), `disponibilite` (QG6). L'en-tête de chaque requête donne le contrôle attendu sur l'exercice 2024. Elles remplaceront les requêtes DTM après le visa de la DRH, sans changer leurs colonnes.

| Tableaux | Fichier | État |
| --- | --- | --- |
| T58, T59, T61, T65 | [t58_t65_effectif_requete_proposee.sql](t58_t65_effectif_requete_proposee.sql) | Une requête pour l'effectif au 31/12 : T58 par tranche d'âge (permanents, puis contractuels), T59 par domaine, T61 par ancienneté, T65 sur cinq ans ; colonnes : les quatre catégories par sexe, hommes, femmes, total, cadres, taux d'encadrement. Règles G1 à G7 ; domaines de T59 en proposition (lot 16 à exécuter). Pas encore exécutée |
| T60 | [t60_requete_proposee.sql](t60_requete_proposee.sql) | Le mouvement des permanents sur cinq ans : début, recrutements, autres entrées, retraites, décès, autres sorties, fin, puis le détail comme le tableau 121. Entrées et sorties par différence d'une fin d'année à l'autre (début + entrées − sorties = fin) ; motif par la première sortie (G8, G9). Remplace grh_mouvement.sql. Pas encore exécutée |
| T62 | [t62_requete_proposee.sql](t62_requete_proposee.sql) | Les permanents formés dans l'exercice (formation commencée dans l'année, Kaya 2017), par catégorie : avec et sans promotion, par sexe ; inscriptions, perfectionnement, diplômante (G10). Ne retombera pas sur l'annuaire (QG4). Pas encore exécutée |
| T63, T64 | — | La paie n'est pas dans la copie, mais le grand livre (STG_GL) y est. Comptes de la CNSS (grille « Evaluation des Ratios CIPRES 2025 ») : masse salariale 651 et 652, formation 63812, 6386 et 6577, charges de fonctionnement 61 à 68, gestion G ou 7. Lot 22 en cours |

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
- Face à l'annuaire de la CNSS 2024 (lot 17) : 742 permanents au 31/12/2024 pour 758 publiés. Les tranches d'âge (en années pleines au 31/12) et d'ancienneté (en années pleines depuis l'embauche) sont retrouvées à quelques unités près : ces deux règles sont celles de la CNSS (Q98, Q100).
- La catégorie de la CNSS est celle de la situation (GRH_SITUATION.CATEGORIE, la catégorie de la grille de salaire), pas le niveau de la qualification : 01 et 02 employés (173 pour 177), 03 agents de maîtrise (207 pour 208), 04 et 05 cadres moyens (304 pour 311), 06 cadres supérieurs (58 pour 62) ; 48,8 % de cadres pour 49 % (Q103). L'atelier de Kaya (2017) rangeait 05 en cadres supérieurs : question à la DRH, réglée par un paramètre. Avec elle, T58 retombe à 4 agents près par catégorie et par sexe, et T65 à 0,3 point près de 2020 à 2024 en comptant les 84 engagés de 2019 comme l'annuaire (lot 19, Q111, Q112).
- Les 84 engagés de 2019 forment un seul lot : engagement non annulé, fiche inactive, aucune sortie datée, aucun nouvel engagement en 2024. La base ne dit ni quand ni pourquoi ils sont partis (Q105). Cela ne touche que les années 2019 à 2023.
- Les 46 contractuels de l'annuaire sont les temporaires sous contrat au 31/12 : 44 engagés (décision 50) en 2024 et 2 au second semestre 2023, sans sortie avant 2025. Pour un temporaire, une fin de contrat (35) n'est pas une sortie si un nouveau contrat suit (lot 19, Q114).
- T62 ne retombe pas sur l'annuaire (480 formés) : 409 agents permanents, 524 inscriptions, et deux fois plus de cadres moyens formés que l'annuaire. L'annuaire vient sans doute d'un autre registre : question à la DRH (lot 19, Q113).
- Les 16 permanents qui manquent au 31/12/2024 (2 %) ne forment aucun groupe : l'idée d'une retraite de décembre comptée l'année suivante ne tient pas (lot 19, Q115, Q116). Écart accepté ; la retraite reste à la date de la décision 02.
- Décès : la liquidation des droits (34) donne 19 sorties sur 2020-2024 pour 17 décès publiés, mais pas année par année. Les disponibilités et les remises en activité ne passent pas par le statut des situations : le lot 20 compte les décisions 07 et 18 et les statuts R, X, Y (lot 18, Q109).
- La date d'état de la fiche (DATE_ETAT_EMPLOYE) ne date pas les départs : remplie jusqu'en 2020 seulement, à moins d'un mois de la sortie une fois sur quatre (Q107).
- De 2020 à 2023, la base compte chaque année 92 à 99 permanents de moins que l'annuaire, contre 16 en 2024. Ce sont les 84 engagés de 2019 : la base les compte en recrutements en 2019 mais jamais présents, car leur fiche est inactive sans sortie datée ; l'annuaire les compte de 2019 à 2023 et les sort en 2024 en « autres sorties ». Sans eux, l'écart tombe à 8 à 16 agents par an (Q99, Q102, lot 13 Q88).
- Mouvements (T60) : les 104 recrutements de 2024 sont retrouvés exactement, les retraites (décision 02) à 4 près chaque année, les démissions (36) exactement, les licenciements (54) à 1 près. Les décès n'ont plus de date depuis 2010 : ils se lisent dans la liquidation des droits (34). L'annuaire compte aussi les disponibilités en sorties et les remises en activité en entrées (Q99 ; lot 18, Q109).
- 7 temporaires actifs pour 46 contractuels publiés ; 410 agents formés en 2024 (525 inscriptions) pour 480 publiés (Q97, Q101).
- Disponibilités et remises en activité : les décisions 07 et 18 retombent sur l'annuaire à 1 à 3 près par an (18 pour 21, 22 pour 19 sur 2020-2024). Les statuts de fin de carrière (R, X, Y) ne sont presque pas saisis ; 10 des 84 engagés de 2019 sont radiés (X) dès 2019, les 74 autres sans trace de départ (lot 20, Q117).

## Règles proposées (lots 17 à 20)

Elles sont prouvées par l'annuaire de la CNSS 2024 (partie IX, tableaux 119 à 124), sauf mention « proposition BI ». Elles sont à faire viser par la DRH avant d'écrire les requêtes proposées. Une question en attente ne bloque rien : elle devient un réglage de la requête.

| N° | Sujet | Règle proposée | Preuve | Tableaux |
| --- | --- | --- | --- | --- |
| G1 | Personnel permanent | Agent CNSS (qualité A) dont la dernière situation commencée au 31/12 est de nature PERMANENT. Les agents de la fonction publique (B) et affectés (C) n'en font pas partie | 742 pour 758 publiés au 31/12/2024 (Q97) | T58, T59, T61, T65 |
| G2 | Présent au 31/12 | Entré au plus tard le 31/12 (embauche, à défaut prise de service), et fiche ACTIVE ou première sortie après le 31/12. Sortie : première décision 02, 34, 35, 36, 52 ou 54 non annulée par une décision 40, à défaut la date de décès | Effectif 2024 à 2 % ; de 2020 à 2023 à 1 à 2 % une fois les 84 engagés de 2019 remis (Q99, Q102, Q115). L'écart restant (16 agents en 2024) ne forme aucun groupe : accepté | T58 à T61, T65 |
| G3 | Catégorie | La catégorie de la dernière situation au 31/12 (GRH_SITUATION.CATEGORIE, grille de la CNSS) : 01 et 02 employés, 03 agents de maîtrise, 04 et 05 cadres moyens, 06 cadres supérieurs. Plus le niveau de la qualification, que lisaient les requêtes DTM | Chaque case catégorie et sexe à 4 agents près (Q103, Q111). Kaya (2017) rangeait 05 en cadres supérieurs : réglage categorie_05 (cadres moyens aujourd'hui) | T58, T62, T65 |
| G4 | Âge | Années pleines au 31/12 ; tranches de l'Annuaire : moins de 20, 20-24… 55-59, 60 et plus. Attention : les prestations comptent l'année moins l'année de naissance (R8), pas la GRH | Identique jusqu'à 34 ans, à 2 près jusqu'à 54 ans (Q98) | T58 |
| G5 | Ancienneté | Années pleines au 31/12 depuis l'embauche, à défaut la prise de service ; tranches moins de 1, 1-4, 5-9… 35-39, 40 et plus | Chaque tranche à 8 près, la plupart à 2 près (Q100) | T61 |
| G6 | Taux d'encadrement | Cadres (catégories 04, 05, 06) sur les permanents présents au 31/12 | 48,8 % pour 49,2 % en 2024 ; à 0,3 point près de 2020 à 2023 avec les 84 engagés de 2019 (Q112) | T65 |
| G7 | Contractuels | Agent CNSS dont la dernière situation au 31/12 est de nature TEMPORAIRE, avec un engagement temporaire (décision 50) non annulé dans l'année ou au second semestre de l'année d'avant, et sans sortie au 31/12. Une fin de contrat (35) n'est pas une sortie si un nouveau contrat suit | 46 pour 46 publiés ; par catégorie, les employés seulement (Q114). Durée du contrat absente de la base : proposition BI, réglage duree_contrat (18 mois) | T58 (partie temporaires, Kaya 2017) |
| G8 | Entrées (T60) | Recrutement : entrée (embauche) dans l'année d'un agent permanent au 31/12. Remise en activité : décision 18 | Recrutements 104 pour 104 en 2024 (Q99) ; remises à 1 à 3 près par an (Q117) | T60 |
| G9 | Sorties (T60) | Première sortie de l'agent dans l'année : retraite = décision 02 (à sa date de vigueur) ; démission = 36 ; licenciement = 54 ; décès = date de décès, à défaut liquidation des droits (34) ; autres = 35, 52. Mise en disponibilité : décision 07 | Retraites à 4 près par an, démissions exactes, licenciements à 1 près (Q99, Q109, Q116) ; disponibilités à 1 à 3 près (Q117). Décès : 19 pour 17 sur 2020-2024, la liquidation venant environ 8 mois après le décès (proposition BI) | T60 |
| G10 | Formés | Agents distincts inscrits à une formation commencée dans l'année (Kaya 2017), par catégorie (G3) et sexe ; finalité : catégorie de la formation (PERF perfectionnement, DIPLO diplômante) ; « avec promotion » : classement changé après la formation | Ne retombe pas : 409 agents permanents et 524 inscriptions pour 480 publiés, deux fois plus de cadres moyens formés (Q101, Q108, Q113). Proposition BI | T62 |
| G11 | Années passées | Les 84 agents engagés en 2019 (décision 51), inactifs sans sortie datée, ne sont jamais comptés présents par la base ; l'annuaire les compte de 2019 à 2023 et les sort en 2024 | Q99, Q105, Q117. Sans effet sur les exercices calculés à partir de 2025 | T58 à T61, T65 (2019 à 2023) |

## Questions à la DRH (non bloquantes)

Chacune a un réglage ou une valeur par défaut dans les requêtes ; la réponse change le réglage, pas le travail.

| N° | Question | En attendant |
| --- | --- | --- |
| QG1 | La catégorie 05 est-elle « cadre moyen » (annuaire 2024) ou « cadre supérieur » (Kaya 2017, liste des qualifications) ? | Cadre moyen, comme l'annuaire (réglage categorie_05) |
| QG2 | Que sont devenus les 84 agents engagés en 2019, et à quelle date sont-ils partis ? | Années 2019 à 2023 laissées telles que la base les donne (G11) |
| QG3 | Les décès ne sont plus datés depuis 2010 : peut-on saisir la date de décès (ou celle du communiqué nécrologique, Kaya 2017) ? | Liquidation des droits (34) |
| QG4 | D'où viennent les 480 formés de 2024 de l'annuaire (tableau 123) ? | Agents distincts de la base (G10) |
| QG5 | Quelle est la durée d'un contrat temporaire ? | 18 mois après le dernier engagement (G7) |
| QG6 | Un agent en disponibilité au 31/12 fait-il partie de l'effectif ? L'annuaire le sort (tableau 121) | Compté, comme aujourd'hui (réglage disponibilite = PRESENT ; HORS_EFFECTIF fait comme l'annuaire) |
| QG7 | Où sont saisis les détachements et les fins de détachement (2 et 1 publiés en 2023 et 2024) ? | Non comptés |
