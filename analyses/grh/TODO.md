# À faire — domaine GRH

Plan du 01/11/2026 : refaire pour la GRH (T58 à T65) ce qui a été fait pour les prestations : contrôler les règles face à l'annuaire de la CNSS, écrire les règles, les requêtes proposées, une fiche de validation par famille de tableaux, le schéma des règles et les fiches de maîtrise.

## Objectifs

- Des règles de calcul prouvées par les données et, quand c'est possible, retrouvées dans l'annuaire de la CNSS.
- Une requête par famille de tableaux, prête pour le DWH, et une fiche Excalidraw pour la faire valider par la DRH.
- Tout le domaine révisable dans les fiches de maîtrise (dossier « 4 gestion administrative »).

## Ce qui est déjà fait (du 21 au 26/10/2026)

- 16 lots de profilage (Q01 à Q96) sur USER_GRH : voir le [README](README.md).
- Quatre requêtes DTM corrigées et testées (effectif, ratio, indicateurs, formation : 885 agents en 2025), branche fix/grh-requetes-dtm ; visa de la DRH attendu.
- Rapport des mouvements du personnel, document T60 (22 questions), questionnaire CIPRES pour la DRH (33 questions) : liens dans le README et le journal.

## Le contrôle par l'annuaire de la CNSS 2024

L'annuaire de la CNSS 2024 (partie IX, Gestion administrative, p. 89 à 92) publie presque tous les tableaux CIPRES du domaine, pour le personnel permanent au 31/12/2024 :

| Annuaire CNSS 2024 | Tableau CIPRES | Chiffres publiés |
| --- | --- | --- |
| Tableau 119 : personnel par qualification et statut | T58 (périmètre) | 758 permanents (employés 177, agents de maîtrise 208, cadres moyens 311, cadres supérieurs 62), 46 contractuels, 804 en tout |
| Tableau 120 : permanents par catégorie, âge et sexe | T58 | 758 ; hommes 412, femmes 346 |
| Tableau 121 : mouvement du personnel permanent, 2015 à 2024 | T60 | 2024 : 767 au début, 104 recrutements, 23 retraites, 3 décès, 84 « autres » sorties, 758 à la fin |
| Tableau 122 : permanents par ancienneté et sexe | T61 | 758 ; moins d'un an 103 |
| Tableau 123 : permanents formés en 2024, par catégorie et finalité | T62 | 480, dont 6 avec promotion |
| Tableau 124 : taux d'encadrement, 2020 à 2024 | T65 | 41 % de 2020 à 2023, 49 % en 2024 (373 cadres sur 758) |

Le lot 17 reconstitue ces tableaux depuis la base, avec les règles des requêtes DTM corrigées.

## Ordre d'attaque des familles

| Ordre | Famille | Tableaux | Déjà prêt | Reste à faire |
| --- | --- | --- | --- | --- |
| 1 | Effectif au 31/12 | T58, T59, T61, T65 | grh_effectif.sql et grh_ratio.sql corrigés ; T58, T61 et T65 publiés par la CNSS | Lot 17 fait (742 pour 758 ; âge et ancienneté retrouvés) ; catégorie trouvée : GRH_SITUATION.CATEGORIE (lot 18, Q103) ; T58 et T65 retrouvés avec elle (lot 19) ; contractuels = engagés (50) de l'année ; requête sur les permanents ; fiche pilote T58 |
| 2 | Mouvements | T60 | Rapport et document T60 ; 22 questions à la DRH ; T60 publié par la CNSS (tableau 121) | Recrutements, retraites (02), démissions, licenciements retrouvés (Q99, Q116) ; les 84 engagés de 2019 expliquent l'écart des années passées ; décès, disponibilités, remises en activité et détachements par les décisions 07, 18, 37 et les statuts X, Y (lot 20, Q117) ; réécrire grh_mouvement.sql ; fiche T60 |
| 3 | Formation | T62 (et la dépense de T64) | grh_formation.sql corrigé ; T62 publié par la CNSS (tableau 123) | Lot 17 fait (410 agents pour 480) ; ni agents (409) ni inscriptions (524) ne retombent (lot 18, Q108) ; ni par la catégorie (lot 19, Q113) : question à la DRH ; proposition : agents distincts, date de début ; fiche T62 |
| 4 | Ratios financiers | T63, T64 | Pistes comptables du lot 9 | La paie manque, mais le grand livre (STG_GL) est dans la copie. Comptes de la CNSS : 651 et 652 (masse salariale), 63812, 6386 et 6577 (formation), 61 à 68 (charges), gestion G ou 7. Lot 22 (structure de STG_GL) à exécuter, puis totaux 2024 |

## Les tâches

| N° | Tâche | Qui | État |
| --- | --- | --- | --- |
| 1 | Lot 17 : contrôle face à l'annuaire de la CNSS 2024 (Q97 à Q102) | toi (exécution), puis Claude | exécuté et reporté le 01/11/2026 (Q99 réécrite après l'erreur ORA-00904, puis relancée) |
| 1 b | Lot 18 : expliquer les écarts (catégorie, 84 engagés de 2019, agents non comptés et contractuels, date d'état des fiches, formés de 2024, décès et disponibilités) (Q103 à Q109) | toi (exécution), puis Claude | exécuté et reporté le 01/11/2026, sauf Q109 (à recoller) |
| 1 c | Lot 19 : T58, T62, T65 avec la catégorie de la CNSS ; contractuels ; 16 permanents manquants ; retraites mois par mois (Q110 à Q116) | toi (exécution), puis Claude | exécuté et reporté le 01/11/2026, avec Q109, Q110 et Q116 relancées |
| 1 d | Lot 20 : décès, disponibilités, remises en activité, détachements et sortie des 84 engagés de 2019 (Q117) | toi (exécution), puis Claude | exécuté et reporté le 01/11/2026 |
| 2 | Lots 14 à 16 (types d'agents, axe type, domaines) | toi | rédigés les 25 et 26/10 ; à exécuter |
| 3 | Les règles GRH (G1…) au README, prouvées par les lots | Claude | fait le 01/11/2026 : G1 à G11 au README, avec leurs preuves, et 7 questions à la DRH (QG1 à QG7), chacune réglée par un paramètre en attendant |
| 4 | Requêtes ajustées (effectif, ratio, formation) et requête T60 réécrite | Claude | proposées le 01/11/2026 : t58_t65_effectif, t60, t62_requete_proposee.sql (réglages categorie_05, duree_contrat, disponibilite) ; reportées dans les requêtes DTM après le visa de la DRH |
| 4 b | Exécuter les trois requêtes proposées (cliché 102026, exercice 2024) et comparer aux contrôles attendus de leur en-tête | toi, puis Claude | à faire |
| 4 c | Effectif (T58, T59, T61, T65) : réunion du 03/11/2026 faite (termes 1 à 9) ; requête réécrite le 04/11/2026 (date d'arrêté, nature, unité, contrats, ancienneté, domaine par l'unité) ; fichier unité → domaine envoyé à la CNSS. Règles, réglages, points susceptibles de changer et les 7 tâches qui restent : [regles_effectif.md](regles_effectif.md) | Claude, toi, CNSS | en cours |
| 4 d | Mouvements (T60) : requête réécrite le 08/11/2026 sur la structure de DTM_GRH_MOUVEMENT, avec le détail des « autres » par les codes de la CNSS ; règles, chiffres attendus et questions dans [regles_t60.md](regles_t60.md). Fiche de lecture (termes de T60) et fiche T60 mises à jour le 08/11/2026. Reste : l'exécuter, puis voir les deux fiches en réunion. Formés (T62) : termes pas encore vus en réunion, requête inchangée | toi, Claude, avec la CNSS | en cours |
| 5 | Fiches de validation : T58 (pilote de T58, T59, T61, T65), T60, T62, T63-T64 | Claude | faites le 01/11/2026 dans excalidraw/schemas/grh/tableaux cipres : T58_effectif_31_12 (pilote), T60_mouvement_personnel (mise à jour le 08/11/2026 avec la requête réécrite), T62_personnel_forme ; T63-T64 attendent les totaux du grand livre (lot 22) |
| 5 b | Faire remplir les fiches T58, T60, T62 par la DRH (cases D'accord / Pas d'accord / À discuter) | toi, avec la DRH | à faire |
| 6 | Schéma des règles GRH (Excalidraw) | Claude | fait le 01/11/2026 : schemas/grh/regles_grh (annuaire face à la base, G1 à G11, QG1 à QG7, index Q97 à Q117) ; à régénérer à chaque nouvelle règle |
| 7 | Fiches de maîtrise « 4 gestion administrative » | Claude | à la fin |

## Questions en attente de réponse

Elles sont dans le questionnaire CIPRES GRH (33 questions) et le document T60 (22 questions), adressés à la DRH. Une réponse peut changer les règles : la reporter dans le README.
