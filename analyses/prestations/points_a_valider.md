# Prestations : règles et questions à valider

Mis à jour le 01/11/2026. Base consultée : copie de USER_DWH du 02/02/2026, comparée à l'annuaire statistique CNSS 2024.

Pour chaque point, ce qu'on fait aujourd'hui, puis ce qu'on demande. Tous les tableaux T25 à T57 sont repris à la fin, chacun avec ses codes et les points qui le concernent. Les chiffres et les requêtes sont dans le README du dossier. S'il n'y a pas le temps de tout voir : les points 1 à 5 d'abord.

## À trancher en priorité

1. Année d'un paiement. Aujourd'hui on rattache un paiement au mois qu'il paie (date d'effet du débours). Pourtant la date de saisie du paiement colle mieux à l'annuaire : 11 lignes sur 16, et les allocations de vieillesse à 0,02 % près. Exemple : un rappel de 15 mois versé en mars 2024 compte pour 2024, ou pour 12 mois en 2023 ?
   Réponse :

2. Date d'arrêt de l'annuaire. Les décès de retraités de 2024 que la base connaissait fin mars 2025 retombent sur le tableau 26 (444 pour 447). L'annuaire est-il arrêté vers fin mars ? Si oui, on calcule l'année N sur la base de fin mars N+1.
   Réponse :

3. Pensionné au 31/12. On compte toute pension en cours au 31/12 (ni radiée ni suspendue, titulaire en vie), payée ou non en fin d'année. C'est ce que fait l'annuaire : 31 093 chez nous pour 30 808 publiés en 2024. Si on exige un paiement, on perd 11 % (coordination payée ailleurs, certificats de vie en attente).
   Réponse :

4. Nouveau pensionné. C'est la pension saisie dans l'année : 2 463 en 2024, 2 489 publiés. À la notification on tomberait à 2 397. Une révision ou une reliquidation ne fait pas un nouveau pensionné.
   Réponse :

5. Réversion. Quand un retraité décède, la pension de sa veuve et de ses orphelins est-elle ouverte dans un nouveau dossier ou dans celui du défunt ? On voit les deux cas. Dans T25, on compte la réversion comme une demande à part, déposée et liquidée sur la ligne des survivants.
   Réponse :

## Ce qu'on applique, à confirmer

6. Un paiement est un débours de type PM au statut D (payé par effet) ou P (payé sans effet). On prend le montant du débours, jamais le net à payer de l'effet. Les autres statuts (A, I, N, S, V) ne sont pas des paiements : c'est bien ça ?
7. Branches : V pensions, A risques professionnels, F prestations familiales, M maternité, rattachée aux prestations familiales.
8. On compte l'ayant droit, une fois par tableau. L'orphelin et non son tuteur ; dans T41, l'allocataire est le dossier (Kaya 2017).
9. Âge = année de l'exercice moins année de naissance, comme dans le glossaire de l'Annuaire, avec la grille de tranches propre à chaque tableau. L'année 1900 ressemble à une date par défaut (503 personnes, presque toutes des veuves) : on la met en âge inconnu.
10. Orphelins comptés jusqu'à 24 ans. Fin 2025, 8 447 pensions d'orphelin sont actives sans aucun paiement au dernier trimestre. Des orphelins de 25 ans et plus jamais radiés ?
11. Coordination : les pensions payées par une autre caisse (COOR_AC) comptent dans l'effectif, sans montant. Services passés = mois cotisés moins PE_COT_AUTRE, pour garder la part CNSS (Kaya 2017). PE_COT_AUTRE, ce sont bien les mois cotisés dans l'autre caisse ?
12. Bonification, majoration pour enfant et contribution aux charges du ménage suivent la pension : pas de pensionné en plus.
13. Dossiers (T25 à T27) : déposé = réception au guichet ; liquidé = première notification ; en instance = ni liquidé ni rejeté au 31/12 ; rejeté = module des rejets. Durée = de la réception à la notification, en trois tranches (moins de 15 jours, 15 à 45, plus de 45), norme 45 jours.
14. Chaque événement compte dans son année : un dossier déposé en 2024 et liquidé en 2025 est un déposé de 2024 et un liquidé de 2025.
15. Le Total de T25 à T27 recalcule la durée sur toutes les demandes. Taux restant à traiter = en instance au 31/12, divisé par (en instance au 1er janvier + déposés de l'année).
16. Services passés : les mois cotisés de la dernière prestation de la personne ; une valeur au-dessus de 600 mois est écartée.
17. Indus (T31) : trop-perçus (TP, TP-CONV) au mois où ils sont établis. Les moins-perçus (MP, RC) sont montrés à part, hors du taux, puisque Kaya 2017 dit que ce ne sont pas des indus. On écarte les remboursements de trop-perçu, les avances, les mois gratuits, les dons COVID et le statut R (annulé ?).
18. Chaque code de prestation va dans une ligne CIPRES : la liste est à la fin, tableau par tableau. Est-elle juste, en particulier pour la retraite anticipée (RPRA) et les accessoires de pension ?

## Questions par tableau

T25 à T27, les dossiers

19. Les dossiers de coordination payés par une autre caisse vont-ils dans T25 ? On les compte ; les dossiers transférés, non.
20. La date de réception au guichet est-elle celle du dossier complet ?
21. Une demande ouverte depuis des années reste-t-elle en instance ? On propose d'écarter les réceptions d'avant 2016 jamais reliées à un dossier et, en AT/MP et PF, ce qui est sans suite depuis 24 mois. Que deviennent les réceptions qui n'ouvrent aucun dossier ?
22. Les refus sont-ils tous saisis dans le module des rejets ? On n'en voit qu'une quinzaine par an en pensions, et 21 en PF en 2025 pour 14 306 réceptions.
23. T26 : une déclaration d'accident sans arrêt, sans soins et sans rente, est-ce une demande ? Les soins entrent-ils dans T26 (autres prestations de la branche), liquidés au premier remboursement ?
24. T26 : la durée d'une rente part-elle de la demande au guichet (14 jours en médiane) ou du certificat de consolidation (64 jours, comme le dit Kaya) ? Et qu'attend une IJ pendant les 7 mois qui suivent la fin de l'arrêt ?
25. T27 : un enfant ajouté au dossier d'un allocataire est-il une demande ? Sans lui, 8 246 liquidations en 2025, avec lui environ 16 400. Une grossesse compte-t-elle sur les deux lignes (IJ et allocation prénatale) ou une seule fois ?

T28 à T30, les contrôles a posteriori

26. On compte les dossiers payés et, parmi eux, ceux contrôlés après paiement, par trimestre ; taux = contrôlés divisés par payés, norme d'au moins 5 %. Or la table des contrôles n'est plus alimentée depuis juillet 2025 (115 contrôles en 2025 pour 155 206 dossiers payés) et contient des années 202, 203 et 2055. Kaya 2017 parle de rapports manuels de la DAI. Où sont saisis les contrôles aujourd'hui ? Le drapeau DEB_VERIFIE n'est pas un contrôle, on est d'accord ?

T31, les indus

27. Une pension versée après un décès déclaré tard devient-elle un trop-perçu ? On n'en trouve que 7 en 2024.
28. Aucun trop-perçu n'est soldé depuis 2017. Comment voit-on qu'un indu est recouvré ?
29. Les moins-perçus des prestations familiales ont presque disparu : 56 362 en 2023, 1 384 en 2025. Pourquoi ?
30. Les rappels et les avances versés par ajustement font-ils partie des prestations payées de T31 ?

T32 à T41, les bénéficiaires au 31/12

31. La retraite anticipée va-t-elle dans T32, avec la vieillesse ? 87 pensionnés en 2024 ; l'annuaire la publie à part.
32. Le montant d'un tableau : ce qui a été payé aux bénéficiaires comptés au 31/12 (37,67 Md en 2024 pour T32), ou tout ce qui a été payé dans l'année, décédés compris (38,63 Md) ? L'annuaire, 38,24 Md, tombe entre les deux.
33. Pension moyenne : on y garde les COOR_AC, que la CNSS ne paie pas ? Elle baisse alors de près de 9 %.
34. Depuis 2023, nos veuves (T34) et nos orphelins (T35) restent sous l'annuaire, alors que la vieillesse retombe juste. Les radiations sont-elles saisies avec retard ?
35. T41 : un enfant qui n'a rien touché dans l'année compte-t-il ? On ne retrouve l'annuaire qu'en comptant les seuls enfants payés (+0,7 %, contre +5 % sinon). L'âge et le sexe sont-ils ceux du titulaire du dossier ?
36. T41 : notre montant reste sous l'annuaire (7,69 Md pour 8,24 Md en 2024). Comptez-vous les rappels payés dans l'année pour des mois anciens ? Une femme qui ne touche qu'une allocation prénatale est-elle allocataire ? Faut-il ajouter un tableau des enfants par âge, comme le tableau 36 de l'annuaire ?
37. 69 pensionnés payés ont 100 ans ou plus, et 215 pensions sont actives alors que le titulaire est décédé. Un contrôle d'existence est-il prévu ?

T42 à T49, les nouveaux bénéficiaires

38. Le tableau 25 de l'annuaire compte 527 femmes parmi les nouveaux retraités de 2024, la base 641. Comment le sexe et l'âge y sont-ils établis ?
39. Le montant d'un nouveau retraité comprend-il le rappel des mois d'avant l'année ?
40. T45 compte-t-il toutes les nouvelles rentes AT/MP, viagères et d'ayants droit ? T47, les ascendants, est-ce bien ceux des rentes AT/MP (Kaya) ? T48 : 44 rentes viagères publiées en 2024, 39 dans la base. Comment sont-elles comptées ?

T50 à T57, l'évolution sur cinq ans

41. Un bénéficiaire de l'année est-il celui dont la prestation est liquidée dans l'année (Kaya le dit pour T54 et T55) ou payée ? Et compte-t-il une fois, ou à chaque prestation ?
42. T56 : où sont payées les prestations en nature des AT/MP (139,7 M publiés en 2024, rien dans les débours) et les rentes AT/MP, 12 % sous le publié dans les débours ? Les rachats de rente (21,5 M) et les rappels de moins-perçus sont-ils des prestations servies ? Que met-on dans la ligne « Autres » ?
43. T57 : seulement les titulaires d'une pension directe (Kaya), ou tous les pensionnés décédés ? À la date du décès ou à celle de sa déclaration ? Un sinistré est-il une victime morte de son accident ? L'annuaire publie 89 décès d'ascendants pour 59 pensions d'ascendant en cours.

## Plutôt pour la DSI

44. Les statuts n'ont pas de libellés (dossier, prestation, débours, effet, réception, notification, ajustement), ni les drapeaux *_VERIFIE.
45. 62 % des paiements PF de 2025 n'ont pas d'effet de paiement, et la date de décaissement manque pour 80 % d'entre eux. Par quel circuit passent les paiements au statut P (virement, mobile) ? Où trouver la date de décaissement ?
46. Les débours liés à un trop-perçu ont les deux signes (de −6,1 M à +6,6 M en 2024). Lesquels sont des retenues, lesquels des versements ?
47. Les mois cotisés valent parfois 999, et une allocation de vieillesse a 489 mois cotisés. Les colonnes « obligatoire » et « volontaire » sont-elles encore remplies ?
48. 120 518 dossiers n'ont pas de catégorie : sont-ils tous ordinaires ? Que veut dire CNSS_AC ?
49. TSINISTRE n'est plus alimentée depuis décembre 2024. Quand la copie du 02/02/2026 sera-t-elle rafraîchie ?
50. La vue V_PRESTATAIRES_DECEDES_BI contient le nom, le prénom, l'adresse et le téléphone des décédés. Peut-on retirer ces colonnes ? DOS_DECES_IMMEDIAT sera-t-il rempli pour les accidents d'avant 2024 ?
51. Lieux de paiement : que veut dire DR_OU_SP quand il est vide (71 lieux) ? Le lieu 201, « caisse tenkodogo », rattaché à la DRB sans service, est-il une erreur ? Que désigne CNSS-DCP ?
52. Des dates de naissance sont partagées par beaucoup de personnes nées la même année : sont-ce des dates approchées (« né vers ») ?

## Tous les tableaux, un par un

- T25, dossiers PVID par durée de liquidation (RPV, RPRA, RPI, RPSV, RPSF, RPSE, RAV, RAS, RASC, RASO, RASA, accessoires) : points 5, 13 à 15, 18 à 22.
- T26, dossiers AT/MP (ARI, ARSV, ARSF, ARSE, ARSA, AIJ, AAI, soins) : points 13 à 15, 18, 20 à 24.
- T27, dossiers PF (FAF, MIJ, FAP, MAP) : points 13 à 15, 18, 20 à 22, 25.
- T28, dossiers PVID contrôlés a posteriori : point 26.
- T29, dossiers AT/MP contrôlés a posteriori : point 26.
- T30, dossiers PF contrôlés a posteriori : point 26.
- T31, prestations indues par branche et par mois : points 1, 6, 17, 27 à 30, 46.
- T32, pensionnés de vieillesse (RPV, RPRA) : points 1, 3, 6, 8, 9, 11, 12, 31 à 33, 37.
- T33, pensionnés d'invalidité (RPI) : points 3, 8, 9, 32.
- T34, pensionnés veufs et veuves (RPSV, RPSF) : points 3, 8, 9, 32, 34.
- T35, pensionnés orphelins (RPSE) : points 3, 8 à 10, 32, 34.
- T36, pensionnés ascendants (RPA) : points 3, 8, 9, 32, 43.
- T37, bénéficiaires de rente viagère AT/MP (ARI) : points 3, 8, 9, 32, 42.
- T38, crédirentiers veufs et veuves (ARSV, ARSF) : points 3, 8, 9, 32, 42.
- T39, crédirentiers orphelins (ARSE) : points 3, 8 à 10, 32, 42.
- T40, crédirentiers ascendants (ARSA) : points 3, 8, 9, 32, 42.
- T41, allocataires PF (FAF) : points 8, 9, 35, 36.
- T42, nouveaux pensionnés de vieillesse et services passés (RPV, RPRA) : points 4, 9, 11, 16, 31, 38, 39.
- T43, nouveaux pensionnés veufs et veuves (RPSV, RPSF) : points 4, 9, 39.
- T44, nouveaux pensionnés orphelins (RPSE) : points 4, 9, 10, 39.
- T45, nouveaux crédirentiers AT/MP (ARI, ARSV, ARSF, ARSE, ARSA) : points 4, 9, 40.
- T46, nouveaux crédirentiers orphelins (ARSE) : points 4, 9, 40.
- T47, nouveaux crédirentiers ascendants (ARSA) : points 4, 9, 40.
- T48, nouveaux bénéficiaires de rente viagère et degré d'IPP (ARI) : points 4, 9, 40.
- T49, nouveaux crédirentiers veufs et veuves (ARSV, ARSF) : points 4, 9, 40.
- T50, allocations de vieillesse sur cinq ans, avec les services passés (RAV) : points 1, 11, 16, 41.
- T51, allocations de survivants (RAS, RASC, RASO, RASA) : points 1, 41.
- T52, indemnités journalières AT/MP (AIJ) : points 1, 41.
- T53, allocations d'incapacité (AAI) : points 1, 41.
- T54, indemnités de maternité (MIJ) : points 1, 41.
- T55, allocations prénatales (FAP, MAP) : points 1, 41.
- T56, prestations servies par branche : points 1, 2, 6, 42.
- T57, décédés par branche : points 2, 43.
