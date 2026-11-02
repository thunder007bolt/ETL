# Prestations : règles et questions à valider

Mis à jour le 01/11/2026. Base consultée : copie de la base des prestations (schéma USER_DWH) du 02/02/2026, comparée à l'annuaire statistique CNSS 2024.

Chaque point dit de quoi on parle, où on le voit dans la base, ce qu'on fait aujourd'hui, puis pose une seule question. Les noms en majuscules sont ceux des tables et des champs de la base : avec eux, on peut refaire chaque chiffre. Tous les tableaux T25 à T57 sont repris à la fin, avec leurs codes de prestation. S'il n'y a pas le temps de tout voir : les points 1 à 5 d'abord.

## Pour lire ce document

- Le dossier, table DOSSIER : la demande d'un assuré (DOS_CODE). Sa branche (TDOS_CODE) : V pensions de vieillesse, d'invalidité et de décès ; A accidents du travail et maladies professionnelles ; F prestations familiales ; M maternité. Son statut (DOS_STATUT) : A actif, F fermé. Sa catégorie (DOS_CATEGORIE) : vide pour un dossier ordinaire, COOR_AC ou COOR_CNSS pour une coordination avec une autre caisse.
- La prestation, table PRESTATION_ESP : un droit ouvert dans un dossier, par exemple une pension de veuve. Elle a un code (TPE_CODE, la liste est à la fin), la personne qui y a droit (IND_ID_PREST) et ses dates : début du droit (PE_DATE_EFFET), saisie (PE_DATE_INSERT), notification (PE_DATE_NOTIFICATION), suspension (PE_DATE_SUSPENSION), radiation (PE_DATE_RADIATION). Elle porte aussi les mois cotisés (PE_MOIS_COTISATION).
- La personne, table INDIVIDU : date de naissance (IND_DATE_NAISSANCE), sexe (IND_SEXE : 1 homme, 2 femme), date de décès (IND_DATE_DECES).
- Le paiement, table DEBOURS : chaque somme due à une personne. Son type (DEB_TYPE : PM pour un paiement), son statut (DEB_STATUT), son montant (DEB_MONTANT), le mois qu'il paie (DEB_DATE_EFFET) et le jour de sa saisie (DEB_DATE_INSERT). Plusieurs débours sont réglés ensemble par un effet de paiement (table EFFET).
- Le guichet, table RECEPTION_DOSSIER : chaque dépôt, avec sa date (RD_DATE_RECEPTION) et son type (RD_TYPE_RECEPTION : NOUVEAU pour un nouveau dossier, ANCIEN pour une pièce sur un dossier existant). Les refus sont dans la table DOSSIER_REJETE.
- Les corrections de paiement, table AJUSTEMENT : un code (TAJ_CODE : TP trop-perçu, MP moins-perçu, AV avance…), une date (AJ_DATE_ETABLISSEMENT) et un statut (AJ_STATUT : A en cours, C soldé, R annulé).

## À trancher en priorité

1. L'année d'un paiement
   C'est quoi : un paiement peut payer un mois ancien. Par exemple, un rappel de 15 mois versé en mars 2024 paie surtout des mois de 2023.
   Dans la base : chaque paiement (DEBOURS) a deux dates : le mois qu'il paie (DEB_DATE_EFFET) et le jour où il a été saisi (DEB_DATE_INSERT).
   Aujourd'hui : on prend le mois payé. Pourtant la date de saisie colle mieux à l'annuaire : sur 11 lignes sur 16, et à 0,02 % près pour les allocations de vieillesse.
   Question : dans l'annuaire, un paiement compte-t-il pour l'année du mois qu'il paie, ou pour l'année où il est versé ?
   Réponse :

2. La date d'arrêt de l'annuaire
   C'est quoi : un décès ou une radiation est souvent saisi des mois après. Plus on calcule tard, plus on en trouve.
   Dans la base : les décès de retraités de 2024 saisis avant avril 2025 (INDIVIDU, IND_DATE_DECES) sont 444, pour 447 dans le tableau 26 de l'annuaire. Aujourd'hui, la base en compte 619.
   Question : l'annuaire est-il arrêté vers la fin mars ? Si oui, on calculera l'année N avec la base de fin mars N+1.
   Réponse :

3. Le pensionné au 31 décembre
   C'est quoi : les tableaux T32 à T41 comptent les bénéficiaires présents au 31 décembre.
   Dans la base : une prestation (PRESTATION_ESP) en cours au 31/12 : commencée avant (PE_DATE_EFFET), ni radiée ni suspendue ce jour-là (PE_DATE_RADIATION, PE_DATE_SUSPENSION), et dont le titulaire est en vie (IND_DATE_DECES vide ou après le 31/12). On ne demande pas qu'elle ait été payée en fin d'année.
   Chiffre : 31 093 pensionnés de vieillesse en 2024 dans la base, 30 808 dans l'annuaire. En exigeant un paiement, on en perdrait 11 % : les pensions payées par une autre caisse et celles qui attendent un certificat de vie.
   Question : un pensionné qui n'a pas été payé en fin d'année compte-t-il au 31/12 ?
   Réponse :

4. Le nouveau pensionné
   C'est quoi : les tableaux T42 à T49 comptent les nouveaux bénéficiaires de l'année.
   Dans la base : la première prestation de la personne dans le tableau, saisie dans l'année (PE_DATE_INSERT). Une révision ou une reliquidation d'une pension existante ne compte pas.
   Chiffre : 2 463 nouveaux retraités en 2024 dans la base, 2 489 dans l'annuaire. En prenant la date de notification (PE_DATE_NOTIFICATION), on tomberait à 2 397.
   Question : un nouveau pensionné se compte-t-il à la saisie de sa pension, ou à sa notification ?
   Réponse :

5. La pension de la veuve
   C'est quoi : quand un retraité décède, sa veuve et ses orphelins reçoivent une pension (la réversion).
   Dans la base : on voit les deux cas : la pension de la veuve ouverte dans un nouveau dossier, ou ajoutée au dossier du défunt (une prestation RPSV ou RPSF de plus dans le même dossier).
   Aujourd'hui : dans T25, la réversion compte comme une demande à part, déposée et liquidée sur la ligne des survivants, même si elle est dans le dossier du défunt.
   Question : la pension de la veuve doit-elle être ouverte dans un nouveau dossier, ou dans celui du défunt ?
   Réponse :

## Ce qu'on applique, à confirmer

6. Ce qu'est un paiement
   Dans la base : un débours de type PM (DEB_TYPE = PM) au statut D, payé par effet, ou P, payé sans effet (DEB_STATUT). On prend le montant du débours (DEB_MONTANT), jamais le net à payer de l'effet, qui se répète sur chaque débours.
   Question : les autres statuts (A, I, N, S, V) ne sont bien pas des paiements ?
   Réponse :

7. Les branches
   Dans la base : la branche du dossier (TDOS_CODE) : V pensions, A risques professionnels, F prestations familiales, M maternité. La maternité va avec les prestations familiales.
   Question : d'accord ?
   Réponse :

8. La personne comptée
   C'est quoi : l'ayant droit est la personne qui a droit à la prestation ; ce n'est pas toujours celle qui est payée. Pour un orphelin, c'est son tuteur qui est payé.
   Dans la base : on compte l'ayant droit (PRESTATION_ESP, IND_ID_PREST), une fois par tableau. Pour T41, l'allocataire est le dossier, comme décidé à Kaya en 2017.
   Question : on garde cette règle ?
   Réponse :

9. L'âge
   Dans la base : année de l'exercice moins année de naissance (IND_DATE_NAISSANCE), comme dans le glossaire de l'Annuaire, avec la grille de tranches propre à chaque tableau. L'année de naissance 1900 ressemble à une date par défaut (503 personnes, presque toutes des veuves) : on la met en « âge inconnu ».
   Question : on garde ce calcul, et 1900 est-elle bien une date par défaut ?
   Réponse :

10. L'âge limite des orphelins
   C'est quoi : un orphelin a droit à une pension jusqu'à un certain âge.
   Aujourd'hui : on compte les orphelins jusqu'à 24 ans, comme l'annuaire.
   Chiffre : fin 2025, 8 447 pensions d'orphelin (code RPSE) sont encore actives sans aucun paiement au dernier trimestre : sans doute des orphelins de 25 ans et plus, jamais radiés.
   Question : la limite est-elle bien 25 ans, et faut-il radier ces pensions ?
   Réponse :

11. La coordination
   C'est quoi : un assuré qui a cotisé dans deux caisses (deux pays, par exemple) reçoit une pension de chacune.
   Dans la base : la catégorie du dossier (DOS_CATEGORIE) vaut COOR_AC quand la pension est payée par l'autre caisse. Ces pensions comptent dans l'effectif, sans montant. Pour les services passés, on garde la part CNSS : les mois cotisés (PE_MOIS_COTISATION) moins les mois de l'autre caisse (PE_COT_AUTRE), comme décidé à Kaya en 2017.
   Question : PE_COT_AUTRE, ce sont bien les mois cotisés dans l'autre caisse ?
   Réponse :

12. Les accessoires de pension
   C'est quoi : la bonification ou la majoration pour enfant et la contribution aux charges du ménage s'ajoutent à une pension.
   Aujourd'hui : elles suivent la pension et ne font pas un pensionné de plus.
   Question : d'accord ?
   Réponse :

13. Les dossiers (T25 à T27)
   Dans la base : un dossier est déposé le jour de sa réception au guichet (RD_DATE_RECEPTION) ; liquidé à la première notification des droits (PE_DATE_NOTIFICATION) ; en instance s'il n'est ni liquidé ni rejeté au 31/12 ; rejeté s'il est dans DOSSIER_REJETE. La durée de liquidation va de la réception à la notification, en trois tranches : moins de 15 jours, de 15 à 45, plus de 45 (norme : 45 jours).
   Question : on garde ces règles ?
   Réponse :

14. L'année de chaque étape
   Aujourd'hui : chaque étape compte dans son année. Un dossier déposé en 2024 et liquidé en 2025 compte dans les déposés de 2024 et dans les liquidés de 2025.
   Question : d'accord ?
   Réponse :

15. La ligne Total et le taux restant à traiter
   Aujourd'hui : la ligne Total de T25 à T27 recalcule la durée sur toutes les demandes ; elle ne fait pas la moyenne des lignes. Le taux restant à traiter est le nombre en instance au 31/12, divisé par le nombre en instance au 1er janvier plus les déposés de l'année.
   Question : d'accord ?
   Réponse :

16. Les services passés
   C'est quoi : le nombre de mois de travail cotisés avant la pension (T42, T50).
   Dans la base : les mois cotisés de la dernière prestation de la personne (PE_MOIS_COTISATION). Une valeur de plus de 600 mois (50 ans) est écartée.
   Question : d'accord ?
   Réponse :

17. Les indus (T31)
   C'est quoi : un trop-perçu est une somme payée en trop ; un moins-perçu, une somme payée en moins, rattrapée ensuite par un rappel.
   Dans la base : dans AJUSTEMENT, les trop-perçus (TAJ_CODE = TP ou TP-CONV) et les moins-perçus (MP ou RC), comptés le mois où ils sont établis (AJ_DATE_ETABLISSEMENT). Les moins-perçus sont montrés à part, hors du taux d'indus : Kaya 2017 dit que ce ne sont pas des indus. On écarte les remboursements de trop-perçu (REM-TP), les avances (AV), les mois gratuits (MPG), les dons COVID et les ajustements au statut R.
   Question : on garde ces règles, et le statut R veut-il bien dire annulé ?
   Réponse :

18. Les codes et les lignes CIPRES
   Dans la base : chaque prestation a un code (TPE_CODE), par exemple RPV pour une pension de vieillesse. La liste des codes de chaque tableau est à la fin du document.
   Question : cette liste est-elle juste, en particulier la retraite anticipée (RPRA) avec la vieillesse, et les accessoires ?
   Réponse :

## Questions par tableau

T25 à T27, les dossiers

19. La coordination dans T25
   C'est quoi : un dossier de coordination payé par une autre caisse est reçu et instruit par la CNSS, mais payé ailleurs.
   Aujourd'hui : on le compte dans T25. Un dossier transféré à une autre caisse, non.
   Question : d'accord ?
   Réponse :

20. La date du dépôt
   Question : la date de réception au guichet (RD_DATE_RECEPTION) est-elle celle du dossier complet ?
   Réponse :

21. Les demandes qui traînent
   Dans la base : des demandes restent ouvertes des années. On écarte les réceptions d'avant 2016 jamais reliées à un dossier ; en risques professionnels et en prestations familiales, une demande sans suite depuis 24 mois n'est plus en instance.
   Question : une demande sans suite reste-t-elle « en instance » sans limite ?
   Réponse :

22. Les refus
   Dans la base : on ne voit qu'une quinzaine de refus par an en pensions (DOSSIER_REJETE), et 21 en prestations familiales en 2025, pour 14 306 réceptions.
   Question : tous les refus sont-ils saisis dans le module des rejets ?
   Réponse :

23. La déclaration d'accident sans suite (T26)
   C'est quoi : après un accident du travail, l'employeur le déclare. Il s'ensuit, ou non, un arrêt payé en indemnités journalières, des soins, puis une rente.
   Chiffre : environ 550 déclarations par an ne donnent ni arrêt, ni soins, ni rente.
   Question : une telle déclaration est-elle une demande à compter dans T26 ?
   Réponse :

24. Les soins (T26)
   Chiffre : environ 650 dossiers par an donnent un remboursement de soins ; 31 % des dossiers n'ont que des soins.
   Question : les soins entrent-ils dans T26, en « autres prestations de la branche », liquidés au premier remboursement ?
   Réponse :

25. La durée d'une rente (T26)
   Chiffre : depuis la demande au guichet, une rente est liquidée en 14 jours (médiane) ; depuis la réception du certificat de consolidation, en 64 jours, comme le dit Kaya 2017.
   Question : la durée d'une rente part-elle de la demande au guichet, ou du certificat de consolidation ?
   Réponse :

26. L'attente des indemnités journalières (T26)
   Chiffre : une indemnité journalière est notifiée environ 7 mois après la fin de l'arrêt de travail.
   Question : qu'attend-elle pendant ce temps (certificat de guérison, attestation de salaire de l'employeur) ?
   Réponse :

27. L'enfant ajouté (T27)
   C'est quoi : quand un allocataire déclare un nouvel enfant, l'allocation est ajoutée à son dossier, sans passer au guichet.
   Chiffre : sans les enfants ajoutés, 8 246 allocations familiales liquidées en 2025 ; avec eux, environ 16 400.
   Question : un enfant ajouté est-il une demande à compter dans T27 ?
   Réponse :

28. La grossesse sur deux lignes (T27)
   C'est quoi : un dossier de maternité porte l'indemnité de maternité et l'allocation prénatale, notifiées ensemble.
   Chiffre : 4 273 indemnités de maternité et 4 245 allocations prénatales liquidées en 2025.
   Question : une grossesse compte-t-elle sur les deux lignes, ou une seule fois ?
   Réponse :

T28 à T30, les contrôles a posteriori

29. Les contrôles
   C'est quoi : un contrôle a posteriori vérifie un dossier après son paiement. T28 à T30 comptent, par trimestre, les dossiers payés et ceux contrôlés ; la norme est d'au moins 5 % de dossiers contrôlés.
   Dans la base : la table DOSSIERS_CONTROLES n'est plus remplie depuis juillet 2025 (115 contrôles en 2025 pour 155 206 dossiers payés) et contient des années 202, 203 et 2055. Le drapeau DEB_VERIFIE des débours n'est pas un contrôle. Kaya 2017 parle de rapports manuels de la DAI.
   Question : où sont notés les contrôles a posteriori aujourd'hui ?
   Réponse :

T31, les indus

30. La pension versée après un décès
   C'est quoi : quand un décès est déclaré tard, la pension a continué d'être versée pour rien.
   Chiffre : on ne trouve que 7 trop-perçus après un décès en 2024, alors que 175 décès de retraités ont été déclarés après mars 2025.
   Question : ces sommes deviennent-elles des trop-perçus ?
   Réponse :

31. Le recouvrement d'un indu
   Dans la base : aucun trop-perçu n'est soldé depuis 2017 (AJ_STATUT = C) : ils restent en cours, repris par des retenues.
   Question : comment voit-on qu'un indu est remboursé ?
   Réponse :

32. Les moins-perçus des prestations familiales
   Chiffre : 56 362 moins-perçus en 2023, 1 384 en 2025.
   Question : pourquoi ont-ils presque disparu ?
   Réponse :

33. Les paiements par ajustement
   C'est quoi : certains rappels et avances sont versés par un ajustement, pas par un débours de paiement.
   Question : font-ils partie des prestations payées de T31 ?
   Réponse :

T32 à T41, les bénéficiaires au 31/12

34. La retraite anticipée (T32)
   Chiffre : 87 pensionnés de retraite anticipée (code RPRA) en 2024. L'annuaire les publie à part.
   Question : la retraite anticipée va-t-elle dans T32, avec la vieillesse ?
   Réponse :

35. Le montant d'un tableau
   Chiffre : pour T32 en 2024, ce qui a été payé aux pensionnés comptés au 31/12 fait 37,67 Md ; tout ce qui a été payé dans l'année, décédés compris, 38,63 Md. L'annuaire publie 38,24 Md, entre les deux.
   Question : le montant d'un tableau est-il ce qui a été payé aux bénéficiaires comptés, ou tout ce qui a été payé dans l'année ?
   Réponse :

36. La pension moyenne et la coordination
   Chiffre : fin 2025, 2 901 pensions de vieillesse sont payées par une autre caisse (COOR_AC) : la CNSS ne leur verse rien. Les garder dans la pension moyenne la fait baisser de près de 9 %.
   Question : faut-il les garder dans la pension moyenne ?
   Réponse :

37. Les veuves et les orphelins
   Chiffre : depuis 2023, nos veuves (T34) et nos orphelins (T35) restent sous l'annuaire, alors que la vieillesse retombe juste.
   Question : les radiations des veuves et des orphelins sont-elles saisies avec retard ?
   Réponse :

38. L'enfant non payé (T41)
   C'est quoi : un enfant peut avoir droit à l'allocation au 31/12 sans avoir rien touché dans l'année (un certificat attendu, par exemple).
   Chiffre : en ne comptant que les enfants payés, on est à 0,7 % de l'annuaire ; en les comptant tous, à 5 %.
   Question : un enfant qui n'a rien touché dans l'année compte-t-il ?
   Réponse :

39. Qui est l'allocataire (T41)
   Dans la base : on prend l'âge et le sexe du titulaire du dossier, l'assuré qui ouvre le droit : 102 637 titulaires pour 102 643 dossiers.
   Question : l'âge et le sexe de T41 sont-ils bien ceux du titulaire du dossier ?
   Réponse :

40. Le montant de T41
   Chiffre : 7,69 Md dans la base pour 8,24 Md dans l'annuaire en 2024.
   Question : comptez-vous les rappels payés dans l'année pour des mois anciens ?
   Réponse :

41. L'allocation prénatale seule (T41)
   Question : une femme qui ne touche qu'une allocation prénatale est-elle une allocataire de T41 ?
   Réponse :

42. Un tableau des enfants (T41)
   Chiffre : 271 577 enfants en 2024, de 0 à 21 ans.
   Question : faut-il ajouter un tableau des enfants par âge, comme le tableau 36 de l'annuaire ?
   Réponse :

43. Les pensionnés très âgés
   Chiffre : 69 pensionnés payés ont 100 ans ou plus, et 215 pensions sont encore actives alors que leur titulaire est décédé.
   Question : un contrôle d'existence est-il prévu ?
   Réponse :

T42 à T49, les nouveaux bénéficiaires

44. Le sexe et l'âge des nouveaux retraités
   Chiffre : le tableau 25 de l'annuaire compte 527 femmes parmi les nouveaux retraités de 2024, la base 641. Âge par âge les chiffres se décalent, mais les tranches de 5 ans retombent.
   Question : comment l'annuaire établit-il le sexe et l'âge des nouveaux retraités ?
   Réponse :

45. Le rappel d'un nouveau retraité
   C'est quoi : le premier paiement d'un nouveau retraité couvre souvent des mois d'avant l'année.
   Chiffre : 2,86 Md pour les mois de 2024 ; 3,22 Md saisis en 2024, dont 0,23 Md de rappel.
   Question : le montant d'un nouveau retraité comprend-il ce rappel ?
   Réponse :

46. T45, les nouveaux crédirentiers
   Question : T45 compte-t-il toutes les nouvelles rentes d'accident du travail, celles des victimes (ARI) et celles de leurs ayants droit (ARSV, ARSF, ARSE, ARSA) ?
   Réponse :

47. T47, les ascendants
   Question : T47 compte-t-il bien les ascendants des rentes d'accident du travail (ARSA), comme le range l'atelier de Kaya ?
   Réponse :

48. T48, les rentes viagères
   Chiffre : 44 nouvelles rentes viagères publiées en 2024, 39 dans la base (68 avec les révisions).
   Question : comment l'annuaire compte-t-il ses nouvelles rentes viagères ?
   Réponse :

T50 à T57, l'évolution sur cinq ans

49. Le bénéficiaire de l'année
   Question : un bénéficiaire de l'année est-il celui dont la prestation est liquidée dans l'année, comme Kaya le dit pour T54 et T55, ou celui qui est payé ?
   Réponse :

50. Une personne ou chaque prestation
   Chiffre : 195 victimes ont eu une indemnité journalière liquidée en 2024, pour 371 indemnités.
   Question : un bénéficiaire qui reçoit plusieurs prestations dans l'année compte-t-il une fois, ou à chaque prestation ?
   Réponse :

51. Des paiements introuvables (T56)
   Chiffre : les soins des accidents du travail (139,7 M publiés en 2024) n'apparaissent dans aucun débours, et les rentes d'accident du travail y sont 12 % sous le publié.
   Question : où sont payés ces montants ?
   Réponse :

52. Les rachats de rente et les rappels (T56)
   Chiffre : les rachats de rente sont versés par ajustement : 21,5 M en 2024 dans la base, 25,4 M dans l'annuaire.
   Question : les rachats de rente et les rappels de moins-perçus sont-ils des prestations servies ?
   Réponse :

53. La ligne « Autres » (T56)
   Question : que met-on dans la ligne « Autres » de T56 ?
   Réponse :

54. Qui compter dans T57
   Chiffre : 622 décès en 2024 parmi les titulaires d'une pension directe, 747 parmi tous les pensionnés (veuves et orphelins compris) ; l'annuaire en publie 648.
   Question : T57 compte-t-il les seuls titulaires d'une pension directe (Kaya 2017), ou tous les pensionnés décédés ?
   Réponse :

55. La date d'un décès (T57)
   Question : un décès compte-t-il l'année où il a lieu, ou l'année où il est déclaré ?
   Réponse :

56. Le sinistré (T57)
   Question : un sinistré décédé est-il une victime d'accident du travail morte de son accident ?
   Réponse :

57. Les décès d'ascendants (T57)
   Chiffre : l'annuaire publie 89 décès d'ascendants en 2024, pour 59 pensions d'ascendant en cours.
   Question : d'où vient ce chiffre ?
   Réponse :

## Plutôt pour la DSI

58. Les statuts sans libellé
   Dans la base : les statuts n'ont pas de liste de libellés : dossier (DOS_STATUT), prestation (PE_STATUT), débours (DEB_STATUT), effet, réception, notification, ajustement ; pas plus que les drapeaux *_VERIFIE.
   Question : que veulent dire ces valeurs ?
   Réponse :

59. Les paiements sans effet
   Chiffre : 62 % des paiements des prestations familiales de 2025 n'ont pas d'effet de paiement.
   Question : par quel circuit passent les paiements au statut P (virement, téléphone mobile) ?
   Réponse :

60. La date de décaissement
   Chiffre : la date de décaissement manque pour 80 % des paiements des prestations familiales et 43 % des paiements de vieillesse de 2025.
   Question : où la trouver ?
   Réponse :

61. Les débours de trop-perçu
   Chiffre : les débours liés à un trop-perçu ont les deux signes, de −6,1 M à +6,6 M en 2024.
   Question : lesquels sont des retenues, lesquels des versements ?
   Réponse :

62. Les mois cotisés
   Dans la base : PE_MOIS_COTISATION vaut parfois 999, et une allocation de vieillesse a 489 mois cotisés.
   Question : que veut dire 999, et les colonnes « obligatoire » et « volontaire » sont-elles encore remplies ?
   Réponse :

63. Les dossiers sans catégorie
   Dans la base : 120 518 dossiers n'ont pas de catégorie (DOS_CATEGORIE vide).
   Question : sont-ils tous des dossiers ordinaires, et que veut dire CNSS_AC ?
   Réponse :

64. Les réceptions sans dossier
   Chiffre : plus de réceptions que de dossiers ouverts : en pensions en 2025, 4 671 réceptions pour 3 638 dossiers.
   Question : que deviennent les réceptions qui n'ouvrent aucun dossier ?
   Réponse :

65. La copie de la base
   Dans la base : la table TSINISTRE n'est plus remplie depuis décembre 2024, et la copie date du 02/02/2026.
   Question : quand sera-t-elle rafraîchie ?
   Réponse :

66. La vue des décédés
   Dans la base : la vue V_PRESTATAIRES_DECEDES_BI contient le nom, le prénom, l'adresse et le téléphone des décédés, et DOS_DECES_IMMEDIAT n'est rempli que depuis 2024.
   Question : peut-on retirer ces colonnes de la vue, et remplir DOS_DECES_IMMEDIAT pour les accidents d'avant 2024 ?
   Réponse :

67. Les lieux de paiement
   Dans la base : DR_OU_SP est vide pour 71 lieux ; le lieu 201, « caisse tenkodogo », est rattaché à la DRB sans service ; on trouve aussi CNSS-DCP.
   Question : que veulent dire ces valeurs, et le lieu 201 est-il une erreur ?
   Réponse :

68. Les dates de naissance approchées
   Dans la base : des dates de naissance sont partagées par beaucoup de personnes nées la même année.
   Question : sont-ce des dates approchées (« né vers ») ?
   Réponse :

## Tous les tableaux, un par un

- T25, dossiers de pension par durée de liquidation (RPV, RPRA, RPI, RPSV, RPSF, RPSE, RAV, RAS, RASC, RASO, RASA, accessoires) : points 5, 13 à 15, 18 à 22.
- T26, dossiers d'accident du travail (ARI, ARSV, ARSF, ARSE, ARSA, AIJ, AAI, soins) : points 13 à 15, 18, 20 à 26.
- T27, dossiers de prestations familiales (FAF, MIJ, FAP, MAP) : points 13 à 15, 18, 20 à 22, 27, 28.
- T28, dossiers de pension contrôlés a posteriori : point 29.
- T29, dossiers d'accident du travail contrôlés a posteriori : point 29.
- T30, dossiers de prestations familiales contrôlés a posteriori : point 29.
- T31, prestations indues par branche et par mois : points 1, 6, 17, 30 à 33, 61.
- T32, pensionnés de vieillesse (RPV, RPRA) : points 1, 3, 6, 8, 9, 11, 12, 34 à 36, 43.
- T33, pensionnés d'invalidité (RPI) : points 3, 8, 9, 35.
- T34, pensionnés veufs et veuves (RPSV, RPSF) : points 3, 8, 9, 35, 37.
- T35, pensionnés orphelins (RPSE) : points 3, 8 à 10, 35, 37.
- T36, pensionnés ascendants (RPA) : points 3, 8, 9, 35, 57.
- T37, bénéficiaires de rente viagère (ARI) : points 3, 8, 9, 35, 51.
- T38, crédirentiers veufs et veuves (ARSV, ARSF) : points 3, 8, 9, 35, 51.
- T39, crédirentiers orphelins (ARSE) : points 3, 8 à 10, 35, 51.
- T40, crédirentiers ascendants (ARSA) : points 3, 8, 9, 35, 51.
- T41, allocataires des prestations familiales (FAF) : points 8, 9, 38 à 42.
- T42, nouveaux pensionnés de vieillesse et services passés (RPV, RPRA) : points 4, 9, 11, 16, 34, 44, 45.
- T43, nouveaux pensionnés veufs et veuves (RPSV, RPSF) : points 4, 9, 45.
- T44, nouveaux pensionnés orphelins (RPSE) : points 4, 9, 10, 45.
- T45, nouveaux crédirentiers d'accident du travail (ARI, ARSV, ARSF, ARSE, ARSA) : points 4, 9, 46.
- T46, nouveaux crédirentiers orphelins (ARSE) : points 4, 9, 46.
- T47, nouveaux crédirentiers ascendants (ARSA) : points 4, 9, 47.
- T48, nouveaux bénéficiaires de rente viagère et degré d'incapacité (ARI) : points 4, 9, 48.
- T49, nouveaux crédirentiers veufs et veuves (ARSV, ARSF) : points 4, 9, 46.
- T50, allocations de vieillesse sur cinq ans, avec les services passés (RAV) : points 1, 11, 16, 49, 50.
- T51, allocations de survivants (RAS, RASC, RASO, RASA) : points 1, 49, 50.
- T52, indemnités journalières d'accident du travail (AIJ) : points 1, 49, 50.
- T53, allocations d'incapacité (AAI) : points 1, 49, 50.
- T54, indemnités de maternité (MIJ) : points 1, 49, 50.
- T55, allocations prénatales (FAP, MAP) : points 1, 49, 50.
- T56, prestations servies par branche : points 1, 2, 6, 51 à 53.
- T57, décédés par branche : points 2, 54 à 57.
