# Prestations : règles et questions à valider

Version 3 du 17/11/2026 : la version du 09/11 avec, sur chaque carte, la ligne « Objectif » (ce que le point permet de faire dans le tableau). Mis à jour le 09/11/2026. Base consultée : copie de la base des prestations (schéma USER_DWH) du 02/02/2026, comparée à l'annuaire statistique CNSS 2024 et au recueil des besoins v7 de la CNSS.

Chaque point dit de quoi on parle, où on le voit dans la base, ce qu'on fait aujourd'hui et à qui poser la question, puis pose une seule question. Les noms en majuscules sont ceux des tables et des champs de la base : avec eux, on peut refaire chaque chiffre. Les points 1 à 68 gardent leur numéro du 02/11 ; les points 69 à 137 sont nouveaux (69 points) : ils viennent du recueil des besoins v7 et de la comparaison avec l'annuaire CNSS 2024. Aucune question ne bloque : en attendant la réponse, on applique ce que dit la ligne « Aujourd'hui ». Tous les tableaux T25 à T57 sont repris à la fin avec leurs points, puis les points de chaque direction. S'il n'y a pas le temps de tout voir : les points à trancher en priorité d'abord.

## Pour lire ce document

- Le dossier, table DOSSIER : la demande d'un assuré (DOS_CODE), au nom de son titulaire (IND_ID), l'assuré qui ouvre le droit. Sa branche (TDOS_CODE) : V pensions de vieillesse, d'invalidité et de décès ; A accidents du travail et maladies professionnelles ; F prestations familiales ; M maternité. Son statut (DOS_STATUT) : A actif, F fermé. Sa catégorie (DOS_CATEGORIE) : vide pour un dossier ordinaire, COOR_AC pour une coordination payée par l'autre caisse, COOR_CNSS pour une coordination payée par la CNSS, CNSS_AC sans doute pour un dossier transféré à une autre caisse. Son service, c'est-à-dire son agence (SP_NO). Pour un accident du travail : le décès de la victime le jour de l'accident (DOS_DECES_IMMEDIAT) et la date où l'avis du médecin conseil est reçu (DOS_DATE_REC_AVIS_MC). Sa case « vérifié » (DOS_VERIFIE) n'a pas de sens connu.
- La prestation, table PRESTATION_ESP : un droit ouvert dans un dossier, par exemple une pension de veuve. Elle a un code (TPE_CODE, voir la puce suivante), un statut (PE_STATUT), la personne qui y a droit, l'ayant droit (IND_ID_PREST), et ses dates : début du droit (PE_DATE_EFFET), saisie (PE_DATE_INSERT), notification des droits (PE_DATE_NOTIFICATION), suspension (PE_DATE_SUSPENSION), radiation (PE_DATE_RADIATION), dernière modification (PE_DATE_UPDATE). Elle porte aussi les mois cotisés (PE_MOIS_COTISATION) et, pour une coordination, les mois cotisés dans l'autre caisse (PE_COT_AUTRE), le salaire moyen (PE_SAL_MOYEN) et, pour une rente, le taux d'incapacité de la victime (PE_TAUX_INCAPACITE). La notification imprimée est dans la table NOTIFICATION_PREST, avec son statut (NOT_STATUT) ; les étapes du calcul des droits sont dans la table ETAPE (TEA_CODE = CIP pour le calcul initial).
- Les codes de prestation (TPE_CODE). Pensions : RPV vieillesse, RPRA retraite anticipée, RPI invalidité, RPSV veuve, RPSF veuf, RPSE orphelin, RPA ascendant (le père ou la mère d'un assuré décédé) ; RAV allocation de vieillesse, versée en une fois ; RASC, RASO et RASA, les parts de l'allocation de survivants du conjoint, de l'orphelin et de l'ascendant (RAS, l'ancien code, ne sert plus) ; RRC remboursement de cotisations. Accessoires d'une pension : RBEC bonification pour enfant à charge, RME et RME-COL majorations pour enfant, RCCM contribution aux charges du ménage. Accidents du travail : ARI rente viagère de la victime, ARSV, ARSF, ARSE et ARSA rentes de la veuve, du veuf, de l'orphelin et de l'ascendant, AIJ indemnité journalière, AAI allocation d'incapacité (moins de 15 % d'incapacité). Prestations familiales : FAF allocation familiale, MAP allocation prénatale de la maternité, FAP ancien code des prestations familiales pour l'allocation prénatale, MIJ indemnité journalière de maternité.
- La personne, table INDIVIDU : date de naissance (IND_DATE_NAISSANCE), sexe (IND_SEXE : 1 homme, 2 femme), date de décès (IND_DATE_DECES). La base ne garde pas le jour où un décès a été saisi.
- Le paiement, table DEBOURS : chaque somme due à une personne, qu'on appelle un débours. Son type (DEB_TYPE) : PM pour un paiement ordinaire ; AJ pour un débours d'ajustement, qui verse ou reprend la somme d'une correction de paiement et lui est relié par son numéro (AJ_ID) ; RB pour un remboursement de soins. Son statut (DEB_STATUT) : D payé par un effet, P payé sans effet ; les autres statuts (A, I, N, S, V) ne comptent pas comme des paiements. Son montant (DEB_MONTANT), le mois payé (DEB_DATE_EFFET), la date de saisie (DEB_DATE_INSERT) et le jour où l'argent est versé (DEB_DATE_DECAISSEMENT), souvent vide. Sa case « vérifié » (DEB_VERIFIE) est cochée à la saisie : ce n'est pas un contrôle.
- L'effet de paiement, table EFFET : le titre qui règle ensemble plusieurs débours, avec son statut (EFP_STATUT) et le jour du versement (EFP_DATE_DECAISSEMENT). Son type dit comment l'argent est versé, par exemple BVB pour un virement.
- Le guichet, table RECEPTION_DOSSIER : chaque dépôt, avec sa date (RD_DATE_RECEPTION), son type (RD_TYPE_RECEPTION : NOUVEAU pour un nouveau dossier, ANCIEN pour une demande sur un dossier existant), son statut (RD_STATUT : E, I, L, R ou A, sens à faire préciser) et le service qui l'a reçu (SP_NO). Chaque pièce reçue a un type, par exemple CME pour un certificat médical. Les refus sont dans la table DOSSIER_REJETE, avec la date du refus (DOSRE_DATE_REJET).
- Les corrections de paiement, table AJUSTEMENT : un code (TAJ_CODE : TP trop-perçu, TP-CONV trop-perçu sur une convention entre caisses, MP moins-perçu, RC rappel sur une convention, REM-TP remboursement d'un trop-perçu, AV avance, AV-COVID19 avance COVID, REV-AV reprise d'une avance, MPG mois de pension gratuits, DON-COVID19 don COVID, RR rachat de rente, OI et OE oppositions), un montant (AJ_MONTANT), une date (AJ_DATE_ETABLISSEMENT), un statut (AJ_STATUT : A en cours, C soldé, R sans doute annulé, I sens inconnu) et une remarque libre (AJ_REMARQUE). La table PE_AJ relie un ajustement à une prestation ; aucun trop-perçu n'y est relié.
- Les contrôles a posteriori, table DOSSIERS_CONTROLES : un contrôle de dossier par ligne, avec deux textes libres, la remarque (DC_REMARQUE) et la recommandation (DC_RECOMMANDE). La vue V_BI_CONTROLE_POSTERIORI, une ligne par contrôle, est seulement proposée à la DSI.
- Les services et les lieux de paiement : la table SERVICE_PROVINCIAL donne les 20 services de la CNSS (15 agences et les sièges des 5 directions régionales), avec leur numéro (SP_NO) ; un service inactif a SP_ACTIVE = N. La table LIEU_PAIEMENT donne les lieux où l'on paie, avec leur direction régionale (DR_NO) et leur service (SP_NO). Les directions régionales : DRO Ouagadougou, DRB Bobo-Dioulasso, DRN Ouahigouya (le Nord), DRF Fada N'Gourma, DRD Dédougou.
- La copie de la base et les vues : la BI ne lit pas l'application des prestations, elle calcule sur une copie de sa base (schéma USER_DWH), faite le 02/02/2026. La copie de la base arrêtée fin mars est celle qu'on prendrait chaque fin mars pour calculer l'année d'avant. Une vue est une table de lecture que la DSI prépare pour la BI : V_PRESTATAIRES_DECEDES_BI pour les décédés, V_BI_BALANCE_COMPTABLE pour la comptabilité (les comptes 601, 602, 603 et 605 pour les prestations). La table TSINISTRE, des accidents du travail, n'est plus alimentée depuis décembre 2024.
- Les mots du document : une prestation est liquidée quand ses droits sont calculés et notifiés (la première notification) ; une demande est en instance quand elle est déposée, ni liquidée ni rejetée. La date de saisie est le jour où une prestation ou un paiement est enregistré dans l'application ; le mois payé, le mois qu'un paiement couvre. Un rappel paie d'un coup des mois en retard. Un trop-perçu, ou indu, est une somme payée en trop ; un moins-perçu, une somme payée en moins, rattrapée par un rappel. L'assuré est le travailleur qui a cotisé ; l'ayant droit est la personne qui a droit à la prestation : l'assuré lui-même pour sa pension ou, après sa mort, sa veuve, son orphelin ou son parent ; quand le recueil oppose « assuré » et « ayant droit », l'ayant droit est ce survivant. Une pension directe est celle de l'assuré : vieillesse, retraite anticipée ou invalidité. Une coordination est une pension due au titre de deux caisses, dont une seule paie. Un rentier, que l'Annuaire CIPRES appelle crédirentier, touche une rente d'accident du travail. Les montants sont en FCFA : M veut dire million, Md milliard.
- Les directions à qui l'on pose les questions : DCP, la Direction centrale des prestations ; DP et DR, pour les accidents du travail, avec les directions régionales (DP est un sigle du recueil, à faire préciser) ; DES et DR, pour les prestations familiales, avec les directions régionales (DES est un sigle du recueil, à faire préciser) ; DAI, la Direction de l'audit interne ; DSI, la Direction des systèmes d'information ; DPC, la Direction de la prospective et de la coopération, dont le service Études et Statistique fait l'annuaire ; DCR, la Direction centrale du recouvrement.
- Les documents cités : l'annuaire statistique CNSS 2024, fait par la DPC et publié en juin 2025, dont on cite les tableaux par leur numéro (« le tableau 26 de l'annuaire ») ; le recueil des besoins v7 de janvier 2026, qui dit, tableau par tableau et indicateur par indicateur, ce que la CNSS attend de la BI. On cite aussi l'Annuaire CIPRES, le modèle des tableaux T25 à T57 (leur maquette) avec son glossaire ; l'atelier de Kaya en 2017, où la CNSS a validé ces tableaux ; la grille des ratios de la CNSS (le classeur « Evaluation des Ratios CIPRES 2025 »), dont les feuilles 03 à 06 donnent les formules du délai de liquidation, du taux restant à traiter, du taux de contrôle et du taux d'indus.

## À trancher en priorité

1. L'année d'un paiement
   C'est quoi : un paiement peut payer des mois anciens : un rappel de 15 mois saisi en mars 2024 paie 12 mois de 2023 et 3 mois de 2024.
   Objectif : dans tous les montants payés de T31 à T56, et dans T41, chaque somme doit tomber dans la bonne année et le bon mois. Elle permet de suivre la dépense de chaque régime, la pension moyenne et le taux d'indus (Kaya 2017). La réponse permet de ranger un rappel de 15 mois comme l'annuaire, et de retomber sur ses chiffres publiés.
   Dans la base : dans DEBOURS, le mois payé (DEB_DATE_EFFET), la saisie (DEB_DATE_INSERT) et le versement (DEB_DATE_DECAISSEMENT, souvent vide : voir le point 60).
   Aujourd'hui : on range chaque paiement dans le mois qu'il paie. La réponse vaut pour tous les montants de T31 à T56 et pour T41, qui ne compte que les allocataires payés dans l'année.
   Chiffre : en 2024, la saisie retombe mieux sur l'annuaire CNSS 2024 pour 11 lignes sur 13, dont les allocations de vieillesse : 497,8 M à la saisie, 497,7 M publiés, 517,3 M au mois payé. Le mois payé l'emporte pour la pension de vieillesse, la plus grosse ligne, et de peu la rente viagère. Mois par mois, les montants publiés suivent les paiements saisis.
   À qui : DPC, avec la DCP
   Question : dans l'annuaire, un paiement est-il rangé dans le mois qu'il paie, le mois où il est saisi (rappels compris), ou le mois où il est versé ?
   Réponse :

2. La date d'arrêt de l'annuaire
   C'est quoi : un décès ou une radiation est souvent saisi des mois après : les chiffres d'une année dépendent du jour où la base a été copiée.
   Objectif : dans tous les tableaux T25 à T57, les chiffres d'une année doivent être calculés sur la même copie de la base que l'annuaire. Cela permet de comparer la BI à l'annuaire CNSS sans écart dû aux décès et radiations saisis en retard, et de garder des chiffres qui ne bougent plus (Proposition BI). La réponse permet de fixer le jour de cette copie, par exemple fin mars.
   Dans la base : INDIVIDU, IND_DATE_DECES donne la date du décès, pas celle de sa déclaration. On s'en approche par la dernière modification de la pension (PRESTATION_ESP, PE_DATE_UPDATE).
   Aujourd'hui : on calcule sur la copie de la base du 02/02/2026. Si la réponse est oui, on calculera chaque année sur la copie de la base arrêtée fin mars de l'année suivante.
   Chiffre : environ 444 retraités morts en 2024 semblent déjà saisis fin mars 2025 (leur pension n'a plus été modifiée après), pour 447 au tableau 26 de l'annuaire CNSS 2024 ; la base d'aujourd'hui en compte 619. C'est un indice fort, pas une preuve.
   À qui : DPC
   Question : l'annuaire d'une année est-il fait avec la base arrêtée vers la fin mars de l'année suivante ?
   Réponse :

29. Les contrôles a posteriori
   C'est quoi : un contrôle a posteriori vérifie un dossier après son paiement. T28 à T30 comptent, par trimestre et par branche, les dossiers payés et les dossiers contrôlés ; la norme est d'au moins 5 % de dossiers contrôlés. Le recueil des besoins donne les fichiers Excel de la DAI comme source.
   Objectif : dans T28 à T30, la colonne des dossiers contrôlés doit montrer, par trimestre et par branche, tous les contrôles faits après paiement. Elle permet de calculer le taux de contrôle a posteriori et de le comparer à la norme d'au moins 5 % (Kaya 2017). La réponse permet de savoir où lire les contrôles faits depuis juillet 2025, pour que le taux ne tombe pas à 0,07 %.
   Dans la base : la table DOSSIERS_CONTROLES n'a que 1 542 lignes en tout, la dernière saisie le 09/07/2025.
   Aujourd'hui : T28 à T30 ne sont pas produits. Une vue avec une ligne par contrôle, à remplir depuis cette table ou les fichiers de la DAI, est proposée à la DSI (voir le point 70).
   Chiffre : en 2025, 115 contrôles notés (jusqu'au 9 juillet) pour 155 206 dossiers qui ont reçu au moins un débours (DEBOURS) dans l'année, soit 0,07 %.
   À qui : DAI, avec la DSI
   Question : depuis juillet 2025, les contrôles a posteriori sont-ils notés dans les fichiers Excel de la DAI, ou ailleurs ?
   Réponse :

69. La table des contrôles, pour le passé
   C'est quoi : si la table des contrôles de l'application a un jour contenu tous les contrôles de la DAI, elle peut remplir T28 à T30 pour les années passées.
   Objectif : dans T28 à T30, les années passées doivent montrer les dossiers contrôlés à côté des dossiers payés. Elles permettent de suivre, année après année, si la norme d'au moins 5 % de dossiers contrôlés est tenue (Kaya 2017). La réponse permet de savoir si la table de l'application peut remplir ces années, ou si elle ne garde qu'une partie des contrôles.
   Dans la base : la table DOSSIERS_CONTROLES a 1 542 lignes en tout, de 1 à 212 contrôles par an et par branche ; la dernière saisie date du 09/07/2025.
   Aujourd'hui : on ne s'en sert pas, même pour le passé.
   Chiffre : 5 % des 155 206 dossiers qui ont reçu au moins un débours en 2025, ce serait environ 7 760 contrôles dans l'année.
   À qui : DAI
   Question : cette table a-t-elle un jour contenu tous vos contrôles ?
   Réponse :

70. Les contrôles des cinq dernières années
   C'est quoi : sans la liste des contrôles de la DAI, T28 à T30 restent vides, comme les trois taux de contrôle du recueil des besoins.
   Objectif : dans T28 à T30, chaque case doit montrer le nombre de dossiers contrôlés par branche et par type de prestation, sur cinq ans. Ces chiffres donnent les trois taux de contrôle a posteriori que suivent la DAI, la DCP et les directions régionales (recueil v7, l. 1111, 1190, 1267). La réponse permet de remplir ces tableaux, vides aujourd'hui, avec les vrais contrôles de la DAI.
   Dans la base : la vue proposée à la DSI, V_BI_CONTROLE_POSTERIORI : une ligne par contrôle, avec le dossier, la branche, la prestation contrôlée, la date, le résultat et le montant de l'indu trouvé. Ni nom ni numéro bancaire.
   Aujourd'hui : rien, la vue est seulement proposée.
   À qui : DAI, avec la DSI
   Question : pouvez-vous nous donner vos contrôles des cinq dernières années, un par ligne, sous cette forme ?
   Réponse :

71. La personne comptée pour un survivant
   C'est quoi : pour T32 à T38 et T43 à T49, le recueil des besoins renvoie au tableau des allocataires : « Idem. Tab 41 ». Or l'allocataire de T41 est le titulaire du dossier, c'est-à-dire l'assuré. Lue à la lettre, la phrase ferait compter l'assuré décédé au lieu de sa veuve, de son orphelin ou de son parent.
   Objectif : dans les tableaux de survivants, de T34 à T49, chaque ligne d'âge doit montrer la veuve, l'orphelin ou le parent qui touche la pension ou la rente, à son âge à lui. Elle permet d'établir leur pyramide des âges et leur pension moyenne, pour les évaluations actuarielles (Kaya 2017). La réponse permet de ne jamais compter un survivant à la place, ou à l'âge, de l'assuré décédé.
   Dans la base : l'ayant droit est dans PRESTATION_ESP (IND_ID_PREST), le titulaire dans DOSSIER (IND_ID) : c'est le mari décédé pour 13 104 pensions de veuve sur 25 541.
   Aujourd'hui : on compte l'ayant droit, à son âge (voir le point 8), et on lit « Idem. Tab 41 » comme « même périodicité » : par mois, par trimestre et par année.
   Chiffre : l'annuaire CNSS 2024 fait pareil : son tableau 77 range les nouveaux rentiers selon qui ils sont, pas selon la victime ; 44 orphelins de rente en 2024 dans la base pour 44 publiés.
   À qui : DCP
   Question : pour une pension ou une rente de survivant, on compte bien l'ayant droit lui-même, et non l'assuré décédé titulaire du dossier ?
   Réponse :

14. L'année de chaque étape
   C'est quoi : un dossier peut être déposé une année et liquidé l'année suivante. Il faut savoir dans quel tableau annuel il compte.
   Objectif : dans T25 à T27, les colonnes des dossiers déposés, liquidés, en instance et rejetés doivent montrer chaque étape dans l'année où elle a lieu. Elles permettent de calculer la durée moyenne de liquidation, face à la norme CIPRES de moins de 45 jours, et les dossiers restant à traiter en fin d'exercice (Kaya 2017). La réponse permet qu'aucun dossier déposé en décembre et liquidé en janvier ne disparaisse.
   Dans la base : le dépôt est la réception au guichet (RECEPTION_DOSSIER, RD_DATE_RECEPTION) ; la liquidation, la première notification des droits (PRESTATION_ESP, PE_DATE_NOTIFICATION) ; le rejet, la date du refus (DOSSIER_REJETE, DOSRE_DATE_REJET).
   Aujourd'hui : chaque étape compte dans son année, comme la grille des ratios de la CNSS (feuille 04) : un dossier déposé le 20/11/2024 et notifié le 05/02/2025 est déposé en 2024 et liquidé en 2025. L'autre lecture, ne suivre que les dossiers déposés dans l'année, écarterait les vieilles demandes (voir le point 21), mais un dossier déposé en décembre et liquidé en janvier ne serait liquidé dans aucun tableau.
   À qui : DCP ; DES et DR pour T27
   Question : le tableau de l'année compte-t-il aussi les dossiers déposés les années d'avant (liquidés dans l'année ou encore en instance au 31/12), ou seulement ceux déposés dans l'année ?
   Réponse :

21. Les demandes qui traînent
   C'est quoi : certaines demandes restent en instance des années, sans suite, et gonflent le taux de dossiers restant à traiter.
   Objectif : dans T25 à T27, la colonne des dossiers en instance au 31/12 doit montrer les demandes vraiment en attente de traitement. Elle sert au taux de dossiers restant à traiter et au calcul des prestations à payer (Kaya 2017). La réponse permet de sortir de ce stock les demandes abandonnées depuis des années, qui portent le taux des pensions à environ 70 % pour une norme de 15 %.
   Dans la base : une demande reçue au guichet (RECEPTION_DOSSIER, RD_DATE_RECEPTION), sans notification (PRESTATION_ESP, PE_DATE_NOTIFICATION vide) ni refus (DOSSIER_REJETE).
   Aujourd'hui : on écarte les réceptions d'avant le 01/01/2016 sans dossier et les dossiers fermés (DOS_STATUT = F) jamais notifiés. En accidents du travail et en prestations familiales, une demande sans suite depuis 24 mois n'est plus en instance ; en pensions, aucune limite : le recueil des besoins n'en prévoit pas.
   Chiffre : fin 2025, sans rien écarter, 23 218 demandes de pension seraient en instance. Avec les règles actuelles, leur taux restant à traiter serait encore d'environ 70 % avec la formule du recueil (estimation), pour une norme de 15 %.
   À qui : DCP pour les pensions ; DP et DR pour les accidents du travail ; DES et DR pour les prestations familiales
   Question : une demande restée sans suite sort-elle des dossiers en instance au bout de 24 mois, d'un autre nombre de mois, ou jamais ?
   Réponse :

## Ce qu'on applique, à confirmer

3. Le pensionné au 31 décembre
   C'est quoi : T32 à T41 comptent les bénéficiaires présents au 31 décembre.
   Objectif : dans T32 à T40, l'effectif doit montrer toutes les personnes qui ont une pension ou une rente en cours au 31 décembre, payées ou non. Il permet d'établir la pyramide des âges et la pension moyenne, et d'en suivre l'évolution pour les évaluations actuarielles (Kaya 2017). La réponse permet de garder les pensions payées par une autre caisse et celles qui attendent un certificat de vie.
   Dans la base : une prestation (PRESTATION_ESP) commencée avant le 31/12 (PE_DATE_EFFET), ni radiée ni suspendue ce jour-là (PE_DATE_RADIATION, PE_DATE_SUSPENSION), titulaire en vie (IND_DATE_DECES).
   Aujourd'hui : on compte la pension en cours, payée ou non. T41 fait exception (voir le point 38).
   Chiffre : fin 2024, 31 093 pensionnés de vieillesse dans la base ; l'annuaire CNSS 2024 fait pareil : 30 808 publiés. En exigeant un paiement dans l'année, 27 670 seulement : il manquerait surtout les pensions payées par une autre caisse et les pensions qui attendent un certificat de vie.
   À qui : DCP ; DP et DR pour les rentes
   Question : on garde la règle : un pensionné compte au 31/12 même s'il n'a pas été payé en fin d'année ?
   Réponse :

4. Le nouveau pensionné ou rentier
   C'est quoi : T42 à T49 comptent la première pension (T42 à T44) ou la première rente d'accident du travail (T45 à T49) d'une personne dans l'année.
   Objectif : dans T42 à T49, l'effectif doit montrer les personnes qui reçoivent pour la première fois une pension ou une rente dans l'année. Il permet de définir la loi des entrées, utile aux évaluations actuarielles (Kaya 2017). La réponse permet de fixer le jour qui fait entrer une pension dans l'année, la saisie plutôt que la notification, sans compter une révision comme une nouvelle pension.
   Dans la base : la première prestation de la personne dans le tableau (PRESTATION_ESP), à sa date de saisie (PE_DATE_INSERT). Une révision ou un recalcul ne compte pas.
   Aujourd'hui : on compte à la saisie. Seule réserve : le délai de liquidation se compte à la notification ; une pension saisie en décembre 2024 et notifiée en janvier 2025 entre dans T42 de 2024 et dans le délai de 2025.
   Chiffre : l'annuaire CNSS 2024 fait pareil : 2 463 nouveaux retraités en 2024 dans la base pour 2 489 publiés. À la notification (PE_DATE_NOTIFICATION), on tomberait à 2 397.
   À qui : DCP
   Question : on garde la date de saisie pour compter un nouveau pensionné ou rentier ?
   Réponse :

6. Ce qu'est un paiement
   Objectif : dans T31 et les montants payés de T32 à T56, chaque somme doit montrer l'argent réellement versé aux assurés et aux ayants droit. Elle permet de suivre la dépense par branche et de calculer le taux d'indus, qui rapporte les trop-perçus aux prestations payées (Kaya 2017). La réponse permet d'écarter sans doute possible les sommes annulées ou en suspens, jamais versées.
   Dans la base : un débours de type PM (DEB_TYPE = PM) au statut D, payé par effet, ou P, payé sans effet (DEB_STATUT). On prend le montant du débours (DEB_MONTANT), jamais le net à payer de l'effet, qui se répète sur chaque débours.
   Chiffre : en 2024, 58,73 Md payés dans les trois branches, pour 58,88 Md de prestations servies dans l'annuaire CNSS 2024 (−0,26 %). Les pensions sont à 1,3 % près ; l'annuaire publie plus pour les prestations familiales et les accidents du travail : le total ne tombe juste que parce que ces écarts se compensent (voir le point 51).
   À qui : DSI, avec la DCP
   Question : les débours aux autres statuts (A, I, N, S, V) ne sont jamais des paiements versés ?
   Réponse :

7. Les branches
   C'est quoi : la CNSS a trois branches : les pensions, les risques professionnels et les prestations familiales.
   Objectif : dans T25 à T31 et dans tous les montants par branche, chaque chiffre doit tomber dans la bonne branche : pensions, risques professionnels ou prestations familiales, maternité comprise. Cela permet de suivre chaque régime à part et de calculer ses ratios, par exemple son taux d'indus (Kaya 2017). La réponse permet de ranger la maternité au même endroit que l'annuaire CNSS 2024.
   Dans la base : la branche du dossier (DOSSIER, TDOS_CODE) : V pensions, A risques professionnels, F prestations familiales, M maternité, qui va avec les prestations familiales. Un trop-perçu prend la branche de son dossier.
   Chiffre : l'annuaire CNSS 2024 fait pareil : ses 11,18 Md de prestations familiales de 2024 comprennent la maternité.
   À qui : DCP
   Question : on garde ce rangement, avec la maternité dans les prestations familiales ?
   Réponse :

8. La personne comptée
   C'est quoi : pour un orphelin, c'est son tuteur qui est payé, mais c'est l'orphelin qu'on compte, à son âge à lui.
   Objectif : dans les tableaux de bénéficiaires, T32 à T49, l'effectif doit montrer des personnes, chacune comptée une fois par tableau. Il permet d'établir la pyramide des âges et une pension moyenne par personne (Kaya 2017). La réponse permet qu'une pension révisée, qui laisse deux pensions en cours, ne fasse pas deux pensionnés, et qu'on n'additionne jamais deux tableaux.
   Dans la base : on compte l'ayant droit (PRESTATION_ESP, IND_ID_PREST) une fois par tableau. Une veuve retraitée compte dans T34 et dans T32 : on n'additionne jamais deux tableaux.
   Aujourd'hui : une pension révisée qui laisse deux pensions en cours au 31/12 compte une personne. Pour T41, l'allocataire est le titulaire du dossier (voir le point 39).
   Chiffre : fin 2024, 31 267 pensions de vieillesse et de retraite anticipée en cours pour 31 093 personnes. L'annuaire CNSS 2024 fait pareil : en comptant les personnes, on retombe mieux sur ses 30 808 retraités.
   À qui : DCP
   Question : on garde cette règle : une personne compte une seule fois par tableau, même avec deux prestations du même tableau ?
   Réponse :

9. L'âge et les naissances en 1900
   C'est quoi : l'âge est l'année du tableau moins l'année de naissance, comme dans le glossaire de l'Annuaire CIPRES.
   Objectif : dans T32 à T49, les lignes d'âge doivent montrer chaque bénéficiaire dans sa vraie tranche de 5 ans. Elles permettent d'établir la pyramide des âges, utile aux évaluations actuarielles (Kaya 2017). La réponse permet de décider où mettre les personnes « nées en 1900 », surtout des veuves, qui gonflent aujourd'hui la tranche « 100 ans et plus ».
   Dans la base : INDIVIDU, IND_DATE_NAISSANCE. L'année 1900 est une date mise par défaut à la saisie : 503 personnes en 2024, dont 499 veuves, qui ont donc 120 ans et plus.
   Aujourd'hui : on les range dans « 100 ans et plus » : la maquette CIPRES n'a pas de ligne « âge inconnu ».
   Chiffre : l'annuaire CNSS 2024 fait pareil : 679 femmes de 100 ans et plus parmi les veuves et les ascendants en 2024 ; on en trouve 635 avec ces dates, 135 sans elles.
   À qui : DCP
   Question : on garde les personnes « nées en 1900 » dans la tranche « 100 ans et plus » ?
   Réponse :

10. L'âge limite des orphelins
   C'est quoi : le recueil des besoins de la CNSS ne compte dans T35 que les orphelins de moins de 25 ans.
   Objectif : dans T35, les lignes d'âge doivent montrer les orphelins pensionnés au 31 décembre, sans tranche « 25 ans et plus » (recueil v7, l. 283). Elles permettent d'établir leur pyramide des âges et leur pension moyenne (Kaya 2017). La réponse permet de savoir si un orphelin de 25 ans ou plus peut encore entrer dans T44 comme nouveau pensionné.
   Dans la base : les pensions d'orphelin (PRESTATION_ESP, code RPSE) et l'âge (IND_DATE_NAISSANCE).
   Aujourd'hui : au 31/12, T35 s'arrête à 24 ans. Pour les nouveaux orphelins de l'année (T44, T46), pas de limite : un orphelin de 25 ans ou plus va dans « 20 ans et plus ».
   Chiffre : l'annuaire CNSS 2024 fait pareil au 31/12 : ses 9 478 orphelins ont tous moins de 25 ans. En 2024, 2 nouveaux orphelins pensionnés (T44) ont de 25 à 29 ans.
   À qui : DCP
   Question : un orphelin de 25 ans ou plus peut-il recevoir une nouvelle pension d'orphelin ?
   Réponse :

11. La coordination
   Objectif : dans T32 et T42, le montant et les services passés d'une pension de coordination doivent montrer la seule part de la CNSS (recueil v7, l. 263 et 298). Ils servent à la pension moyenne et aux évaluations actuarielles, sans mêler l'autre caisse (Kaya 2017). La réponse permet de retirer les bons mois, ceux cotisés dans l'autre caisse.
   Dans la base : la catégorie du dossier (DOS_CATEGORIE) vaut COOR_AC quand l'autre caisse paie, COOR_CNSS quand la CNSS paie. Les mois cotisés sont dans PE_MOIS_COTISATION, ceux de l'autre caisse dans PE_COT_AUTRE.
   Aujourd'hui : les pensions COOR_AC comptent dans l'effectif, sans montant, comme dans l'annuaire CNSS 2024. Pour les services passés, on garde PE_MOIS_COTISATION moins PE_COT_AUTRE, comme l'a décidé l'atelier de Kaya en 2017.
   Chiffre : parmi les nouveaux retraités de 2024, PE_COT_AUTRE est rempli pour les 234 COOR_AC et pour 118 des 123 COOR_CNSS (révisions comprises) ; il est en moyenne plus petit que le total des mois.
   À qui : DCP, avec la DSI
   Question : PE_COT_AUTRE donne-t-il bien les mois cotisés dans l'autre caisse, déjà compris dans le total des mois ?
   Réponse :

12. Les accessoires de pension
   C'est quoi : la bonification pour enfant à charge, les majorations pour enfant et la contribution aux charges du ménage s'ajoutent à une pension.
   Objectif : dans T32, le montant doit montrer la pension seule, sans la bonification, les majorations pour enfant ni la contribution aux charges du ménage. Il permet de suivre la pension moyenne et, d'une année à l'autre, le maintien du pouvoir d'achat des pensionnés (Kaya 2017). La réponse permet de garder ces accessoires hors de cette moyenne, sur une ligne à part, comme l'annuaire CNSS 2024.
   Dans la base : PRESTATION_ESP, codes RBEC, RME et RME-COL, RCCM.
   Aujourd'hui : elles ne font pas un pensionné de plus. Leur argent n'entre pas dans le montant de T32 ; il peut aller sur une ligne à part.
   Chiffre : l'annuaire CNSS 2024 fait pareil : il publie la majoration pour enfant (537,3 M en 2024) à part, hors de la pension de retraite (38,24 Md).
   À qui : DCP
   Question : on garde l'argent des accessoires hors du montant de T32 ?
   Réponse :

13. Les dossiers (T25 à T27)
   C'est quoi : T25 (pensions), T26 (accidents du travail) et T27 (prestations familiales, maternité comprise) comptent des demandes : déposées, liquidées, en instance au 31/12 ou rejetées. Un dossier peut en porter plusieurs, par exemple la pension d'un retraité puis celle de sa veuve.
   Objectif : dans T25 à T27, chaque ligne doit montrer les demandes déposées, liquidées, en instance et rejetées, avec la durée de liquidation : moins de 15 jours, 15 à 45, plus de 45. Elle permet de calculer la durée moyenne par type de prestation et de la comparer à la norme CIPRES de moins de 45 jours (Kaya 2017). La réponse permet de valider ces règles de comptage.
   Dans la base : une demande est déposée à sa réception (RECEPTION_DOSSIER, RD_DATE_RECEPTION), liquidée à la première notification (PRESTATION_ESP, PE_DATE_NOTIFICATION), rejetée si elle est dans DOSSIER_REJETE, sinon en instance.
   Aujourd'hui : la durée compte les jours de la réception à la notification : moins de 15 jours, de 15 à 45, plus de 45. La norme CIPRES est une durée moyenne de moins de 45 jours.
   Chiffre : l'annuaire CNSS 2024 fait pareil : 831 personnes liquidées en 2024 pour une allocation de vieillesse (RAV), pour 833 publiées (tableau 24).
   À qui : DCP ; DP et DR ; DES et DR
   Question : on garde ces règles ?
   Réponse :

15. La formule du taux restant à traiter
   C'est quoi : la part des dossiers encore en attente à la fin de la période, pour une norme de 15 % au plus. Le recueil des besoins divise les dossiers en instance à la fin par les dossiers complets reçus dans la période. La grille des ratios de la CNSS (feuille 04) les divise par les dossiers en instance au début, plus les reçus, moins les rejetés.
   Objectif : dans les indicateurs de chaque branche, le taux de dossiers restant à traiter doit montrer la part des demandes encore en attente en fin de période. Il permet aux directions de suivre le retard de traitement (recueil v7, l. 1104, 1180, 1239). La réponse permet de choisir la formule : environ 70 % ou 40 % pour les pensions, face à une norme de 15 %.
   Aujourd'hui : on prend la formule du recueil, la source la plus forte.
   Chiffre : pour les pensions fin 2025 (estimation) : environ 70 % avec la formule du recueil, de l'ordre de 40 % avec celle de la grille. Hors norme dans les deux cas.
   À qui : DAI, avec la DCP
   Question : pour le taux de dossiers restant à traiter, on retient la formule du recueil ou celle de votre feuille 04 ?
   Réponse :

16. Les services passés
   C'est quoi : le total des mois de travail salarié cotisés avant la pension (T42 ; T50 pour les allocations de vieillesse).
   Objectif : dans T42 et T50, la colonne des services passés doit montrer les mois de travail salarié cotisés à la CNSS avant la pension (recueil v7, l. 296 et 319). Elle permet de suivre la durée des carrières des nouveaux retraités et sert aux évaluations actuarielles (Kaya 2017). La réponse permet de valider ce calcul, sans les mois de l'autre caisse ni les carrières de plus de 50 ans.
   Dans la base : les mois cotisés (PE_MOIS_COTISATION) de la dernière pension de la personne saisie dans l'année (pour T50, de sa dernière allocation de vieillesse liquidée dans l'année), écartés au-delà de 600 mois (50 ans). Pour une coordination, on retire les mois de l'autre caisse (voir le point 11).
   Chiffre : le champ n'est jamais vide de 2020 à 2025. Médiane de 270 mois pour les nouveaux retraités de 2024 hors coordination ; 95,5 mois en moyenne (part CNSS) pour les 831 allocataires de vieillesse de 2024.
   À qui : DCP
   Question : on garde ce calcul des services passés ?
   Réponse :

17. Les indus (T31)
   C'est quoi : T31 montre, mois par mois et par branche, les trop-perçus à côté des prestations payées. Les moins-perçus ne sont pas des indus (atelier de Kaya en 2017).
   Objectif : dans T31, la colonne des indus doit montrer, mois par mois et par branche, les trop-perçus à côté des prestations payées. Elle permet de calculer le taux de prestations indues et de le comparer à la norme de 2 % au plus (Kaya 2017). La réponse permet de fixer le mois où compte chaque trop-perçu, et de garder les moins-perçus à part.
   Dans la base : dans AJUSTEMENT, trop-perçus TP ou TP-CONV (TAJ_CODE), moins-perçus MP ou RC, montant AJ_MONTANT, branche du dossier (TDOS_CODE), la maternité allant avec les prestations familiales. Les autres codes et le statut R (AJ_STATUT) sont écartés.
   Aujourd'hui : chacun compte une fois, le mois où il est établi (AJ_DATE_ETABLISSEMENT) : la base ne garde pas les mois payés en trop. Un trop-perçu établi en novembre 2024 et repris jusqu'en juin 2025 compte en novembre 2024. Les moins-perçus sont montrés à part (voir le point 91).
   Chiffre : en 2024, 684 trop-perçus pour 229,5 M, et 414,4 M de moins-perçus.
   À qui : DCP
   Question : on garde ces règles, avec chaque trop-perçu et chaque moins-perçu compté le mois où il est établi ?
   Réponse :

18. Les codes et les lignes CIPRES
   Objectif : dans tous les tableaux T25 à T57, chaque ligne doit montrer les bonnes prestations, par exemple la retraite anticipée avec la vieillesse et les ascendants dans les allocations de survivants. Elle permet de comparer les chiffres, ligne par ligne, avec l'annuaire CNSS et d'une année à l'autre (Proposition BI). La réponse permet de corriger un code mal rangé avant de produire les tableaux.
   Dans la base : chaque prestation a un code (TPE_CODE), par exemple RPV pour la vieillesse ; la liste de chaque tableau est à la fin du document.
   Aujourd'hui : la retraite anticipée (RPRA) va avec la vieillesse : sur une ligne à part dans T32 (voir le point 34), avec les nouveaux retraités dans T42 (voir le point 116). Les accessoires de pension ne font pas un pensionné de plus. T51 compte aussi les ascendants (RASA), T55 l'allocation prénatale (MAP et l'ancien code FAP).
   Chiffre : l'annuaire CNSS 2024 fait pareil, avec une ligne par code : fin 2024, 31 093 pensionnés de vieillesse et de retraite anticipée dans la base, 30 808 publiés.
   À qui : DCP
   Question : la liste des codes de chaque tableau, à la fin du document, est-elle juste ?
   Réponse :

72. La ligne Total
   C'est quoi : la dernière ligne de T25, T26 et T27.
   Objectif : dans T25, T26 et T27, la ligne Total doit montrer toutes les demandes de la branche, même celles sans ligne, avec leur vraie durée moyenne. Elle permet de juger la branche entière : durée face à la norme CIPRES de moins de 45 jours et dossiers restant à traiter (Proposition BI). La réponse permet de garder une durée recalculée sur toutes les demandes, et non une moyenne des lignes.
   Aujourd'hui : le Total additionne toutes les demandes, y compris celles qui n'ont pas de ligne, par exemple une déclaration d'accident sans suite (voir le point 23). Sa durée moyenne est recalculée sur toutes les demandes ; elle n'est pas la moyenne des lignes.
   À qui : DCP ; DP et DR ; DES et DR
   Question : on garde ce calcul de la ligne Total ?
   Réponse :

73. T26, T39 et T40 sans ligne du recueil
   C'est quoi : le recueil des besoins ne dit rien de T26 (dossiers d'accident du travail), ni de T39 et T40 (orphelins et ascendants qui touchent une rente d'accident du travail). L'atelier de Kaya en 2017 ne les examinait pas.
   Objectif : dans T26, T39 et T40, les chiffres doivent montrer les dossiers d'accident du travail et les orphelins et ascendants rentiers d'un accident du travail. T26 compare la durée de liquidation à la norme de moins de 45 jours ; T39 et T40 donnent la pyramide des âges et la rente moyenne (Annuaire CIPRES, p. 12 et 14). La réponse permet de les produire avec les règles de leurs voisins.
   Aujourd'hui : on les produit comme l'Annuaire CIPRES les décrit, avec les règles de leurs voisins : T26 comme T25 et T27 ; T39 et T40 comme T38, en disant « rentiers » au lieu de « crédirentiers ». La même question est posée pour T56 (voir le point 124).
   À qui : DP et DR, avec la DCP
   Question : on produit T26, T39 et T40 de cette façon ?
   Réponse :

## Questions par tableau

T25 à T27, les dossiers

5. La pension de la veuve (T25)
   C'est quoi : quand un retraité décède, sa veuve et ses orphelins reçoivent une pension : la réversion.
   Objectif : dans T25, la ligne des pensions de survivant doit montrer les demandes de réversion reçues et liquidées dans l'année, et leur délai. Elle permet de comparer ce délai à la norme CIPRES de moins de 45 jours et de chiffrer les dossiers restant à traiter (Kaya 2017). La réponse permet de compter la veuve de la même façon, que sa pension soit dans un nouveau dossier ou celui du défunt.
   Dans la base : la pension de la veuve (RPSV ou RPSF, PRESTATION_ESP) est ouverte dans un nouveau dossier ou ajoutée au dossier du défunt. La moitié des pensions de veuve sont dans le dossier du défunt.
   Aujourd'hui : dans T25, la réversion compte comme une demande à part sur la ligne des survivants, dans les deux cas. Une veuve et ses orphelins notifiés ensemble dans le même dossier font une seule demande.
   Chiffre : l'annuaire CNSS 2024 compte autrement : des personnes, 1 542 nouveaux survivants en 2024 (tableau 23). La ligne des survivants de T25 sera plus basse.
   À qui : DCP
   Question : la réversion compte-t-elle comme une demande à part, qu'elle soit dans un nouveau dossier ou dans celui du défunt ?
   Réponse :

19. La coordination dans T25 à T27
   C'est quoi : un dossier de coordination dont la pension est payée par l'autre caisse est quand même reçu et étudié par la CNSS.
   Objectif : dans T25 à T27, les dossiers déposés et liquidés doivent montrer tout le travail de la CNSS, y compris les dossiers de coordination qu'elle reçoit et étudie pour une autre caisse. Ils servent au délai moyen de liquidation et au nombre de dossiers restant à traiter (Kaya 2017). La réponse permet de garder ou d'écarter ces dossiers, environ 6 à 8 % de T25.
   Dans la base : la catégorie du dossier (DOSSIER, DOS_CATEGORIE) vaut COOR_AC : 3 326 dossiers, dont 76 % reçus au guichet de la CNSS. Les dossiers transférés à une autre caisse (CNSS_AC, sens à confirmer) sont écartés.
   Aujourd'hui : on compte les dossiers COOR_AC dans les déposés et les liquidés de T25 à T27.
   Chiffre : 238 dépôts et 292 liquidations de pension en 2025, soit 6 à 8 % de T25. L'annuaire CNSS 2024 compte ces pensions dans ses pensionnés.
   À qui : DCP
   Question : un dossier de coordination, payé par une autre caisse mais reçu et étudié par la CNSS, compte-t-il dans T25 à T27 ?
   Réponse :

20. La date du dépôt
   C'est quoi : le dépôt d'un dossier, c'est sa réception au guichet. L'atelier de Kaya en 2017 demandait de ne plus recevoir de dossiers incomplets.
   Objectif : dans T25 à T27, les dossiers déposés et les durées de liquidation doivent partir du jour où le dossier complet arrive au guichet. Le délai moyen qui en sort se compare à la norme CIPRES de moins de 45 jours (recueil v7, l. 1097 et 1224). La réponse permet de savoir si la date de réception mesure bien ce délai, ou si l'attente des pièces manquantes y est comptée.
   Dans la base : la date de réception (RECEPTION_DOSSIER, RD_DATE_RECEPTION).
   Aujourd'hui : on date le dépôt du jour de la réception, comme si le dossier était complet ce jour-là. Une demande sans réception prend sa date de demande.
   Chiffre : 4 671 réceptions de pension en 2025 ; 96 % des pièces sont saisies le jour de la réception.
   À qui : DCP ; DP et DR ; DES et DR
   Question : la date de réception au guichet (RD_DATE_RECEPTION) est-elle celle du dossier complet ?
   Réponse :

22. Les refus
   C'est quoi : un dossier refusé n'est plus en instance et n'a pas de durée de liquidation.
   Objectif : dans T25 à T27, la colonne des dossiers rejetés doit montrer les refus de l'année, et celle des dossiers en instance les seuls dossiers qui attendent encore. Le nombre de dossiers restant à traiter en fin d'exercice entre dans le calcul des prestations à payer (Kaya 2017). La réponse permet de savoir si des refus non saisis gonflent les dossiers en instance.
   Dans la base : le module des rejets (DOSSIER_REJETE), l'année du rejet (DOSRE_DATE_REJET). Ni le statut de la réception, ni la fermeture du dossier ne valent refus.
   Aujourd'hui : un refus non saisi dans le module reste en instance. L'atelier de Kaya en 2017 notait déjà que le module n'est pas exploité.
   Chiffre : 14 rejets de pension en 2025 pour 4 671 réceptions ; 21 en prestations familiales en 2025 pour 14 306 réceptions.
   À qui : DCP, avec la DSI
   Question : tous les refus de dossier sont-ils saisis dans le module des rejets de l'application ?
   Réponse :

23. La déclaration d'accident sans suite (T26)
   C'est quoi : après un accident du travail, l'employeur le déclare. Il s'ensuit, ou non, un arrêt payé en indemnités journalières, des soins, puis une rente.
   Objectif : dans T26, le Total des dossiers déposés doit montrer toutes les déclarations d'accident reçues dans l'année, même celles restées sans suite. Il sert à chiffrer les dossiers restant à traiter en fin d'exercice, qui entrent dans le calcul des prestations à payer (Annuaire CIPRES, p. 12). La réponse permet de décider si ces déclarations, plus de la moitié en 2024, comptent comme des dossiers en attente.
   Dans la base : un dossier d'accident du travail (DOSSIER, TDOS_CODE = A) sans aucune prestation ensuite (PRESTATION_ESP) ni remboursement de soins (DEBOURS).
   Aujourd'hui : la déclaration compte dans les déposés de T26, sur la seule ligne Total ; elle reste en instance 24 mois au plus.
   Chiffre : 629 des 1 189 dossiers d'accident du travail ouverts en 2024 n'ont rien donné. L'annuaire CNSS 2024 compte aussi toutes les déclarations : 1 171 en 2024 (tableau 47).
   À qui : DP et DR
   Question : une déclaration d'accident qui n'a donné ni arrêt, ni soins, ni rente compte-t-elle comme un dossier déposé, en attente de traitement ?
   Réponse :

24. Les soins (T26)
   C'est quoi : la CNSS rembourse les frais médicaux d'une victime d'accident du travail : ce sont les soins.
   Objectif : dans T26, la ligne « Autres prestations de la branche » doit montrer les dossiers de soins des victimes d'accident du travail, déposés et liquidés dans l'année. Elle permet de suivre le délai de liquidation des remboursements de soins, indicateur de la DP et des DR (recueil v7, l. 1157). La réponse permet de décider si ces dossiers entrent dans T26, et sur quelle ligne.
   Dans la base : chaque remboursement de soins est un paiement (DEBOURS) du dossier d'accident du travail, saisi sans passer par le guichet.
   Aujourd'hui : un dossier avec des soins fait une demande sur la ligne « Autres prestations de la branche » de T26, liquidée à la saisie du premier remboursement, et non à son approbation : en 2024, 973 vieux dossiers ont été approuvés d'un coup.
   Chiffre : environ 800 dossiers par an ont un remboursement de soins, hors 2024. L'annuaire CNSS 2024 compte autrement : 139,7 M de soins publiés en 2024, contre 18,8 M dans la base. Des dossiers de soins peuvent donc manquer à T26.
   À qui : DP et DR
   Question : les remboursements de soins des accidents du travail entrent-ils dans T26, sur la ligne « Autres prestations de la branche » ?
   Réponse :

25. La durée d'une rente (T26)
   C'est quoi : une rente viagère est versée à vie à la victime d'un accident quand sa blessure est stabilisée (la consolidation). Un certificat médical de consolidation le constate, puis la victime dépose sa demande au guichet.
   Objectif : dans T26, la ligne des rentes viagères doit montrer en combien de jours une victime d'accident obtient sa rente. Elle permet de comparer ce délai à la norme CIPRES de moins de 45 jours (Annuaire CIPRES, p. 12). La réponse permet de choisir le point de départ du délai : la demande au guichet, ou le certificat de consolidation, qui allonge nettement la durée.
   Aujourd'hui : la durée part de la demande au guichet, en attendant. Le recueil des besoins et l'atelier de Kaya en 2017 la font partir du certificat.
   Chiffre : en 2025, une rente est liquidée en 14 jours (médiane) depuis la demande, en 64 jours depuis le certificat. Le certificat manque pour 15 des 43 rentes viagères liquidées en 2025.
   À qui : DP et DR
   Question : la durée d'une rente viagère part-elle de la réception du certificat de consolidation, ou de la demande au guichet ?
   Réponse :

74. Le délai des rentes de survivants (T26)
   C'est quoi : le recueil des besoins demande le délai des « rentes de survivants » depuis la réception du certificat de consolidation. Or ce certificat concerne la victime blessée, pas sa veuve ni ses orphelins.
   Objectif : dans T26, l'indicateur du délai des rentes doit partir d'une date qui existe pour chaque rente comptée. Il suit la rapidité de liquidation des rentes, par direction régionale et par agence (recueil v7, l. 1170). La réponse permet de savoir s'il vise la rente de la victime, mesurée depuis le certificat de consolidation, ou celle des survivants, mesurée depuis leur demande.
   Aujourd'hui : une rente de survivant part de la demande du survivant au guichet, avec le certificat de décès.
   Chiffre : le certificat de consolidation n'est rempli que pour 1 rente de survivant sur 13 liquidées en 2025.
   À qui : DP et DR
   Question : le délai des « rentes de survivants » du recueil vise-t-il en fait la rente viagère de la victime ?
   Réponse :

26. L'attente des indemnités journalières (T26)
   C'est quoi : l'indemnité journalière remplace le salaire pendant l'arrêt de travail qui suit un accident.
   Objectif : dans T26, la ligne des indemnités journalières doit montrer en combien de jours la victime est payée de son arrêt de travail. Elle permet de comparer ce délai à la norme CIPRES de moins de 45 jours (Annuaire CIPRES, p. 12). La réponse permet de savoir ce qu'on attend après l'arrêt, donc d'où faire partir le délai, ou d'expliquer pourquoi il dépasse la norme.
   Dans la base : ni la reprise du travail, ni le certificat de guérison ne sont saisis.
   Aujourd'hui : la durée de liquidation est comptée depuis la déclaration de l'accident et publiée telle quelle.
   Chiffre : en 2025, en médiane, l'indemnité est notifiée 211 jours (environ 7 mois) après la fin de l'arrêt et 251 jours après la déclaration.
   À qui : DP et DR
   Question : entre la fin de l'arrêt et la notification, attend-on le certificat de guérison, l'attestation de salaire de l'employeur, ou autre chose ?
   Réponse :

27. L'enfant ajouté (T27)
   C'est quoi : quand un allocataire déclare un nouvel enfant, l'allocation est ajoutée à son dossier, sans passer au guichet.
   Objectif : dans T27, la ligne des allocations familiales doit montrer le vrai travail de liquidation de l'année. Elle permet de calculer le délai moyen, comparé à la norme CIPRES de moins de 45 jours, et les dossiers restant à traiter (Kaya 2017). La réponse permet de décider si un enfant ajouté compte comme un dossier de plus, ce qui double presque la ligne.
   Dans la base : une allocation familiale de plus (code FAF, PRESTATION_ESP) sur le dossier, sans nouvelle réception au guichet (RECEPTION_DOSSIER).
   Aujourd'hui : chaque enfant ajouté compte comme une demande de plus, déposée à la date de demande de son allocation et liquidée à sa notification.
   Chiffre : sans les enfants ajoutés, 8 246 allocations familiales liquidées en 2025 ; avec eux, environ 16 400.
   À qui : DES et DR
   Question : quand un allocataire déclare un nouvel enfant, est-ce un dossier déposé et liquidé de plus dans T27 ?
   Réponse :

28. La grossesse sur deux lignes (T27)
   C'est quoi : un dossier de maternité porte l'indemnité journalière de maternité et l'allocation prénatale, notifiées ensemble.
   Objectif : dans T27, les lignes des indemnités de maternité et des allocations prénatales doivent montrer les dossiers de l'année sans gonfler le Total. Elles permettent de suivre le délai de liquidation par type de prestation, indemnité de maternité comprise (recueil v7, l. 1102). La réponse permet de compter une grossesse une fois ou deux fois dans le Total de T27.
   Dans la base : une prestation MIJ et une prestation MAP (PRESTATION_ESP) dans le même dossier de maternité (TDOS_CODE = M) : 99,4 % des dossiers de maternité portent les deux.
   Aujourd'hui : la grossesse compte sur les deux lignes de T27. Dans l'indicateur de délai des prestations familiales, elle ne compte qu'une fois, par l'indemnité, comme le veut le recueil des besoins.
   Chiffre : 4 273 indemnités de maternité et 4 245 allocations prénatales liquidées en 2025 : sur une seule ligne, le Total de T27 baisserait d'environ 4 200.
   À qui : DES et DR
   Question : une grossesse compte-t-elle sur les deux lignes de T27, ou une seule fois ?
   Réponse :

75. Le délai sur la première demande
   C'est quoi : les indicateurs de délai du recueil des besoins donnent le nombre moyen de jours pour liquider un dossier. La grille des ratios de la CNSS (feuille 03) ne le mesure que sur la première demande : nouvel allocataire, première demande de rente ou de pension.
   Objectif : dans T25 à T27 et les indicateurs de délai, la durée moyenne de liquidation doit montrer combien de temps un assuré attend ses droits. Elle se compare à la norme CIPRES de moins de 45 jours (recueil v7, l. 1094 et 1217). La réponse permet de mesurer ce délai sur les seules premières demandes, comme la feuille 03 de la CNSS, ou sur toutes les demandes liquidées.
   Aujourd'hui : on mesure le délai sur toutes les demandes liquidées, enfants ajoutés et indemnités journalières compris.
   À qui : DCP ; DP et DR ; DES et DR
   Question : pour les indicateurs de délai, mesure-t-on seulement la première demande, comme votre feuille 03 ?
   Réponse :

76. Les départs à la retraite de l'annuaire (T25)
   C'est quoi : l'annuaire CNSS 2024 ne publie aucun chiffre de dossiers déposés. Son tableau 27, « départs à la retraite », pourrait en être un.
   Objectif : dans T25, les dossiers de pension déposés doivent pouvoir se vérifier contre un chiffre publié par la CNSS, car l'annuaire CNSS 2024 n'en publie aucun. Ils servent au calcul des dossiers restant à traiter en fin d'exercice (Kaya 2017). La réponse permet de savoir si le tableau 27 de l'annuaire, « départs à la retraite », peut servir à ce contrôle.
   Aujourd'hui : on n'en tire rien, mais on y comparera les demandes de pension directe reçues en 2024, sans l'allocation de vieillesse.
   Chiffre : 3 729 départs publiés en 2024, soit 12,3 % de plus que les 3 322 entrées publiées (nouveaux retraités et allocations de vieillesse).
   À qui : DPC
   Question : le tableau 27 de l'annuaire, « départs à la retraite » (3 729 en 2024), compte-t-il les demandes de pension reçues au guichet ?
   Réponse :

77. La maquette d'origine pour la CIPRES
   C'est quoi : la maquette d'un tableau CIPRES, ce sont ses lignes et ses colonnes telles que l'Annuaire CIPRES les dessine. Le recueil des besoins en change certaines : une ligne « Pension de retraite anticipée » dans T25, « Frais médicaux de maternité » au lieu de « Autres prestations de la branche » dans T27, des lignes de T28 à T30, « TOTAL » au lieu de « Autres » dans T57. Rien ne dit si la CIPRES l'accepte.
   Objectif : dans T25, T27, T28 à T30 et T57, les lignes doivent montrer les tableaux tels que la CNSS les veut et tels que la CIPRES les attend. Garder les deux versions permet de répondre à la CIPRES sur sa propre grille sans perdre le détail voulu par la CNSS (Proposition BI). La réponse permet de savoir s'il faut aussi livrer la maquette d'origine de l'Annuaire CIPRES.
   Aujourd'hui : on produit la version du recueil. La maquette d'origine se refait en additionnant les lignes, par exemple la retraite anticipée avec la vieillesse.
   À qui : DCP, avec la DPC
   Question : pour la CIPRES, faut-il aussi garder la maquette d'origine de ces tableaux ?
   Réponse :

78. Le rachat de rente (T26)
   C'est quoi : un rachat de rente est un capital versé en une fois à la victime d'un accident du travail, à la place de sa rente. T26 compte les dossiers d'accident du travail déposés, liquidés et en instance.
   Objectif : dans T26, la colonne des dossiers liquidés doit montrer toutes les décisions de la branche des accidents du travail rendues dans l'année. Elle permet de calculer le délai moyen et les dossiers restant à traiter (Annuaire CIPRES, p. 12). La réponse permet de décider si un rachat de rente, un capital versé à la place d'une rente, compte comme un dossier liquidé.
   Dans la base : ce n'est pas une prestation (PRESTATION_ESP), mais un débours d'ajustement (DEBOURS, DEB_TYPE = AJ) lié à un ajustement de code RR (AJUSTEMENT, TAJ_CODE = RR).
   Aujourd'hui : il ne compte pas comme un dossier liquidé de T26. Pour T56, voir le point 52.
   Chiffre : 11 rachats versés en 2024 dans la base, pour 21,5 M ; l'annuaire CNSS 2024 en publie 25,4 M (tableau 90), comme une prestation à part.
   À qui : DP et DR
   Question : un rachat de rente compte-t-il comme un dossier liquidé dans T26 ?
   Réponse :

T28 à T30, les contrôles a posteriori

79. Ce que vise un contrôle
   C'est quoi : un dossier peut porter plusieurs prestations, par exemple la pension du défunt et celle de sa veuve. T28 à T30 rangent chaque contrôle sur la ligne d'un type de prestation.
   Objectif : dans T28 à T30, chaque ligne doit montrer, par type de prestation, combien de dossiers payés ont été vérifiés après paiement. Elle permet de calculer le ratio de contrôle a posteriori, dont la norme est d'au moins 5 % (Kaya 2017). La réponse permet de ranger un contrôle sur la bonne ligne quand le dossier porte plusieurs prestations.
   Dans la base : DOSSIERS_CONTROLES a une colonne pour le code de la prestation contrôlée ; on ne sait pas encore si elle est remplie.
   Aujourd'hui : le contrôle va sur la ligne de la prestation contrôlée.
   Chiffre : un ordre de grandeur, les années diffèrent : 62 231 pensions au 31/12/2024 dans l'annuaire CNSS 2024 (tableaux 18 et 22), 47 732 dossiers de pension (TDOS_CODE = V) qui ont reçu un débours en 2025 dans la base. Une pension n'est pas un dossier : la veuve et les orphelins sont souvent payés sur le dossier du défunt.
   À qui : DAI
   Question : un contrôle porte-t-il sur tout le dossier, ou sur une seule prestation du dossier ?
   Réponse :

80. Le résultat d'un contrôle
   C'est quoi : un contrôle peut finir conforme, trouver une anomalie, ou être encore en cours. Le résultat sert à relier les contrôles aux indus.
   Objectif : dans T28 à T30, les colonnes des dossiers contrôlés doivent montrer les vérifications faites dans chaque trimestre. Elles permettent de calculer le ratio de contrôle a posteriori, norme 5 % au moins (Kaya 2017). La réponse permet de savoir si un contrôle compte dès qu'il est enregistré, même encore en cours, ou seulement une fois terminé.
   Aujourd'hui : tout contrôle enregistré compte, quel que soit son résultat.
   À qui : DAI
   Question : un contrôle compte-t-il dans le taux quel que soit son résultat, même s'il est encore en cours ?
   Réponse :

81. Le dénominateur du taux de contrôle
   C'est quoi : le taux de contrôle divise les dossiers contrôlés par des dossiers que trois documents nomment autrement : « dossiers payés » (Annuaire CIPRES), « dossiers liquidés et payés » (recueil des besoins), « liquidés ou en portefeuille », c'est-à-dire en cours, payés ou non (grille des ratios).
   Objectif : dans T28 à T30, la colonne des dossiers payés doit montrer la base sur laquelle on mesure l'effort de contrôle. Elle permet de calculer le ratio de contrôle a posteriori, norme 5 % au moins (recueil v7, l. 1111, 1190 et 1267). La réponse permet de choisir le diviseur : les dossiers payés dans la période, ou tous les dossiers liquidés ou en portefeuille.
   Dans la base : un dossier payé a un débours PM au statut D ou P dans la période (DEBOURS). Une coordination payée par l'autre caisse (DOS_CATEGORIE = COOR_AC) est en portefeuille sans rien recevoir de la CNSS.
   Aujourd'hui : on divise par les dossiers payés dans la période ; un contrôle sur un dossier non payé, comme cette coordination, ne compte pas.
   À qui : DAI, avec la DCP et les DR
   Question : divise-t-on par les dossiers payés dans la période, ou par les dossiers liquidés ou en portefeuille ?
   Réponse :

82. Le dossier payé ou liquidé
   C'est quoi : les indemnités journalières, allocations prénatales et allocations de vieillesse sont versées une fois ou sur quelques mois : le dossier peut être liquidé une année et payé la suivante.
   Objectif : dans T28 à T30, la colonne des dossiers payés doit montrer, pour les prestations versées une fois ou sur quelques mois, les dossiers de la bonne année. Elle permet de calculer le ratio de contrôle a posteriori, norme 5 % au moins (Kaya 2017). La réponse permet de compter ces dossiers l'année du paiement, ou l'année de la liquidation comme l'annuaire CNSS 2024.
   Dans la base : payé : un débours PM au statut D ou P (DEBOURS) dans la période. Liquidé : la première notification (PRESTATION_ESP, PE_DATE_NOTIFICATION) dans la période.
   Aujourd'hui : on compte le dossier payé dans la période.
   Chiffre : l'annuaire CNSS 2024 compte autrement : 3 893 bénéficiaires d'indemnités de maternité en 2024 (tableaux 35 et 39), soit les 3 890 liquidées et non les 4 881 payées.
   À qui : DAI, avec la DCP
   Question : pour ces prestations, le dossier compté dans T28 à T30 est-il celui payé dans la période, ou celui liquidé dans la période ?
   Réponse :

83. Les dossiers payés au trimestre
   C'est quoi : T28 à T30 se produisent au trimestre, au semestre et à l'année. La maquette n'a qu'une colonne de dossiers payés, à côté des contrôles des quatre trimestres.
   Objectif : dans T28 à T30, la colonne des dossiers payés doit montrer le bon diviseur pour un trimestre ou un semestre. Ces tableaux se produisent au trimestre, au semestre et à l'année (recueil v7, l. 136) et donnent le ratio de contrôle a posteriori, norme 5 % au moins (Kaya 2017). La réponse permet de compter les dossiers payés depuis le 1er janvier, ou dans la seule période.
   Aujourd'hui : les dossiers payés se comptent du 1er janvier à la fin de la période, les contrôles trimestre par trimestre. Au 2e trimestre 2025 : dossiers payés du 1er janvier au 30 juin, contrôles d'avril à juin. Le taux du 4e trimestre est celui de l'année. Le même choix se pose pour les montants des bénéficiaires au 31/12 (voir le point 96).
   À qui : DAI, avec la DCP
   Question : pour un trimestre ou un semestre, compte-t-on les dossiers payés depuis le 1er janvier ?
   Réponse :

84. Le dossier contrôlé deux fois
   C'est quoi : selon le recueil des besoins, la colonne TOTAL de T28 à T30 additionne les contrôles des quatre trimestres ; sa formule du taux compte des « dossiers contrôlés après paiement ».
   Objectif : dans T28 à T30, la colonne TOTAL additionne les contrôles des quatre trimestres, et le taux de l'année doit montrer la part des dossiers payés réellement vérifiés. Ce ratio de contrôle a posteriori a une norme de 5 % au moins (Kaya 2017). La réponse permet de compter une ou deux fois, dans le taux, un dossier contrôlé deux fois dans l'année.
   Aujourd'hui : un dossier contrôlé le 12/02/2025 puis le 20/10/2025 compte deux fois dans la colonne TOTAL, une seule fois dans le taux de l'année.
   À qui : DAI
   Question : dans le taux de contrôle, un dossier contrôlé deux fois dans l'année compte-t-il deux fois ?
   Réponse :

85. Qui voit le détail des contrôles
   C'est quoi : le résultat d'un contrôle et l'indu trouvé touchent des assurés nommément. Le recueil des besoins demande un accès aux données selon le profil de chaque utilisateur.
   Objectif : dans T28 à T31, les taux doivent être visibles de tous, et le détail par dossier seulement de ceux qui en ont le droit, car il touche des assurés nommément. La solution doit régler l'accès aux données selon le profil de chaque utilisateur (recueil v7, l. 72). La réponse permet de fixer qui voit le détail des contrôles et des indus, et qui ne voit que les taux.
   Dans la base : la BI ne lit pas les textes libres DC_REMARQUE et DC_RECOMMANDE (DOSSIERS_CONTROLES), ni AJ_REMARQUE (AJUSTEMENT).
   Aujourd'hui : seuls les taux sont diffusés ; le détail par dossier reste réservé.
   À qui : DAI, avec la DSI
   Question : le détail par dossier reste-t-il réservé à quelques profils, les autres ne voyant que les taux ?
   Réponse :

86. L'achat de rente (T29)
   C'est quoi : l'atelier de Kaya en 2017 a ajouté à T29 une ligne « achat de rente ». La base et l'annuaire CNSS parlent de rachat de rente : un capital versé une fois à la place d'une rente d'accident du travail.
   Objectif : dans T29, la ligne « achat de rente », ajoutée par l'atelier de Kaya, doit montrer les dossiers d'accident du travail qui ont reçu un capital à la place d'une rente (Kaya 2017). Elle entre dans le ratio de contrôle a posteriori de la branche, norme 5 % au moins. La réponse permet de remplir cette ligne avec les rachats de rente de la base.
   Dans la base : AJUSTEMENT, TAJ_CODE = RR, payé par un débours d'ajustement (DEBOURS, DEB_TYPE = AJ), pas par un paiement ordinaire (PM).
   Aujourd'hui : la ligne compte les dossiers qui ont reçu un rachat de rente dans la période.
   Chiffre : en 2024, 11 rachats pour 21,5 M dans la base ; l'annuaire CNSS 2024 en publie 25,4 M (tableaux 90, 96 et 97), un écart pas encore expliqué.
   À qui : DCP, avec les DR
   Question : l'« achat de rente » de T29 est-il bien le rachat de rente ?
   Réponse :

87. Les frais médicaux de maternité (T30)
   C'est quoi : la dernière ligne de T30 s'appelle « Autres prestations de la branche ». Pour la même ligne de T27, le recueil des besoins demande « Frais médicaux de maternité » ; il ne dit rien pour T30.
   Objectif : dans T30, la dernière ligne doit montrer les dossiers de frais médicaux de maternité payés et contrôlés, sous le même nom que dans T27 (recueil v7, l. 127). Elle entre dans le ratio de contrôle a posteriori des prestations familiales, norme 5 % au moins (Kaya 2017). La réponse permet de renommer cette ligne comme dans T27, ou de garder le nom de l'Annuaire CIPRES.
   Dans la base : aucun soin remboursé (DEBOURS, DEB_TYPE = RB) en 2024 sur les dossiers de maternité (TDOS_CODE = M).
   Aujourd'hui : la ligne est renommée comme dans T27 et reste vide. Pour savoir où ces frais sont enregistrés, voir le point 129.
   Chiffre : l'annuaire CNSS 2024 publie 25,0 M de frais médicaux de maternité payés en 2024 (tableaux 40 et 41).
   À qui : DCP, avec la DES
   Question : dans T30, la ligne « Autres prestations de la branche » devient-elle « Frais médicaux de maternité », comme dans T27 ?
   Réponse :

T31, les indus

30. La pension versée après un décès (T31)
   C'est quoi : quand un décès est déclaré tard, la pension continue d'être versée après la mort. La CNSS peut alors établir un trop-perçu pour reprendre ces sommes.
   Objectif : dans T31, la colonne des trop-perçus des pensions doit montrer toutes les sommes versées à tort, y compris après un décès déclaré en retard. Elle permet de calculer le ratio des prestations indues, norme 2 % au plus (Kaya 2017). La réponse permet de savoir si ces pensions versées après la mort doivent devenir des trop-perçus, et donc compter dans T31.
   Dans la base : la date du décès (INDIVIDU, IND_DATE_DECES) et les trop-perçus (AJUSTEMENT, TAJ_CODE = TP ou TP-CONV) établis après ce décès (AJ_DATE_ETABLISSEMENT).
   Aujourd'hui : on ne compte que les trop-perçus enregistrés.
   Chiffre : en 2024, 7 trop-perçus seulement sont établis après un décès, alors qu'environ 175 décès de retraités de 2024 n'ont été saisis qu'après mars 2025 (voir le point 2).
   À qui : DCP
   Question : une pension versée après un décès déclaré en retard devient-elle un trop-perçu ?
   Réponse :

31. Le recouvrement d'un indu (T31)
   C'est quoi : un trop-perçu est repris peu à peu par des retenues sur les paiements suivants, ou remboursé par l'assuré.
   Objectif : dans T31, la colonne des trop-perçus doit montrer ce qui a été versé à tort dans la période. Elle sert au taux de prestations indues, norme 2 % au plus, que suit aussi le recouvrement (recueil v7, l. 1608). La réponse permet de savoir comment repérer un trop-perçu remboursé, et s'il faut le déduire de ce taux.
   Dans la base : dans AJUSTEMENT, depuis 2017, aucun trop-perçu n'est soldé (AJ_STATUT = C), tous restent en cours (voir le point 61). Les remboursements ont leur code, TAJ_CODE = REM-TP.
   Aujourd'hui : un remboursement ne diminue pas les trop-perçus de T31.
   Chiffre : en 2024, 13,0 M de remboursements pour 229,5 M de trop-perçus. Les déduire ferait passer le taux d'indus de 0,39 % à 0,37 %.
   À qui : DCP, avec la DSI
   Question : comment voit-on qu'un trop-perçu est remboursé : par les retenues sur les paiements, par les remboursements notés à part (REM-TP), ou autrement ?
   Réponse :

32. Les moins-perçus des prestations familiales (T31)
   Objectif : dans T31, la colonne des moins-perçus des prestations familiales doit montrer les sommes payées en moins puis rattrapées dans l'année. Elle permet de suivre, mois par mois et par branche, les paiements et leurs écarts (Kaya 2017). La réponse permet de savoir si la chute de ces moins-perçus est réelle, ou si une partie manque à la colonne.
   Dans la base : AJUSTEMENT, TAJ_CODE = MP, sur un dossier de prestations familiales (TDOS_CODE = F), compté l'année où il est établi (AJ_DATE_ETABLISSEMENT).
   Aujourd'hui : on les compte tels qu'ils sont.
   Chiffre : 56 362 moins-perçus en 2023, 17 126 en 2024, 1 384 en 2025.
   À qui : DCP
   Question : la chute des moins-perçus des prestations familiales est-elle réelle, ou ces rappels sont-ils maintenant versés autrement ?
   Réponse :

33. Les paiements par ajustement
   C'est quoi : certains rappels de moins-perçus et certaines avances sur pension sont versés par un ajustement, pas par un paiement ordinaire. Ce sont bien des sommes versées à l'assuré, ce que l'atelier de Kaya en 2017 appelle les « payées ».
   Objectif : dans T31 et T56, les prestations payées doivent montrer tout l'argent versé aux assurés, « la somme décaissée au profit de l'assuré » (Kaya 2017). Elles servent de diviseur au ratio des prestations indues, norme 2 % au plus. La réponse permet d'y ajouter, ou non, les rappels et les avances versés par un ajustement.
   Dans la base : un débours d'ajustement (DEBOURS, DEB_TYPE = AJ) relié à l'ajustement (AJUSTEMENT, TAJ_CODE = MP ou RC pour les rappels, AV pour les avances), non un débours PM.
   Aujourd'hui : elles ne sont ni dans les prestations payées de T31, ni dans les prestations servies de T56. Pour T56, la question ne porte que sur les rappels (rachats de rente : voir le point 52).
   Chiffre : en 2024, 408,3 M de rappels et 373,2 M d'avances versés ainsi ; les rappels font environ 0,7 % des prestations payées.
   À qui : DCP, avec la DAI et la DPC
   Question : les sommes versées par un ajustement (rappels de moins-perçus, avances) sont-elles des prestations payées ?
   Réponse :

88. Les prestations payées de T31
   C'est quoi : le taux d'indus divise les trop-perçus par les prestations payées, qui devraient égaler, sur l'année et par branche, les prestations servies de l'annuaire CNSS.
   Objectif : dans T31, la colonne « payées » doit montrer, par branche, les mêmes sommes que les prestations servies de l'annuaire CNSS. Elle sert de diviseur au ratio des prestations indues, norme 2 % au plus (Kaya 2017). La réponse permet de savoir s'il faut y ajouter les soins, les rachats de rente et les frais médicaux de maternité, surtout pour les accidents du travail.
   Dans la base : T31 prend les débours PM au statut D ou P (DEBOURS), sans les soins des accidents du travail (DEB_TYPE = RB) ni les rachats de rente (AJUSTEMENT, TAJ_CODE = RR). Les frais médicaux de maternité ne sont pas dans la base.
   Aujourd'hui : les paiements ordinaires seulement (pour ces soins, voir le point 51).
   Chiffre : en 2024, 58,73 Md dans la base pour 58,88 Md dans l'annuaire CNSS 2024 (tableau 106), mais les branches se compensent : en accidents du travail, 744,6 M pour 934,4 M publiés en argent (tableaux 90, 96 et 97).
   À qui : DCP, avec la DPC
   Question : les prestations payées de T31 doivent-elles égaler les prestations servies de l'annuaire CNSS, soins, rachats de rente et frais médicaux de maternité compris ?
   Réponse :

89. La source des indus (T31)
   C'est quoi : le recueil des besoins donne les fichiers Excel de la DAI comme source de T31. Or la base a tous les trop-perçus enregistrés par les services ; les fichiers de la DAI n'ont peut-être que ceux trouvés par ses contrôles.
   Objectif : dans T31, la colonne des trop-perçus doit montrer, par mois et par branche, les sommes payées en trop. Elle permet de calculer le ratio des prestations indues, norme 2 % au plus (Kaya 2017). La réponse permet de prendre tous les trop-perçus de l'application, ou les seuls trouvés par les contrôles de la DAI, que le recueil donne comme source (recueil v7, l. 145).
   Dans la base : AJUSTEMENT, TAJ_CODE = TP ou TP-CONV, par date d'établissement (AJ_DATE_ETABLISSEMENT).
   Aujourd'hui : T31 lit la base ; le fichier de la DAI servira à comparer.
   Chiffre : en 2024, 684 trop-perçus dans la base, pour 229,5 M : 0,39 % des prestations payées.
   À qui : DAI, avec la DCP
   Question : T31 doit-il prendre tous les trop-perçus de l'application, ou seulement ceux trouvés par vos contrôles ?
   Réponse :

90. Le dénominateur du taux d'indus (T31)
   C'est quoi : le recueil des besoins et l'atelier de Kaya en 2017 divisent les trop-perçus par les prestations payées de la période ; la grille des ratios de la CNSS, par le montant des dossiers contrôlés.
   Objectif : dans T31, le taux d'indus doit montrer quelle part de l'argent versé l'a été à tort. Il se compare à la norme CIPRES de 2 % au plus (Kaya 2017). La réponse permet de choisir le diviseur : toutes les prestations payées de la période, ou le seul montant des dossiers contrôlés, qui donnerait un taux bien plus haut.
   Dans la base : le montant des dossiers contrôlés n'est nulle part : il faudrait l'ajouter à la liste des contrôles de la DAI (voir le point 70).
   Aujourd'hui : on divise par toutes les prestations payées de la période.
   Chiffre : en 2024, 0,39 % des prestations payées. Sur les seuls dossiers contrôlés, le taux pourrait être bien plus haut.
   À qui : DAI
   Question : le taux d'indus se calcule-t-il sur toutes les prestations payées, ou sur le seul montant des dossiers contrôlés ?
   Réponse :

91. Les moins-perçus dans le taux d'indus (T31)
   C'est quoi : l'atelier de Kaya en 2017 a dit que les moins-perçus ne sont pas des indus. La grille des ratios de la CNSS additionne pourtant trop-perçus et moins-perçus, et l'Annuaire CIPRES parle d'indus « en trop ou en moins ».
   Objectif : dans T31, le taux d'indus doit montrer les sommes versées à tort, à comparer à la norme de 2 % au plus. Pour l'atelier de Kaya, les moins-perçus ne sont pas des indus (Kaya 2017). La réponse permet de les laisser hors du taux, ou de les y ajouter comme la grille des ratios de la CNSS.
   Dans la base : AJUSTEMENT, TAJ_CODE = MP ou RC.
   Aujourd'hui : ils sont montrés à part, hors du taux ; le taux des deux ensemble est affiché à côté.
   Chiffre : en 2024, 0,39 % sans eux, 1,10 % avec eux : sous la norme de 2 % dans les deux cas.
   À qui : DAI, avec la DCP
   Question : les moins-perçus entrent-ils dans le taux d'indus ?
   Réponse :

92. Les trop-perçus sur convention (T31)
   C'est quoi : un trop-perçu sur convention est une somme payée en trop sur un dossier de coordination avec la caisse d'un autre pays.
   Objectif : dans T31, la colonne des trop-perçus des pensions doit montrer les vraies sommes payées en trop, coordination comprise. Elle permet de suivre mois par mois les indus de chaque branche et le ratio des prestations indues, norme 2 % au plus (Kaya 2017). La réponse permet de savoir si le bond de 2025 est réel, ou s'il vient d'une autre façon de saisir.
   Dans la base : AJUSTEMENT, TAJ_CODE = TP-CONV, par date d'établissement (AJ_DATE_ETABLISSEMENT).
   Aujourd'hui : comptés comme les autres trop-perçus, dans la colonne des pensions.
   Chiffre : 15 en 2024 pour 12,1 M ; 758 en 2025 pour 54,8 M.
   À qui : DCP
   Question : le bond des trop-perçus sur convention en 2025 est-il réel, ou vient-il d'une autre façon de saisir ?
   Réponse :

93. Les indus par type de prestation (T31)
   C'est quoi : le recueil des besoins demande les taux d'indus par type de prestation (pension de vieillesse, allocation familiale…).
   Objectif : dans T31, le taux d'indus doit montrer où se trouvent les sommes versées à tort. Le recueil demande ce ratio par type de prestation, par direction régionale et par agence (recueil v7, l. 1127). La réponse permet de savoir si le taux par branche suffit, puisqu'aucun trop-perçu n'est relié à sa prestation.
   Dans la base : aucun des 684 trop-perçus de 2024 n'est relié à une prestation (table PE_AJ) : on ne connaît que la branche, par le dossier (TDOS_CODE).
   Aujourd'hui : on donne le taux par branche seulement.
   À qui : DCP, avec la DSI
   Question : le taux d'indus par branche suffit-il ?
   Réponse :

94. L'arrondi des taux
   C'est quoi : le recueil des besoins ne fixe l'arrondi que pour le tableau T76 (l'action sanitaire et sociale) : 2 chiffres après la virgule.
   Objectif : dans T28 à T31, les taux de contrôle et d'indus doivent rester lisibles et comparables à leurs normes, 5 % au moins et 2 % au plus (Kaya 2017). La réponse permet de fixer le nombre de chiffres après la virgule, pour qu'un petit taux, comme celui des indus des prestations familiales, ne soit pas écrasé par l'arrondi.
   Aujourd'hui : 3 chiffres après la virgule, pour le taux de contrôle comme pour le taux d'indus.
   Chiffre : avec 2 chiffres, le taux d'indus des prestations familiales de 2024 s'écrirait 0,04 % au lieu de 0,045 %.
   À qui : DAI
   Question : les taux de contrôle et d'indus s'affichent-ils avec 2 chiffres après la virgule ?
   Réponse :

95. Qui suit le taux d'indus (T31)
   C'est quoi : le recueil des besoins range le taux de prestations indues avec les indicateurs des prestations et avec ceux du recouvrement, suivis par la DCR. L'atelier de Kaya en 2017 le rangeait déjà sous le recouvrement.
   Objectif : dans T31, le taux de prestations indues doit être un seul chiffre, le même pour tous ceux qui le suivent. Le recueil le range avec les indicateurs des prestations et avec ceux du recouvrement, sous la DCR et les DR (recueil v7, l. 1369 et 1608). La réponse permet de savoir à quelle direction le livrer et qui en répond.
   Aujourd'hui : un seul calcul, le même pour les deux.
   À qui : DCR, avec la DAI
   Question : le taux de prestations indues est-il suivi par la DCR, ou par la DAI ?
   Réponse :

T32 à T41, les bénéficiaires au 31/12

34. La retraite anticipée (T32)
   C'est quoi : une pension de vieillesse prise avant l'âge normal de la retraite.
   Objectif : dans T32, la ligne « retraite anticipée » doit montrer à part les pensionnés partis avant l'âge normal de la retraite. Elle permet de suivre ces départs précoces, comme le recueil le demande pour les dossiers (recueil v7, l. 248) et comme l'annuaire les publie (annuaire CNSS 2024, tableau 22). La réponse permet de garder des tranches d'âge de la seule vieillesse ordinaire, sans y mêler les anticipés.
   Dans la base : PRESTATION_ESP, code RPRA ; la vieillesse ordinaire a le code RPV.
   Aujourd'hui : une ligne à part sous T32 ; les tranches d'âge ne comptent que la vieillesse. Pour la maquette CIPRES d'origine, on additionne les deux.
   Chiffre : fin 2024, 87 pensions de retraite anticipée en cours dans la base (le nombre de personnes reste à mesurer). L'annuaire CNSS 2024 fait pareil : il publie à part ses 78 retraités en retraite anticipée.
   À qui : DCP
   Question : on garde la retraite anticipée sur une ligne à part sous T32, hors des tranches d'âge ?
   Réponse :

35. Le montant d'un tableau
   C'est quoi : à côté de l'effectif, chaque tableau donne le montant payé dans l'année. Des personnes sortent en cours d'année (décès, radiation).
   Objectif : dans T32 à T41, la colonne « montant » doit montrer ce que la CNSS a versé dans l'année aux bénéficiaires. Divisée par l'effectif, elle donne la pension ou la rente moyenne, signe du pouvoir d'achat des bénéficiaires (Kaya 2017). La réponse permet de dire si l'on prend aussi les sommes versées aux personnes sorties : 38,63 Md au lieu de 37,67 Md pour T32 en 2024.
   Dans la base : les paiements : DEBOURS, DEB_TYPE = PM, DEB_STATUT D ou P, montant DEB_MONTANT.
   Aujourd'hui : on prend ce qui a été payé aux personnes comptées : c'est le seul montant qui se range par âge et par sexe.
   Chiffre : pour T32 en 2024, 37,67 Md payés aux pensionnés comptés, 38,63 Md en tout. L'annuaire CNSS 2024 publie 38,24 Md, entre les deux.
   À qui : DCP pour les pensions ; DP et DR pour les rentes ; DES et DR pour T41
   Question : le montant d'un tableau est-il ce qui a été payé dans l'année aux personnes comptées au 31/12, ou tout ce qui a été payé dans l'année, y compris aux personnes sorties ?
   Réponse :

36. La pension moyenne et la coordination
   C'est quoi : la pension moyenne est le montant divisé par l'effectif. Une coordination que paie l'autre caisse est dans l'effectif, mais la CNSS ne lui verse rien.
   Objectif : dans T32, la pension moyenne doit montrer ce que touche en moyenne un pensionné de vieillesse. Son évolution d'une année à l'autre permet de juger du maintien du pouvoir d'achat des pensionnés (Kaya 2017). La réponse permet de dire si les pensions de coordination payées par l'autre caisse restent dans l'effectif qui divise le montant, alors que sans elles la moyenne serait 9,7 % plus haute.
   Dans la base : DOSSIER, DOS_CATEGORIE = COOR_AC.
   Aujourd'hui : on les garde : le montant est divisé par l'effectif total.
   Chiffre : fin 2025, 2 901 pensions de vieillesse sont payées par une autre caisse : sans elles, la pension moyenne de T32 serait 9,7 % plus haute.
   À qui : DCP
   Question : faut-il garder dans la pension moyenne les pensions de coordination que paie l'autre caisse ?
   Réponse :

37. Les veuves qui manquent (T34)
   C'est quoi : un décès ou l'arrêt d'une pension est parfois saisi des mois après. Si l'annuaire est fait avant, la veuve y est encore ; plus tard, elle n'y est plus.
   Objectif : dans T34, l'effectif doit montrer les veuves et veufs vraiment pensionnés au 31/12, par âge et par sexe. Il permet d'établir leur pyramide des âges et leur pension moyenne, utiles aux évaluations actuarielles (Kaya 2017). La réponse permet de savoir si les décès saisis en retard expliquent les 5,0 % qui manquent face à l'annuaire, et donc à quelle date prendre la copie de la base.
   Dans la base : pensions de veuve et de veuf (PRESTATION_ESP, codes RPSV et RPSF) en cours au 31/12 ; décès (IND_DATE_DECES), arrêt (PE_DATE_RADIATION). Pas de date de déclaration du décès.
   Aujourd'hui : on compte sur la copie de la base du 02/02/2026. Prendre la copie arrêtée fin mars de l'année suivante est proposé (voir le point 2).
   Chiffre : fin 2024, 20 359 veuves dans la base pour 21 438 dans l'annuaire CNSS 2024 (5,0 % de moins), surtout à Bobo-Dioulasso et à Ouagadougou.
   À qui : DCP, avec les DR
   Question : le décès d'une veuve pensionnée, ou l'arrêt de sa pension, est-il souvent saisi plusieurs mois après les faits ?
   Réponse :

38. L'enfant non payé (T41)
   C'est quoi : un enfant sans certificat (de scolarité, par exemple) n'est pas radié : il n'est plus payé. Il peut donc avoir droit au 31/12 sans rien toucher dans l'année.
   Objectif : dans T41, l'effectif doit montrer les allocataires dont au moins un enfant a réellement touché ses allocations dans l'année. Il permet d'établir la pyramide des âges des allocataires et l'allocation moyenne, utiles aux évaluations actuarielles (Kaya 2017). La réponse permet de dire si les enfants restés en droit mais jamais payés y entrent, ce qui mettrait T41 5,4 % au-dessus de l'annuaire.
   Dans la base : l'allocation de l'enfant (PRESTATION_ESP, code FAF) en cours au 31/12, enfant de 21 ans au plus ; ses paiements (DEBOURS, type PM, statut D ou P).
   Aujourd'hui : seuls les enfants payés dans l'année comptent ; l'allocataire compte s'il a au moins un enfant compté.
   Chiffre : l'annuaire CNSS 2024 fait pareil : en 2024, 98 048 allocataires pour 97 378 publiés (0,7 % d'écart) ; avec les enfants non payés, 5,4 % au-dessus.
   À qui : DES et DR
   Question : un enfant dont l'allocation court au 31/12, mais qui n'a rien touché dans l'année, compte-t-il dans T41 ?
   Réponse :

39. Qui est l'allocataire (T41)
   C'est quoi : le recueil des besoins de la CNSS dit : « Allocataire : Titulaire du dossier (assuré) ». T41 donne son âge et son sexe, et le compte une fois, même s'il a deux dossiers.
   Objectif : dans T41, chaque ligne doit compter l'allocataire une seule fois, à son âge et sous son sexe (recueil v7, l. 152). Elle permet de tracer la pyramide des âges des allocataires, en comptant le dossier et non les enfants (Kaya 2017). La réponse permet de dire qui ranger par âge et par sexe quand le titulaire est décédé : lui, ou la personne qui touche pour les enfants.
   Dans la base : le titulaire (DOSSIER, IND_ID), son âge, son sexe et son décès (INDIVIDU, IND_DATE_NAISSANCE, IND_SEXE, IND_DATE_DECES).
   Aujourd'hui : on prend le titulaire, même décédé si ses enfants touchent encore par un tuteur, à l'âge qu'il aurait.
   Chiffre : en 2024, 102 637 titulaires pour 102 643 dossiers (enfants non payés compris) ; le nombre de titulaires décédés n'est pas encore mesuré.
   À qui : DES et DR
   Question : si le titulaire du dossier est décédé et que ses enfants touchent encore, l'allocataire de T41 est-il le titulaire décédé, ou la personne qui touche les allocations pour les enfants ?
   Réponse :

40. Le montant de T41
   C'est quoi : ce qui a été payé dans l'année en allocations familiales.
   Objectif : dans T41, la colonne « montant » doit montrer les allocations familiales versées dans l'année. Divisée par l'effectif, elle donne l'allocation moyenne par allocataire, utile aux évaluations actuarielles (Kaya 2017). La réponse permet de savoir si l'écart entre nos 7,69 Md et les 8,24 Md publiés vient d'allocations validées mais pas encore versées, et donc quel montant retenir.
   Dans la base : les paiements des enfants comptés : DEBOURS, DEB_TYPE = PM, DEB_STATUT D ou P, montant DEB_MONTANT.
   Aujourd'hui : on prend les allocations payées aux enfants comptés.
   Chiffre : en 2024, 7,69 Md pour les enfants comptés, au plus 8,00 Md en prenant la date de saisie. L'annuaire CNSS 2024 publie 8,24 Md, alors que l'effectif retombe à 0,7 % près. Son montant est un multiple de 500 FCFA, comme les allocations, pas comme les rappels versés par une correction de paiement (AJUSTEMENT) : il n'en contient sans doute pas.
   À qui : DPC, avec la DES
   Question : le montant des allocations familiales de votre annuaire (8,24 Md en 2024) comprend-il des allocations validées mais pas encore versées ?
   Réponse :

41. L'allocation prénatale seule (T41)
   C'est quoi : l'allocation prénatale est versée à une future mère pendant sa grossesse.
   Objectif : dans T41, l'effectif doit montrer les seuls allocataires des allocations familiales, par âge et par sexe. Il permet d'établir leur pyramide des âges et l'allocation moyenne (Kaya 2017). La réponse permet de dire si une future mère qui ne touche que l'allocation prénatale, déjà suivie dans T55 (Kaya 2017), doit aussi y entrer.
   Dans la base : T41 ne prend que les allocations familiales (PRESTATION_ESP, code FAF), pas les prénatales (codes FAP et MAP).
   Aujourd'hui : une femme qui ne touche que l'allocation prénatale n'est pas allocataire de T41.
   Chiffre : l'annuaire CNSS 2024 fait pareil : 98 048 allocataires en 2024 pour 97 378 publiés.
   À qui : DES et DR
   Question : on garde T41 sans les femmes qui ne touchent que l'allocation prénatale ?
   Réponse :

42. Le tableau des enfants (T41)
   C'est quoi : un tableau des enfants par tranche d'âge et par sexe, avec le montant. L'atelier de Kaya en 2017 le demandait ; l'annuaire CNSS 2024 le publie (tableau 36).
   Objectif : dans le tableau des enfants ajouté à T41, chaque tranche doit montrer les enfants dont les allocations sont versées, avec le montant. Il permet de connaître les enfants couverts, comme le demandait l'atelier (Kaya 2017) et comme le publie l'annuaire (annuaire CNSS 2024, tableau 36). La réponse permet de savoir si la tranche des 20-21 ans compte aussi les enfants sortis à 21 ans dans l'année.
   Dans la base : les allocations des enfants (PRESTATION_ESP, code FAF, IND_ID_PREST), leur âge (IND_DATE_NAISSANCE) et leurs paiements (DEBOURS).
   Aujourd'hui : on ajoute ce tableau, avec les enfants en cours au 31/12 et payés dans l'année.
   Chiffre : même avec les enfants non payés, la base a 32,1 % de 20-21 ans de moins que l'annuaire en 2024. Ceux de l'annuaire touchent en moyenne 18 177 FCFA dans l'année, contre 28 000 à 34 000 FCFA pour les autres tranches : comme s'ils n'étaient payés qu'une partie de l'année.
   À qui : DPC, avec la DES
   Question : votre tableau des enfants d'allocataires (tableau 36) compte-t-il aussi les enfants sortis dans l'année parce qu'ils ont atteint 21 ans ?
   Réponse :

43. Les pensionnés très âgés
   C'est quoi : un contrôle d'existence vérifie qu'un pensionné est toujours en vie (certificat de vie, par exemple).
   Objectif : dans T32 à T36, l'effectif doit montrer les seuls pensionnés encore en vie, jusqu'aux tranches des plus âgés. Il permet d'établir une pyramide des âges juste, base des évaluations actuarielles (Kaya 2017). La réponse permet de savoir si un contrôle d'existence vérifiera les 69 pensionnés payés de 100 ans ou plus et les 215 pensions encore actives après un décès.
   Dans la base : l'âge (INDIVIDU, IND_DATE_NAISSANCE), le décès (IND_DATE_DECES) et la pension en cours (PRESTATION_ESP, sans PE_DATE_RADIATION).
   Aujourd'hui : on compte la personne tant que sa pension est en cours et qu'elle n'est pas déclarée décédée.
   Chiffre : fin 2025, 69 pensionnés payés ont 100 ans ou plus, et 215 pensions sont encore actives alors que leur titulaire est décédé. En 2024, 74 retraités de 100 ans et plus dans la base, 56 dans l'annuaire CNSS 2024.
   À qui : DCP
   Question : un contrôle d'existence est-il prévu pour ces pensions ?
   Réponse :

96. Le montant du mois, seul ou cumulé
   C'est quoi : le recueil des besoins de la CNSS veut T32 à T41 à la fin de chaque mois et de chaque trimestre. L'effectif est celui du dernier jour ; pour le montant, il faut choisir.
   Objectif : dans T32 à T41 sortis à la fin d'un mois ou d'un trimestre, la colonne « montant » doit montrer ce qui a été payé. Elle permet de suivre les sommes versées au fil de l'année, comme le recueil le demande par mois et par trimestre (recueil v7, l. 153). La réponse permet de choisir entre le montant de la période seule et le cumul depuis le 1er janvier.
   Dans la base : les paiements (DEBOURS, type PM, statut D ou P), rangés par mois payé (DEB_DATE_EFFET).
   Aujourd'hui : on prend le montant du mois ou du trimestre seul ; celui de l'année est la somme des mois.
   Chiffre : l'annuaire CNSS 2024 fait pareil dans ses tableaux mois par mois (tableaux 28, 41 et 90) : il publie le montant de chaque mois, pas un cumul.
   À qui : DCP ; DP et DR ; DES et DR
   Question : pour un tableau du mois ou du trimestre, le montant est-il celui de la période seule, ou le cumul depuis le 1er janvier ?
   Réponse :

97. Le rapport démographique et la coordination
   C'est quoi : le nombre de cotisants actifs pour un retraité ; selon le recueil des besoins, un retraité est un bénéficiaire de pension directe. Une coordination que paie l'autre caisse est dans l'effectif sans rien recevoir de la CNSS.
   Objectif : dans le rapport démographique, le nombre de retraités doit montrer les bénéficiaires de pensions directes (recueil v7, l. 1251). Le rapport permet de mesurer combien de cotisants actifs font face à chaque retraité, par DR et par agence (recueil v7, l. 1247). La réponse permet de dire si les pensions de coordination payées par l'autre caisse, près de 9 % des retraités, entrent dans ce compte.
   Dans la base : PRESTATION_ESP, codes RPV, RPRA, RPI, en cours au 31/12, une fois par personne ; coordination payée ailleurs : DOS_CATEGORIE = COOR_AC.
   Aujourd'hui : pas encore calculé ; on prévoit de compter ces pensions comme des retraités.
   Chiffre : fin 2025, 2 901 pensions de vieillesse sont payées par une autre caisse, près de 9 % des retraités. L'annuaire CNSS 2024 les compte dans son effectif des pensionnés.
   À qui : DCP
   Question : pour le rapport démographique, une pension de coordination payée par l'autre caisse compte-t-elle comme un retraité de la CNSS ?
   Réponse :

98. Le taux de remplacement
   C'est quoi : la part du dernier salaire que la pension remplace : la pension divisée par le salaire moyen des cinq meilleures années, selon le recueil des besoins de la CNSS. Il vise les pensions directes.
   Objectif : dans le taux de remplacement, le chiffre doit montrer quelle part du dernier salaire, le salaire moyen des cinq meilleures années, la pension directe remplace (recueil v7, l. 1256). Il permet de juger, par DR et par agence, si les pensions suffisent à remplacer le revenu d'activité (Proposition BI). La réponse permet de dire s'il porte sur toutes les pensions directes en cours ou sur les seules nouvelles de l'année.
   Dans la base : PRESTATION_ESP, codes RPV, RPRA, RPI, et le salaire moyen de chaque pension (PE_SAL_MOYEN), jamais étudié (voir le point 133).
   Aujourd'hui : pas encore calculé. On prévoit de prendre toutes les pensions directes en cours à la date du tableau.
   À qui : DCP, avec la DES
   Question : le taux de remplacement porte-t-il sur toutes les pensions directes en cours, ou seulement sur les nouvelles pensions de l'année ?
   Réponse :

99. L'enfant payé dans les 12 mois (T41)
   C'est quoi : le recueil des besoins veut T41 à chaque fin de mois et de trimestre. Au 31/12, on prend les enfants payés dans l'année (voir le point 38) ; en cours d'année, il faut une période, car une partie des allocations est payée en retard.
   Objectif : dans T41 sorti à la fin d'un mois ou d'un trimestre, l'effectif doit montrer les allocataires dont un enfant est réellement payé, comme au 31/12. Il permet de suivre le nombre d'allocataires en cours d'année, comme le recueil le demande (recueil v7, l. 153). La réponse permet de fixer la période de paiement, les 12 mois avant la date du tableau, pour ne pas perdre les enfants payés en retard.
   Dans la base : DEBOURS, type PM, statut D ou P, mois payé (DEB_DATE_EFFET).
   Aujourd'hui : un enfant compte s'il a été payé dans les 12 mois avant la date du tableau ; au 31/12, c'est l'année civile.
   Chiffre : en 2024, avec les seuls enfants payés au dernier trimestre, on serait 1 % sous l'annuaire CNSS 2024 ; avec ceux payés dans l'année, 0,7 % au-dessus.
   À qui : DES et DR
   Question : pour T41 à la fin d'un mois ou d'un trimestre, un enfant compte-t-il s'il a été payé dans les 12 mois qui précèdent ?
   Réponse :

100. Les invalides âgés (T33)
   C'est quoi : la loi veut qu'une pension d'invalidité devienne une pension de vieillesse à l'âge de la retraite. L'application ne le fait pas ; le recueil des besoins demande un module pour cela (une fonction de l'application qui ferait le changement).
   Objectif : dans T33, la ligne « 60 ans et + » doit montrer les invalides qui ont passé l'âge de la retraite sans être changés en retraités. Elle permet de voir combien de pensions attendent le module de conversion demandé à la DSI (recueil v7, l. 270). La réponse permet de les laisser dans les tranches de T33 ou de les isoler sur une ligne à part.
   Dans la base : PRESTATION_ESP, code RPI, et l'âge (IND_DATE_NAISSANCE).
   Aujourd'hui : on les garde dans T33, ligne « 60 ans et + », comme l'application.
   Chiffre : en 2024, 72 des 125 invalides ont 60 ans et plus. L'annuaire CNSS 2024 fait pareil : 70 sur 119.
   À qui : DCP
   Question : en attendant ce module, on garde dans T33 les invalides qui ont passé l'âge de la retraite, ou on les montre sur une ligne à part ?
   Réponse :

101. Les dossiers transférés à une autre caisse
   C'est quoi : un dossier de pension peut être transféré à une autre caisse, qui le gère alors. T25 à T27 retirent ces dossiers (voir le point 19).
   Objectif : dans T32 à T36, l'effectif au 31/12 doit montrer les pensions que la CNSS gère encore. Il permet d'établir la pyramide des âges et la pension moyenne de ses pensionnés (Kaya 2017). La réponse permet de dire si les pensions dont le dossier est passé à une autre caisse, 34 de vieillesse et 79 de veuve ou de veuf fin 2025, en sortent comme de T25 à T27.
   Dans la base : DOSSIER, DOS_CATEGORIE = CNSS_AC, lu « transféré à une autre caisse » ; son sens est demandé à la DSI.
   Aujourd'hui : ces pensions restent comptées au 31/12.
   Chiffre : fin 2025, 34 pensions de vieillesse dans ce cas (2 payées par la CNSS) et 79 pensions de veuve ou de veuf.
   À qui : DCP, avec la DSI
   Question : une pension dont le dossier a été transféré à une autre caisse compte-t-elle encore parmi vos bénéficiaires au 31/12 ?
   Réponse :

102. L'âge limite des rentes d'orphelin (T39)
   C'est quoi : quand une victime d'accident du travail décède, ses orphelins reçoivent une rente. Ni le recueil des besoins ni l'atelier de Kaya en 2017 ne disent jusqu'à quel âge.
   Objectif : dans T39, l'effectif doit montrer les orphelins qui touchent vraiment une rente d'accident du travail au 31/12. Il permet d'établir leur pyramide des âges et la rente moyenne, utiles aux évaluations actuarielles (Annuaire CIPRES, p. 13). La réponse permet de fixer l'âge limite, qui fait varier l'effectif de 454 à 531 en 2024.
   Dans la base : PRESTATION_ESP, code ARSE, en cours au 31/12, et l'âge (IND_DATE_NAISSANCE).
   Aujourd'hui : comme pour les pensions (voir le point 10), on ne compte que les orphelins de moins de 25 ans.
   Chiffre : en 2024, 454 rentiers orphelins de moins de 25 ans, 531 sans limite d'âge. L'annuaire CNSS 2024 compte autrement : 485, entre les deux. De 2021 à 2023, ses chiffres sont sous les nôtres, comme avec une limite plus basse.
   À qui : DP et DR
   Question : jusqu'à quel âge une rente d'orphelin d'accident du travail est-elle versée ?
   Réponse :

103. Le « cumul » des rentiers veufs (T38)
   C'est quoi : pour T38, le recueil des besoins demande « le cumul sans préciser un exercice » (une année). On le lit comme un stock à la date choisie ; autre lecture : le montant cumulé depuis le début de chaque rente.
   Objectif : dans T38, l'effectif doit montrer tous les rentiers veufs et veuves en cours à la date choisie, quelle que soit l'année où leur rente a commencé. Il permet d'établir leur pyramide des âges et la rente moyenne, utiles aux évaluations actuarielles (Kaya 2017). La réponse permet de lire « le cumul sans préciser un exercice » comme ce stock, et non comme un montant cumulé (recueil v7, l. 195).
   Dans la base : PRESTATION_ESP, codes ARSV et ARSF, en cours à la date d'arrêté.
   Aujourd'hui : un stock à la date d'arrêté, quelle que soit l'année de début de la rente.
   Chiffre : fin 2024, 288 rentiers veufs et veuves dans la base ; l'annuaire CNSS 2024 fait pareil : 285, dans un tableau « ensemble au 31-12 ».
   À qui : DP et DR
   Question : pour T38, « le cumul sans préciser un exercice » veut-il dire tous les rentiers en cours à la date choisie, quelle que soit l'année où leur rente a commencé ?
   Réponse :

104. Les orphelins jamais radiés (T35)
   C'est quoi : une pension d'orphelin s'arrête à 25 ans ; elle devrait alors être radiée, arrêtée pour de bon.
   Objectif : dans T35, l'effectif doit montrer les seuls orphelins de moins de 25 ans, puisque la tranche « 25 ans et plus » est supprimée (recueil v7, l. 283). Il permet d'établir leur pyramide des âges et la pension moyenne (Kaya 2017). La réponse permet de faire radier les pensions d'orphelin de 25 ans et plus restées ouvertes, pour que la base dise la même chose que le tableau.
   Dans la base : PRESTATION_ESP, code RPSE, sans date de radiation (PE_DATE_RADIATION), orphelin de 25 ans ou plus.
   Aujourd'hui : T35 ne les compte pas (voir le point 10) ; on les signale seulement.
   Chiffre : fin 2024, 5 774 pensions d'orphelin de 25 ans et plus encore en cours, presque jamais payées. Fin 2025, 8 447 pensions d'orphelin actives sans aucun paiement au dernier trimestre.
   À qui : DCP
   Question : faut-il radier les pensions d'orphelin de 25 ans et plus ?
   Réponse :

105. Les orphelins publiés fin 2024
   C'est quoi : au 31/12, l'annuaire CNSS compte les orphelins de pension de moins de 25 ans (tableau 20). Environ 760 passent 25 ans chaque année, autant qu'il en entre : le nombre devrait rester à peu près stable. Il monte pourtant de 791 en 2024.
   Objectif : dans T35, l'effectif doit montrer les orphelins pensionnés de moins de 25 ans au 31/12, d'une année sur l'autre. Il permet de suivre leur nombre et de le comparer à celui de l'annuaire (annuaire CNSS 2024, tableau 20). La réponse permet de savoir si la hausse de 791 orphelins publiée fin 2024 vient d'orphelins hors des nouveaux de l'année, et donc d'expliquer l'écart avec la base.
   Dans la base : les pensions d'orphelin (PRESTATION_ESP, code RPSE) en cours au 31/12 (voir le point 3), d'un orphelin de moins de 25 ans (IND_DATE_NAISSANCE).
   Aujourd'hui : sans effet sur les nouveaux orphelins (T44, T46) ; à garder en tête pour T35 et T39.
   Chiffre : dans l'annuaire CNSS 2024 : 8 687 orphelins fin 2023, plus 769 nouveaux, moins 2 décès, font 9 454 sans retirer ceux qui passent 25 ans, pour 9 478 publiés fin 2024. Dans la base, ils baissent : 8 123 fin 2023, 7 995 fin 2024.
   À qui : DPC
   Question : votre nombre d'orphelins de fin 2024 compte-t-il des orphelins qui ne sont pas parmi vos 769 nouveaux orphelins de 2024 ?
   Réponse :

T42 à T49, les nouveaux bénéficiaires

44. Le sexe des nouveaux retraités (T42)
   C'est quoi : T42 range les nouveaux retraités par sexe et par tranche d'âge.
   Objectif : dans T42, la colonne du sexe doit montrer combien d'hommes et de femmes partent à la retraite chaque année. Elle sert à la loi des entrées, qui permet aux évaluations actuarielles de prévoir les pensions futures (Kaya 2017). La réponse permet de savoir si l'écart avec l'annuaire, 527 femmes publiées pour 641 dans la base, vient de l'endroit où le sexe est lu.
   Dans la base : le sexe de la fiche de la personne (INDIVIDU, IND_SEXE).
   Aujourd'hui : on garde le sexe de la fiche. L'âge est posé à part (voir le point 106).
   Chiffre : l'annuaire CNSS 2024 compte autrement : son tableau 25 donne 527 femmes (21,2 %) parmi les nouveaux retraités de 2024, la base 641 (25,6 %), révisions comprises. Sur l'ensemble des retraités, le sexe retombe à moins de 3 % près.
   À qui : DPC
   Question : votre tableau 25 prend-il le sexe des nouveaux retraités sur la fiche de la personne ?
   Réponse :

106. L'âge dans l'annuaire CNSS
   C'est quoi : les tableaux par âge de l'annuaire CNSS (19, 25 et 36) ne disent pas comment l'âge est calculé.
   Objectif : dans T32 à T49, chaque tranche d'âge doit montrer les bénéficiaires à l'âge du glossaire CIPRES : l'année du tableau moins l'année de naissance. Ces tranches servent à la pyramide des âges et à la loi des entrées (Kaya 2017). La réponse permet de savoir si l'annuaire CNSS calcule l'âge de la même façon, pour comparer nos tranches aux siennes sans glissement d'un an.
   Dans la base : la date de naissance (INDIVIDU, IND_DATE_NAISSANCE).
   Aujourd'hui : l'âge est l'année du tableau moins l'année de naissance, comme dans le glossaire de l'Annuaire CIPRES (voir le point 9).
   Chiffre : nouveaux retraités de 2024, révisions comprises : par tranche de 5 ans, l'annuaire CNSS 2024 fait pareil (55 à 59 ans : 1 450 dans la base pour 1 429 publiés). Âge par âge, nos âges glissent souvent d'un an (60 ans : 377 pour 519).
   À qui : DPC
   Question : dans vos tableaux par âge, l'âge est-il l'année du tableau moins l'année de naissance, ou un âge calculé à une autre date ?
   Réponse :

107. L'âge sur une période à cheval
   C'est quoi : le recueil demande T43 à T49 aussi sur une période libre, entre deux dates : de juillet 2024 à juin 2025, par exemple. L'année du tableau n'est alors plus unique.
   Objectif : dans T43 à T49 sur une période libre, chaque tranche d'âge doit montrer l'âge du nouveau bénéficiaire l'année de son entrée. Elles permettent de suivre la loi des entrées sur la période voulue, que le recueil demande en paramètre (recueil v7, l. 305). La réponse permet de fixer l'année qui sert au calcul de l'âge quand la période couvre deux années.
   Dans la base : la date de saisie de la prestation (PE_DATE_INSERT), qui fait l'entrée, et la date de naissance (IND_DATE_NAISSANCE).
   Aujourd'hui : l'âge est l'année de l'entrée moins l'année de naissance, le même que celui du point 9 sur une année civile. De juillet 2024 à juin 2025, une personne entrée en octobre 2024 prend son âge de 2024, une personne entrée en mars 2025 son âge de 2025.
   À qui : DPC, avec la DCP
   Question : sur une période à cheval sur deux années, l'âge d'un nouveau se compte-t-il avec l'année de son entrée ?
   Réponse :

45. Le rappel d'un nouveau retraité
   C'est quoi : le premier paiement d'un nouveau retraité couvre souvent des mois d'avant l'année : c'est le rappel.
   Objectif : dans T42, la colonne « montant » doit montrer ce qui est payé aux nouveaux retraités pour les mois de l'année, le rappel des mois d'avant étant mis à part. Elle permet de connaître la pension servie aux entrants, élément de la loi des entrées (Kaya 2017). La réponse permet de dire si le rappel, 8 % de ce montant en 2024, entre dans la colonne ou reste à côté.
   Dans la base : les paiements (DEBOURS), avec le mois payé (DEB_DATE_EFFET) et la date de saisie (DEB_DATE_INSERT).
   Aujourd'hui : on prend les mois de l'année ; le rappel sera montré dans une colonne à part. Pour l'année d'un paiement, voir le point 1.
   Chiffre : nouveaux retraités saisis en 2024, révisions comprises : 2,86 Md payés pour les mois de 2024 ; le rappel saisi en 2024, 0,23 Md, vaut 8 % de ce montant.
   À qui : DCP
   Question : le montant d'un nouveau retraité comprend-il le rappel des mois d'avant l'année ?
   Réponse :

108. Le montant des nouveaux sur un mois
   C'est quoi : le recueil demande T42 à T49 par mois, par trimestre et par année. Une personne n'entre qu'une fois : les effectifs des mois s'additionnent pour faire l'année, pas les montants.
   Objectif : dans T42 à T49 sortis par mois ou par trimestre, la colonne « montant » doit montrer ce que pèsent les nouveaux bénéficiaires dans la période (recueil v7, l. 299). Elle permet de suivre, au fil de l'année, la charge des nouvelles pensions et rentes (Proposition BI). La réponse permet de choisir entre ce qui leur est payé dans la période et leur montant mensuel.
   Dans la base : les paiements (DEBOURS) des prestations du tableau, pour les mois de la période (DEB_DATE_EFFET).
   Aujourd'hui : on prend ce qui est payé pour les mois de la période : un retraité entré le 15/03/2025 à 100 000 FCFA par mois pèse 100 000 FCFA en mars 2025, 1 000 000 FCFA sur 2025. Même question au 31/12 : voir le point 96.
   À qui : DCP
   Question : dans un tableau du mois ou du trimestre, le montant d'un nouveau est-il ce qui lui est payé dans la période, ou le montant mensuel de sa pension ou de sa rente ?
   Réponse :

109. La part CNSS d'une coordination (T42)
   C'est quoi : pour T42, le recueil demande « uniquement la partie CNSS » d'une pension de coordination, sans dire si c'est pour les mois de travail, pour le montant, ou pour les deux.
   Objectif : dans T42, les colonnes « services passés » et « montant » doivent montrer la seule part CNSS d'une pension de coordination (recueil v7, l. 298). Elles servent à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de savoir si cette part vaut pour les deux colonnes, alors que 234 nouveaux retraités payés par l'autre caisse y ont aujourd'hui un montant nul.
   Dans la base : pour les mois, PE_MOIS_COTISATION moins PE_COT_AUTRE (voir le point 11) ; pour le montant, les paiements de la CNSS (DEBOURS), donc rien quand DOS_CATEGORIE = COOR_AC.
   Aujourd'hui : on applique la part CNSS aux deux.
   Chiffre : 234 nouveaux retraités payés par l'autre caisse en 2024 comptent dans T42, avec un montant nul.
   À qui : DCP
   Question : pour T42, la part CNSS vaut-elle pour les mois de travail, pour le montant, ou pour les deux ?
   Réponse :

110. Quand la CNSS paie une coordination
   C'est quoi : la CNSS peut verser toute une pension de coordination, puis se faire rembourser la part de l'autre caisse. Le montant de T42 serait alors trop haut.
   Objectif : dans T42, la colonne « montant » doit montrer la seule part CNSS des pensions de coordination (recueil v7, l. 298). Elle permet de connaître ce que les nouveaux retraités coûtent vraiment à la CNSS, pour la loi des entrées (Kaya 2017). La réponse permet de savoir si, pour les 123 nouveaux retraités dont la CNSS paie toute la pension, il faut retirer la part que l'autre caisse rembourse.
   Dans la base : DOS_CATEGORIE = COOR_CNSS ; le montant est la somme des paiements (DEBOURS).
   Aujourd'hui : le montant est ce que la CNSS a payé.
   Chiffre : 123 nouveaux retraités COOR_CNSS saisis en 2024, révisions comprises.
   À qui : DCP
   Question : quand la CNSS paie une pension de coordination, son paiement comprend-il la part de l'autre caisse ?
   Réponse :

111. Les nouvelles pensions des anciens retraités
   C'est quoi : un retraité peut recevoir une nouvelle pension dans l'application quand la sienne est recalculée ou revalorisée. Ce n'est pas un nouveau retraité.
   Objectif : dans T42, l'effectif doit montrer les seules personnes qui deviennent retraitées dans l'année. Il sert à la loi des entrées, qui permet aux évaluations actuarielles de prévoir les pensions futures (Kaya 2017). La réponse permet de confirmer que les 220 nouvelles pensions de 2025 sont des révisions de pensions existantes, à écarter, et non de nouveaux retraités.
   Dans la base : une pension de vieillesse (PRESTATION_ESP, RPV ou RPRA) saisie dans l'année (PE_DATE_INSERT) pour une personne (IND_ID_PREST) qui en avait déjà une.
   Aujourd'hui : elles ne comptent pas comme nouvelles (voir le point 4).
   Chiffre : 220 cas en 2025, 47 en 2024 : près de cinq fois plus en un an.
   À qui : DCP, avec la DSI
   Question : ces 220 nouvelles pensions de 2025 sont-elles des révisions de pensions existantes ?
   Réponse :

112. L'invalide devenu retraité (T42)
   C'est quoi : le recueil demande à la DSI un module qui transformera une pension d'invalidité en pension de vieillesse à l'âge de la retraite. Il n'existe pas encore.
   Objectif : dans T42, l'effectif doit montrer les seules personnes qui entrent pour la première fois en retraite. Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de décider, avant que le module de conversion existe (recueil v7, l. 270), si un invalide changé en retraité à l'âge de la retraite y entre, alors qu'il était déjà pensionné.
   Dans la base : PRESTATION_ESP, TPE_CODE = RPI pour l'invalidité, RPV pour la vieillesse.
   Aujourd'hui : sans objet. Avec la règle de la première fois (voir le point 4), l'invalide converti serait compté comme nouveau retraité l'année de la conversion, alors qu'il était déjà pensionné.
   Chiffre : 119 pensionnés d'invalidité fin 2024 (tableau 18 de l'annuaire CNSS).
   À qui : DCP, avec la DSI
   Question : quand ce module existera, l'invalide converti comptera-t-il comme nouveau retraité dans T42 ?
   Réponse :

113. La dernière tranche d'âge (T43)
   C'est quoi : la grille de T43, les nouveaux veufs et veuves, s'arrête à « 60 ans et plus ».
   Objectif : dans T43, la dernière tranche d'âge doit montrer les nouveaux veufs et veuves de 60 ans et plus. Les tranches servent à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de garder la grille CIPRES telle quelle ou de détailler cette tranche, qui réunit 53 % des nouveaux veufs et veuves.
   Dans la base : pensions RPSV et RPSF (PRESTATION_ESP), âge tiré de la date de naissance (IND_DATE_NAISSANCE).
   Aujourd'hui : on garde la grille de l'Annuaire CIPRES telle quelle.
   Chiffre : 413 des 772 nouveaux veufs et veuves saisis en 2024 (53 %) tombent dans cette tranche.
   À qui : DCP, avec la DPC
   Question : pour T43, on garde la tranche « 60 ans et plus », ou on la détaille par tranches de 5 ans ?
   Réponse :

46. Les nouveaux crédirentiers (T45)
   C'est quoi : T45 compte les nouveaux rentiers ; le recueil dit « Nouveaux crédirentiers : rentiers ».
   Objectif : dans T45, l'effectif doit montrer tous les nouveaux rentiers d'accident du travail de l'année, victimes et ayants droit (recueil v7, l. 202). Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de confirmer ce périmètre, sans l'allocation d'incapacité : 102 nouveaux rentiers en 2024 dans la base pour 105 dans l'annuaire.
   Dans la base : PRESTATION_ESP, codes ARI (la victime), ARSV, ARSF, ARSE et ARSA (ses ayants droit). La personne compte l'année de sa première rente. L'allocation d'incapacité (AAI) n'y est pas.
   Chiffre : 102 nouveaux rentiers en 2024 dans la base ; les tableaux 75 et 77 de l'annuaire CNSS en font 105 ensemble. L'annuaire fait pareil pour les orphelins de rente : 44 pour 44 publiés.
   À qui : DP et DR
   Question : T45 compte-t-il toutes les nouvelles rentes d'accident du travail, celles des victimes et celles de leurs ayants droit ?
   Réponse :

47. Les ascendants de rente (T47)
   C'est quoi : un ascendant est le père ou la mère d'une victime décédée. Le titre de T47 ne nomme pas la branche, mais « crédirentier » veut dire rente, donc accident du travail.
   Objectif : dans T47, l'effectif doit montrer les pères et mères de victimes décédées qui reçoivent pour la première fois une rente d'accident du travail. Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de confirmer qu'on laisse de côté les ascendants de la branche des pensions, qui n'ont pas de tableau des nouveaux.
   Dans la base : la rente d'ascendant, code ARSA (PRESTATION_ESP). La pension d'ascendant (RPA) n'a pas de tableau des nouveaux.
   Aujourd'hui : T47 compte les seules rentes ARSA, comme l'atelier de Kaya en 2017 et le tableau 77 de l'annuaire CNSS.
   Chiffre : 15 nouveaux ascendants de rente en 2024 dans la base, 13 dans le tableau 77 (voir le point 114).
   À qui : DP et DR
   Question : on garde dans T47 les seuls ascendants des rentes d'accident du travail (ARSA) ?
   Réponse :

114. Le compte des ascendants (T47)
   C'est quoi : un ascendant de rente est le père ou la mère d'une victime décédée d'un accident du travail.
   Objectif : dans T47, l'effectif doit montrer les nouveaux ascendants de rente, comptés comme dans l'annuaire de la CNSS (annuaire CNSS 2024, tableau 77). Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de comprendre pourquoi l'annuaire en compte 13 et la base 15, pour reprendre sa règle ou s'en écarter en connaissance de cause.
   Dans la base : la première rente ARSA de la personne (PRESTATION_ESP) saisie dans l'année (PE_DATE_INSERT), avec sa date d'effet (PE_DATE_EFFET) et sa radiation (PE_DATE_RADIATION).
   Aujourd'hui : les 15 sont comptés ; on les regardera un par un.
   Chiffre : l'annuaire CNSS 2024 compte autrement : 13 nouveaux ascendants de rente en 2024 dans son tableau 77, 15 dans la base, à la saisie comme à la notification.
   À qui : DP et DR, avec la DPC
   Question : votre tableau 77 écarte-t-il des ascendants selon la date d'effet de leur rente, leur sortie dans l'année, ou leur lien avec la victime ?
   Réponse :

48. Les nouvelles rentes viagères (T48)
   C'est quoi : la rente viagère est la rente de la victime elle-même, versée à vie.
   Objectif : dans T48, l'effectif doit montrer les victimes qui reçoivent leur première rente viagère dans l'année. Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de choisir la date qui fait l'entrée, saisie, notification ou effet, pour retrouver les 44 nouvelles rentes viagères publiées en 2024 (annuaire CNSS 2024, tableau 75).
   Dans la base : la première rente ARI de la personne (PRESTATION_ESP) saisie dans l'année (PE_DATE_INSERT) ; sa notification est dans PE_DATE_NOTIFICATION.
   Aujourd'hui : on compte la première rente viagère de la personne ; le compte à la notification ou à la date d'effet reste à essayer sur la base.
   Chiffre : l'annuaire CNSS 2024 compte autrement : son tableau 75 publie 44 nouvelles rentes viagères pour 2024. La base en a 39 la première fois, 57 notifiées dans l'année.
   À qui : DP et DR, avec la DPC
   Question : pour vos 44 nouvelles rentes viagères de 2024, une victime compte-t-elle l'année où sa première rente est saisie, notifiée, ou prend effet ?
   Réponse :

115. Le taux d'incapacité (T48)
   C'est quoi : l'IPP (incapacité permanente partielle) est le taux d'incapacité de la victime, en %. Pour T48, le recueil demande la moyenne des taux de chaque tranche ; une rente peut être recalculée dans l'année avec un autre taux.
   Objectif : dans T48, la colonne « degré IPP » doit montrer, pour chaque tranche, le taux d'incapacité moyen des nouvelles victimes (recueil v7, l. 222). Elle permet de décrire la gravité des incapacités des nouveaux rentiers, utile à la loi des entrées (Kaya 2017). La réponse permet de choisir le taux à l'entrée ou le dernier taux de l'année quand une rente est recalculée.
   Dans la base : la rente viagère (PRESTATION_ESP, code ARI) et son taux (PE_TAUX_INCAPACITE).
   Aujourd'hui : on prend le taux de la dernière rente viagère de l'année.
   Chiffre : de 2020 à 2025, le taux va de 15 à 100 %. Parmi les 39 nouveaux de 2024, au plus 1 personne a eu deux rentes dans l'année.
   À qui : DP et DR
   Question : pour T48, prend-on le taux de la victime à son entrée, ou son dernier taux de l'année ?
   Réponse :

116. La retraite anticipée des nouveaux (T42)
   C'est quoi : une pension de vieillesse prise avant l'âge normal de la retraite. T42 n'a pas de ligne par type de pension.
   Objectif : dans T42, l'effectif doit montrer tous les nouveaux retraités de l'année, retraite anticipée comprise. Il sert à la loi des entrées des évaluations actuarielles (Kaya 2017). La réponse permet de confirmer qu'on y range les quelques départs anticipés de chaque année, comme l'annuaire qui en compte 3 en 2024 dans ses 2 489 nouveaux retraités.
   Dans la base : une prestation RPRA (TPE_CODE), la première de la personne, saisie dans l'année (PE_DATE_INSERT).
   Aujourd'hui : on la compte dans T42, avec les nouveaux retraités de vieillesse.
   Chiffre : environ 4 nouvelles retraites anticipées par an dans la base. L'annuaire CNSS 2024 en publie 3 à part en 2024 ; avec elles, il compte 2 489 nouveaux retraités, la base 2 463.
   À qui : DCP
   Question : la retraite anticipée compte-t-elle dans T42, avec les nouveaux retraités de vieillesse ?
   Réponse :

T50 à T57, l'évolution sur cinq ans

49. Le bénéficiaire de l'année
   C'est quoi : T50 à T55 comptent, sur cinq ans, les personnes qui ont reçu une prestation dans l'année ; T51 pour l'allocation de survivants, T52 l'indemnité journalière d'accident du travail, T53 l'allocation d'incapacité.
   Objectif : dans T51, T52 et T53, l'effectif doit montrer les personnes qui ont reçu l'allocation de survivants, l'indemnité journalière ou l'allocation d'incapacité. Il permet de suivre ces bénéficiaires d'une année à l'autre, pour les évaluations actuarielles (Kaya 2017). La réponse permet de les compter comme dans T50 et T54, à la liquidation et non au paiement : 571 et non 420 dans T51 en 2024.
   Dans la base : une personne compte l'année où une prestation du tableau lui est notifiée (PRESTATION_ESP, PE_DATE_NOTIFICATION).
   Aujourd'hui : on compte de même T51, T52, T53 et T55, dont l'annuaire CNSS 2024 ne publie pas l'effectif. L'atelier de Kaya en 2017 le demande pour T54 et T55.
   Chiffre : l'annuaire CNSS 2024 fait pareil pour T50 : 831 allocataires de vieillesse liquidés en 2024 pour 833 publiés (tableau 24). En comptant plutôt les personnes payées, T51 aurait 420 personnes en 2024, au lieu de 571.
   À qui : DCP
   Question : pour T51, T52 et T53, on garde la personne dont la prestation est liquidée dans l'année, comme pour T50 et T54 ?
   Réponse :

50. Une personne ou chaque prestation
   C'est quoi : une même personne peut recevoir plusieurs prestations du même tableau dans l'année : une victime dont l'arrêt de travail est prolongé deux fois reçoit trois indemnités journalières.
   Objectif : dans T50 à T55, et d'abord T52, l'effectif doit montrer des personnes et non des prestations : 195 victimes en 2024, pas 371 indemnités. Il permet de suivre combien de personnes touchent chaque prestation, pour les évaluations actuarielles (Kaya 2017). La réponse permet de confirmer qu'une victime dont l'arrêt est prolongé compte une seule fois, avec toutes ses indemnités dans le montant.
   Dans la base : l'ayant droit de la prestation (PRESTATION_ESP, IND_ID_PREST) ; pour un orphelin, l'orphelin, pas son tuteur.
   Aujourd'hui : on compte chaque personne une fois par tableau et par année, avec toutes ses prestations dans le montant, comme au 31/12 (voir le point 8).
   Chiffre : 195 victimes ont eu une indemnité journalière liquidée en 2024, pour 371 indemnités.
   À qui : DCP
   Question : on garde chaque personne comptée une seule fois par tableau et par année ?
   Réponse :

51. Des paiements d'accident introuvables
   C'est quoi : une partie de l'argent versé pour les accidents du travail manque dans la base des prestations : la plupart des soins, une part des rentes et des indemnités journalières.
   Objectif : dans T56, la ligne des accidents du travail, comme les montants de T52 et T53, doit montrer tout l'argent versé aux victimes et à leurs familles. Elle permet de suivre ce que coûte la branche, comme l'annuaire (annuaire CNSS 2024, tableau 90). La réponse permet de retrouver les soins et les paiements absents de la base, pour lever la mention « incomplet ».
   Dans la base : les paiements (DEBOURS, DEB_TYPE = PM, statut D ou P) et les remboursements de soins (DEB_TYPE = RB) des dossiers d'accident du travail (DOSSIER, TDOS_CODE = A).
   Aujourd'hui : on prend ce que la base contient ; les montants des rentes portent la mention « incomplet ».
   Chiffre : l'annuaire CNSS 2024 compte plus. Soins de 2024 : 18,8 M dans la base pour 139,7 M publiés. Rentes viagères de 2024 : 399,4 M pour 455,0 M publiés (tableau 90).
   À qui : DSI, avec la DP et les DR
   Question : ces paiements sont-ils enregistrés ailleurs que dans la base des prestations (une autre application, la comptabilité) ?
   Réponse :

52. Les rachats de rente (T56)
   C'est quoi : un rachat de rente est un capital versé en une fois à la victime d'un accident du travail, à la place de sa rente.
   Objectif : dans T56, la ligne des accidents du travail doit montrer tout l'argent versé dans l'année, rachats de rente compris : 21,5 M en 2024. Elle permet de récapituler les prestations servies par branche (Annuaire CIPRES, p. 16), comme l'annuaire (annuaire CNSS 2024, tableau 90). La réponse permet de garder ou non, parmi les prestations servies, ce capital versé à la place d'une rente.
   Dans la base : ce n'est pas un paiement ordinaire, mais un débours d'ajustement (DEBOURS, DEB_TYPE = AJ) lié à un ajustement de code RR (AJUSTEMENT, TAJ_CODE = RR).
   Aujourd'hui : la requête de T56 ne le compte pas encore ; on propose de l'ajouter à la branche des accidents du travail. Les rappels de moins-perçus, versés de même, sont au point 33 ; pour T26, voir le point 78.
   Chiffre : 11 rachats versés en 2024 dans la base, pour 21,5 M ; l'annuaire CNSS 2024 en publie 25,4 M, parmi les prestations des accidents du travail (tableau 90).
   À qui : DPC, avec la DCP
   Question : on garde le rachat de rente parmi les prestations servies de T56 ?
   Réponse :

53. La ligne « Autres » (T56)
   C'est quoi : T56 additionne l'argent versé par branche ; « Autres » est sa dernière ligne. L'action sanitaire et sociale est ce que la CNSS mène en plus de ses branches, par exemple ses centres de santé.
   Objectif : dans T56, la ligne « Autres » doit montrer l'argent de l'action sanitaire et sociale, à côté des trois branches. Elle permet que le total de T56 récapitule tout ce que la CNSS sert (Annuaire CIPRES, p. 16), comme l'annuaire (annuaire CNSS 2024, tableau 106). La réponse permet de choisir le montant à y mettre, 650,7 M ou 1 571,7 M en 2024, et de remplir cette ligne, vide aujourd'hui.
   Dans la base : ce montant n'est pas dans la base des prestations ; il doit être le même que dans les indicateurs de l'action sanitaire et sociale du recueil des besoins.
   Aujourd'hui : la ligne reste vide.
   Chiffre : l'annuaire CNSS 2024 met l'action sanitaire et sociale dans cette ligne (tableau 106), et donne un autre montant de ses dépenses (tableau 114).
   À qui : DPC
   Question : la ligne « Autres » prend-elle le montant de l'action sanitaire et sociale du tableau 106 (650,7 M en 2024), ou ses dépenses du tableau 114 (1 571,7 M en 2024) ?
   Réponse :

54. Qui compter dans T57
   C'est quoi : T57 compte, par année et par branche, les assurés décédés. Le recueil des besoins veut un filtre « assuré ou ayant droit ».
   Objectif : dans T57, la colonne des pensions doit montrer, chaque année, les assurés décédés (Kaya 2017). Elle permet de suivre les décès qui font sortir les pensionnés du stock et ouvrent des pensions aux survivants (Proposition BI). La réponse permet de dire si l'assuré mort en activité, sans pension à son nom mais dont la veuve devient pensionnée, entre dans ce chiffre.
   Dans la base : la date du décès (INDIVIDU, IND_DATE_DECES) et, en vieillesse, les titulaires d'une pension directe en cours (PRESTATION_ESP, RPV, RPRA, RPI) ; avec le filtre « ayant droit », aussi RPSV, RPSF, RPSE et RPA.
   Aujourd'hui : le filtre montre les assurés. Un assuré mort en activité n'est pas compté, faute de prestation à son nom, même si son décès ouvre une pension à sa veuve.
   Chiffre : 622 décès en 2024 parmi les titulaires d'une pension directe dans la copie du 02/02/2026, qui a des décès saisis tard (voir le point 2). L'annuaire CNSS 2024 publie 648 pensionnés décédés, dont 451 de droit direct (tableau 26).
   À qui : DCP, avec la DPC
   Question : un assuré mort en activité, dont le décès ouvre une pension ou une allocation de survivants, compte-t-il dans T57 ?
   Réponse :

55. La date d'un décès (T57)
   C'est quoi : un décès est souvent saisi des mois après. On peut le compter l'année où il a lieu, ou l'année où il est saisi.
   Objectif : dans T57, le nombre de retraités décédés d'une année doit montrer les décès survenus cette année-là, comparables aux 447 de l'annuaire pour 2024 (annuaire CNSS 2024, tableau 26). Il permet de suivre les décès des pensionnés d'une année à l'autre (Proposition BI). La réponse permet de ranger un décès saisi tard dans l'année où il a eu lieu, et non dans celle de sa saisie.
   Dans la base : la date du décès (INDIVIDU, IND_DATE_DECES). Le jour de la saisie n'est pas gardé : on s'en approche par la dernière modification de la pension (PRESTATION_ESP, PE_DATE_UPDATE).
   Aujourd'hui : un décès compte l'année où il a lieu, s'il est déjà saisi quand on calcule (voir le point 2).
   Chiffre : retraités morts en 2024 : 619 dans la copie de la base du 02/02/2026, environ 444 déjà saisis fin mars 2025 ; l'annuaire CNSS 2024 en publie 447 (tableau 26).
   À qui : DPC
   Question : on garde : un décès compte l'année où il a lieu, et non l'année où il est saisi ?
   Réponse :

56. Le sinistré (T57)
   C'est quoi : un sinistré est la victime d'un accident du travail. L'annuaire CNSS 2024 publie les « décès immédiats » : les victimes mortes sur le coup.
   Objectif : dans T57, la colonne des accidents du travail doit montrer les rentiers et les sinistrés décédés (Kaya 2017). Elle permet de suivre, chaque année, les victimes que les accidents du travail font mourir (Proposition BI). La réponse permet de dire si l'on compte seulement les victimes mortes sur le coup, comme l'annuaire, ou aussi celles qui meurent de leur accident des mois après, comme aujourd'hui.
   Dans la base : la date du décès de la victime (INDIVIDU, IND_DATE_DECES), face à la date de l'accident. Le champ DOS_DECES_IMMEDIAT (DOSSIER) n'est rempli que pour 2 % des accidents de 2024.
   Aujourd'hui : on compte toute victime morte dans l'année qui suit son accident.
   Chiffre : par année de l'accident, de 2020 à 2024 : 20, 17, 24, 16 et 9 victimes mortes dans l'année dans la base. L'annuaire CNSS 2024 compte autrement : il ne publie que les décès immédiats, 13, 11, 13, 11 et 14 (tableau 49).
   À qui : DCP
   Question : un sinistré décédé est-il seulement la victime morte sur le coup, ou aussi celle qui meurt de son accident des mois après ?
   Réponse :

57. Les décès d'ascendants (T57)
   Objectif : dans T57, avec le filtre « ayant droit », la colonne des pensions doit montrer aussi les pensionnés ascendants décédés, père ou mère d'un assuré. Elle permet d'expliquer les sorties de ce stock et sert aux calculs actuariels (Proposition BI). La réponse permet de comprendre les 89 décès publiés, plus nombreux que les pensions d'ascendant, et de comparer T57 à l'annuaire.
   Dans la base : les pensions d'ascendant (PRESTATION_ESP, code RPA) et le décès de leur titulaire (INDIVIDU, IND_DATE_DECES).
   Aujourd'hui : sans effet sur T57 : avec le filtre « assuré », les ascendants ne comptent pas.
   Chiffre : l'annuaire CNSS 2024 publie 89 décès de pensionnés ascendants en 2024 (tableau 26), alors qu'il compte 54 pensions d'ascendant fin 2023 et 7 nouvelles en 2024 (tableaux 18 et 23).
   À qui : DPC
   Question : ces 89 décès comptent-ils d'autres personnes que les titulaires d'une pension d'ascendant ?
   Réponse :

117. Le montant liquidé de la maternité
   C'est quoi : le recueil des besoins demande pour T54 (indemnités de maternité) et T55 (allocations prénatales) le « montant liquidé » : ce qui est dû pour les prestations liquidées dans l'année, même payé plus tard.
   Objectif : dans T54 et T55, la colonne des montants doit montrer le montant liquidé : tout ce qui est dû pour les prestations de maternité accordées dans l'année, même payé plus tard (Kaya 2017 ; recueil v7, l. 159). Elle permet de suivre le coût de la maternité d'une année à l'autre, pour les évaluations actuarielles (Kaya 2017). La réponse permet d'y compter les paiements faits l'année suivante.
   Dans la base : les paiements (DEBOURS, DEB_TYPE = PM, statut D ou P) des prestations MIJ, MAP et FAP notifiées dans l'année (PRESTATION_ESP, PE_DATE_NOTIFICATION), quelle que soit la date du paiement.
   Aujourd'hui : la requête prend encore les paiements de l'année ; elle passera au montant liquidé quand elle sera revue.
   Chiffre : indemnités de maternité de 2024 : 2,75 Md payés dans l'année, pour 2,87 Md de « montants perçus » dans l'annuaire CNSS 2024 (tableau 35). Le montant liquidé reste à mesurer.
   À qui : DCP
   Question : pour T54 et T55, le montant liquidé est-il tout ce qui est dû pour les prestations liquidées dans l'année, même payé l'année suivante ?
   Réponse :

118. L'allocation prénatale de l'épouse (T55)
   C'est quoi : l'allocation prénatale va à la salariée enceinte, ou à l'épouse enceinte d'un salarié. L'épouse non salariée n'a pas droit à l'indemnité de maternité.
   Objectif : dans T55, la ligne des allocations prénatales doit montrer toutes les femmes qui les reçoivent, salariées ou épouses de salariés. Elle permet de suivre ces bénéficiaires et les montants d'une année à l'autre, pour les évaluations actuarielles (Kaya 2017). La réponse ne change pas le chiffre : elle aide à lire le tableau, en disant qui reçoit chacune des deux sortes d'allocation prénatale.
   Dans la base : les codes MAP et FAP (PRESTATION_ESP, TPE_CODE).
   Aujourd'hui : T55 compte les deux codes : la réponse aide seulement à lire le tableau.
   Chiffre : en 2024, 3 857 femmes ont une allocation MAP et 56 une allocation FAP, pour 3 890 mères qui ont une indemnité de maternité.
   À qui : DCP
   Question : l'épouse non salariée d'un assuré reçoit-elle l'allocation prénatale sous le code FAP plutôt que MAP ?
   Réponse :

119. Les ayants droit des prestations familiales (T57)
   C'est quoi : avec le filtre « ayant droit » de T57, on montre les ayants droit décédés : en vieillesse et en accidents du travail, les survivants pensionnés ou rentiers. En prestations familiales, c'est moins clair.
   Objectif : dans T57, avec le filtre « ayant droit », la colonne des prestations familiales doit montrer les ayants droit décédés, comme les autres branches. Ce filtre suit les décès des survivants pour expliquer les sorties du stock et servir aux calculs actuariels (Proposition BI). La réponse permet de dire si les conjoints et les enfants des allocataires y entrent, ou si la colonne reste vide.
   Dans la base : le titulaire du dossier (DOSSIER, TDOS_CODE = F) dont une allocation familiale (PRESTATION_ESP, code FAF) est en cours à son décès (INDIVIDU, IND_DATE_DECES).
   Aujourd'hui : on ne compte que les allocataires : avec le filtre « ayant droit », la colonne des prestations familiales reste vide.
   Chiffre : 194 allocataires décédés en 2024 dans la base. L'annuaire CNSS 2024 ne publie pas ces décès, mais compte 39 856 conjoints et 268 481 enfants bénéficiaires en 2024 (tableau 37).
   À qui : DCP, avec la DPC
   Question : avec le filtre « ayant droit », compte-t-on dans les prestations familiales les conjoints et les enfants d'allocataires décédés ?
   Réponse :

120. Le total des branches (T57)
   C'est quoi : le recueil des besoins remplace la colonne « Autres » de T57 par un TOTAL des branches. Une même personne peut être dans deux branches : par exemple un retraité qui touche encore des allocations familiales.
   Objectif : dans T57, la colonne TOTAL doit montrer, chaque année, les décédés de toutes les branches (recueil v7, l. 173 ; Kaya 2017). Elle permet de suivre en un seul chiffre les décès qui touchent la CNSS (Proposition BI). La réponse permet de dire si ce total reste la somme des branches, où une personne présente dans deux branches compte deux fois.
   Aujourd'hui : le TOTAL est la somme des branches : cette personne y compte deux fois.
   À qui : DPC
   Question : une personne comptée dans deux branches compte-t-elle deux fois dans le TOTAL de T57 ?
   Réponse :

121. Les rentiers décédés (T57)
   C'est quoi : une rente radiée est une rente arrêtée, par exemple faute de certificat de vie.
   Objectif : dans T57, la colonne des accidents du travail doit montrer les rentiers décédés de l'année : 16 dans la base, 24 dans l'annuaire (annuaire CNSS 2024, tableau 81). Elle permet de suivre les sorties du stock des rentiers (Proposition BI). La réponse permet de compter ou non le rentier dont la rente a été arrêtée, pour une raison administrative, avant son décès.
   Dans la base : les rentes (PRESTATION_ESP, codes ARI, ARSV, ARSF, ARSE, ARSA), leur radiation (PE_DATE_RADIATION) et le décès du titulaire (INDIVIDU, IND_DATE_DECES).
   Aujourd'hui : T57 compte le rentier dont la rente est en cours à son décès : non radiée, ou radiée au plus tôt 31 jours avant.
   Chiffre : 16 rentiers décédés en 2024 dans la base ; l'annuaire CNSS 2024 en compte 24 (tableau 81).
   À qui : DPC, avec la DCP
   Question : dans l'annuaire, un rentier décédé doit-il avoir une rente en cours à son décès ?
   Réponse :

122. Les années passées d'un tableau
   C'est quoi : T50 à T57 montrent l'année calculée et les quatre d'avant. En recalculant, les années passées bougent, à cause des décès, radiations et rappels saisis tard.
   Objectif : dans T50 à T57, les quatre années d'avant doivent montrer les chiffres de chaque année passée, à côté de l'année calculée. Elles permettent de suivre l'évolution annuelle de chaque prestation (Kaya 2017). La réponse permet de choisir entre les chiffres déjà publiés, avec une note en cas de correction (annuaire CNSS 2024, tableau 114), et un recalcul : 1,53 Md ou 2,19 Md pour la maternité de 2020.
   Aujourd'hui : les quatre années passées sont recalculées sur la copie de la base du moment.
   Chiffre : indemnités de maternité de 2020 : 1,53 Md publiés dans l'annuaire CNSS 2024 (tableau 35), 2,19 Md en recalculant sur la base d'aujourd'hui. L'annuaire signale parfois une année passée corrigée, avec une note (tableau 114).
   À qui : DPC
   Question : dans un tableau sur cinq ans, les années passées reprennent-elles les chiffres déjà publiés ?
   Réponse :

123. La source des montants (T56)
   C'est quoi : T56 donne, par branche, l'argent versé chaque année. L'annuaire CNSS 2024 y met les charges techniques de sa comptabilité.
   Objectif : dans T56, chaque branche doit montrer l'argent servi dans l'année. Elle permet de récapituler les prestations servies par branche (Annuaire CIPRES, p. 16), comme l'annuaire, qui y met les charges techniques de sa comptabilité (annuaire CNSS 2024, tableau 106). La réponse permet de choisir la source des montants : les paiements de l'application, justes au total mais faibles en accidents du travail, ou la comptabilité.
   Dans la base : les paiements de l'application (DEBOURS), ou les comptes de prestations de la comptabilité (601, 602, 603 et 605), lus par la vue V_BI_BALANCE_COMPTABLE.
   Aujourd'hui : on prend les paiements de l'application.
   Chiffre : en 2024, les paiements, soins remboursés compris, sont à 0,2 % près du total publié des trois branches (tableau 106), mais à −28,9 % en accidents du travail (voir le point 51).
   À qui : DPC, avec la DSI
   Question : pour T56, prend-on les paiements de l'application des prestations, ou les charges techniques de la comptabilité ?
   Réponse :

124. Un tableau absent du recueil (T56)
   C'est quoi : le recueil des besoins décrit chaque tableau à produire. Il n'a aucune ligne pour T56, les prestations servies par branche.
   Objectif : le tableau T56 doit montrer, sur cinq ans, l'argent servi par chaque branche. Il permet de récapituler toutes les prestations servies par branche (Annuaire CIPRES, p. 16) ; l'annuaire en publie l'équivalent (annuaire CNSS 2024, tableau 106). La réponse permet de savoir si la BI doit produire ce tableau, absent du recueil, ou l'abandonner.
   Aujourd'hui : on produit T56 comme le décrit l'Annuaire CIPRES ; l'annuaire CNSS 2024 en publie l'équivalent (tableau 106). Même question pour T26, T39 et T40 (voir le point 73).
   À qui : DPC
   Question : T56 reste-t-il à produire ?
   Réponse :

125. Le ratio des prestations sur les cotisations
   C'est quoi : le recueil des besoins demande, par branche, le « ratio des ressources de cotisations allouées aux prestations » : les prestations notifiées de la période divisées par les cotisations, en pourcentage. Les soins sont des prestations en nature ; pensions et allocations, en espèces.
   Objectif : l'indicateur doit montrer, par branche, la part des cotisations qui part en prestations notifiées, en pourcentage (recueil v7, l. 1129). Il permet de voir si les cotisations de chaque branche suffisent à payer ses prestations (Proposition BI). La réponse permet de dire si les soins entrent dans le calcul, comme dans T56, ou seulement les pensions et les allocations.
   Aujourd'hui : le ratio n'est pas encore calculé ; on prévoit d'y compter les soins, comme dans T56.
   Chiffre : l'annuaire CNSS 2024 ne publie pas ce ratio. Repère recalculé pour 2024 avec ses tableaux 103 à 105 (charges techniques divisées par produits techniques) : pensions 45,96 %, prestations familiales 25,91 %, risques professionnels 9,82 %.
   À qui : DAI, avec la DPC
   Question : ce ratio compte-t-il aussi les soins, ou seulement les prestations en espèces ?
   Réponse :

126. Les nouvelles prestations promues
   C'est quoi : le recueil des besoins demande un indicateur annuel, la « proportion de nouvelles prestations promues », suivi par la DCP. Il donne son nom et sa fréquence, pas sa formule, et écrit lui-même qu'il est à confirmer avec la DAI et la DPC.
   Objectif : l'indicateur annuel doit montrer, en pourcentage, la place des nouvelles prestations promues par la CNSS (recueil v7, l. 1340). Il permet à la DCP de suivre son objectif de promouvoir de nouvelles prestations (recueil v7, l. 1338). La réponse permet de savoir quoi compter pour le calculer ; sans elle, l'indicateur reste vide.
   Aujourd'hui : l'indicateur n'est pas calculé.
   À qui : DCP, avec la DAI et la DPC
   Question : pouvez-vous nous donner la formule de cet indicateur ?
   Réponse :

127. Le document de référence des tableaux
   C'est quoi : le recueil des besoins annonce un « document de référence des tableaux de la CNSS », sans le nommer ni le joindre. S'il définit des tableaux, il pourrait passer avant l'Annuaire CIPRES, pour tous les tableaux.
   Objectif : dans tous les tableaux T25 à T57, chaque ligne et chaque colonne doivent suivre la définition retenue par la CNSS. Le recueil annonce un document qui fixe ces définitions (recueil v7, l. 81). La réponse permet de vérifier nos règles sur ce document, qui pourrait passer avant l'Annuaire CIPRES, et de corriger les tableaux s'il le faut.
   Aujourd'hui : on s'appuie sur le recueil des besoins, l'atelier de Kaya en 2017, l'Annuaire CIPRES et l'annuaire CNSS 2024.
   À qui : DPC
   Question : pouvez-vous nous donner ce document ?
   Réponse :

128. Pension ou allocation de vieillesse (T50)
   C'est quoi : une allocation de vieillesse va à un assuré dont la carrière est trop courte pour une pension. T50 donne la moyenne de ses mois cotisés (les services passés).
   Objectif : dans T50, la colonne « services passés moyens » doit montrer la vraie longueur des carrières courtes, environ 8 ans aujourd'hui. Elle permet de suivre, d'une année à l'autre, si les allocataires ont cotisé plus ou moins longtemps, et sert aux évaluations actuarielles (Kaya 2017). La réponse permet d'écarter de cette moyenne une carrière de 40 ans qui n'a rien à y faire.
   Dans la base : PRESTATION_ESP, TPE_CODE = RAV ; les mois cotisés sont dans PE_MOIS_COTISATION.
   Aujourd'hui : tous les mois de 1 à 600 entrent dans la moyenne de T50.
   Chiffre : en 2024, 831 allocataires, avec 95,5 mois cotisés en moyenne à la CNSS (environ 8 ans). Pourtant une allocation de vieillesse en a 489 (40 ans).
   À qui : DCP
   Question : au-delà de combien de mois cotisés un assuré a-t-il droit à une pension plutôt qu'à une allocation de vieillesse ?
   Réponse :

## Plutôt pour la DSI

58. Les statuts sans libellé
   C'est quoi : un statut est un code d'une lettre qui dit où en est une ligne (en cours, payée, fermée…). L'application n'en donne pas les libellés.
   Objectif : dans tous les tableaux T25 à T57, les chiffres doivent compter les bonnes lignes : les paiements faits, les prestations en cours, les demandes en instance, sans les lignes annulées. Aujourd'hui, ce tri suit le sens lu dans les données (Proposition BI). La réponse permet de confirmer ce sens et de fiabiliser chaque chiffre ; pour T31, elle ne change rien depuis 2015.
   Dans la base : le sens se lit dans les données pour le dossier (DOS_STATUT : A, F) et l'ajustement (AJ_STATUT : A, C, R). Restent inconnus : PE_STATUT (A, F, S), RD_STATUT (E, I, L, R, A), EFP_STATUT (A, D, I, N), NOT_STATUT (A, I), le statut I des ajustements et les cases « vérifié » (DEB_VERIFIE, DOS_VERIFIE…).
   Aujourd'hui : on applique le sens lu dans les données ; les ajustements au statut R sont écartés de T31 comme annulés, ce qui ne change rien depuis 2015 : ce statut n'apparaît plus depuis 2014.
   À qui : DSI
   Question : pouvez-vous nous donner la liste des libellés de ces statuts ?
   Réponse :

59. Les paiements sans effet
   C'est quoi : un paiement sans effet est approuvé et comptabilisé, mais aucun titre ne dit comment l'argent a été versé.
   Objectif : la part des prestations payées par virement doit montrer combien de paiements passent par la banque (recueil v7, l. 1085). Elle permet de suivre ce mode de paiement par prestation, direction régionale et agence (recueil v7, l. 1090). La réponse permet de classer les paiements sans titre, 62 % des prestations familiales de 2025, sans quoi cette part ne peut pas être juste.
   Dans la base : un débours de paiement (DEB_TYPE = PM) au statut P (DEB_STATUT) n'a pas d'effet (EFFET). Ce circuit existe depuis 2019.
   Aujourd'hui : on les compte comme payés. Mais le virement est un type d'effet (BVB) : sans effet, on ne sait pas si le paiement a été viré, alors que le recueil des besoins demande la part payée par virement.
   Chiffre : 62 % des paiements des prestations familiales de 2025 n'ont pas d'effet de paiement.
   À qui : DSI
   Question : par quel circuit passent les paiements au statut P : virement, téléphone mobile, ou un autre ?
   Réponse :

60. La date de décaissement
   C'est quoi : le décaissement est le jour où l'argent est réellement versé.
   Objectif : dans T31 à T56, chaque montant doit tomber dans la bonne période. Aujourd'hui, un paiement compte au mois qu'il paie ; le jour du versement pourrait servir s'il était toujours rempli (Proposition BI). La réponse permet de savoir si ce jour existe ailleurs dans la base, pour comparer les montants à l'annuaire, dont certains collent mieux au jour du versement.
   Dans la base : DEBOURS, DEB_DATE_DECAISSEMENT, et EFFET, EFP_DATE_DECAISSEMENT. Seuls des paiements au statut D l'ont ; ceux au statut P jamais.
   Aujourd'hui : on ne s'en sert pas : un paiement compte au mois qu'il paie.
   Chiffre : elle manque pour 80 % des paiements des prestations familiales et 43 % de ceux de vieillesse de 2025. Là où elle existe, elle colle parfois mieux : en 2024, les indemnités journalières d'accident du travail font 60,8 M au versement, plus près des 65,4 M de l'annuaire CNSS 2024 que la date de saisie.
   À qui : DSI
   Question : quand DEB_DATE_DECAISSEMENT est vide, où la base garde-t-elle le jour du versement ?
   Réponse :

61. Les débours de trop-perçu
   C'est quoi : un trop-perçu est souvent repris par des retenues sur la prestation.
   Objectif : dans T31, le taux d'indus doit montrer la part des sommes payées en trop (Kaya 2017) ; la réponse ne change pas ce chiffre. Elle sert à fiabiliser les données : savoir quels paiements liés à un trop-perçu sont des retenues et lesquels des versements, pour suivre le remboursement de chaque trop-perçu (Proposition BI).
   Dans la base : un débours de type AJ (DEB_TYPE) est relié par AJ_ID à un ajustement ; quand c'est un trop-perçu (TAJ_CODE = TP), son montant (DEB_MONTANT) est tantôt négatif, tantôt positif.
   Aujourd'hui : T31 lit le trop-perçu le mois où il est établi, pas ces débours : la réponse ne change pas le taux d'indus. Elle aiderait à voir qu'un trop-perçu est remboursé (voir le point 31).
   Chiffre : en 2024, ce montant va de −6,1 M à +6,6 M.
   À qui : DSI
   Question : parmi les paiements liés à un trop-perçu, lesquels sont des retenues, lesquels des versements ?
   Réponse :

62. Les mois cotisés
   C'est quoi : les mois de travail cotisés à la CNSS, additionnés pour les services passés de T42 et T50.
   Objectif : dans T42 et T50, la colonne des services passés moyens doit montrer la vraie durée de cotisation des nouveaux retraités et des allocataires (Kaya 2017). Elle sert aux évaluations actuarielles (Kaya 2017). La réponse permet de confirmer que la valeur 999 veut dire « inconnu », pour l'écarter de ces moyennes à bon droit.
   Dans la base : PRESTATION_ESP, PE_MOIS_COTISATION, recopié sur chaque prestation du dossier. Il vaut parfois 999.
   Aujourd'hui : une valeur de plus de 600 mois (50 ans) est écartée des services passés (voir le point 16).
   À qui : DSI
   Question : la valeur 999 veut-elle dire que les mois cotisés sont inconnus ?
   Réponse :

63. Les dossiers sans catégorie
   C'est quoi : la catégorie d'un dossier dit s'il est partagé avec une autre caisse, dans une coordination.
   Objectif : dans tous les tableaux T25 à T57, un dossier ordinaire doit compter en entier, et un dossier de coordination selon sa propre règle. Les 120 518 dossiers sans catégorie comptent aujourd'hui comme ordinaires (Proposition BI). La réponse permet de confirmer qu'aucun n'est partagé avec une autre caisse, sans quoi des bénéficiaires et des montants seraient comptés à tort.
   Dans la base : DOSSIER, DOS_CATEGORIE vide. Les autres valeurs sont COOR_AC, COOR_CNSS et CNSS_AC (voir le point 136).
   Aujourd'hui : un dossier sans catégorie est traité comme un dossier ordinaire : il compte partout.
   Chiffre : 120 518 dossiers n'ont pas de catégorie.
   À qui : DSI
   Question : un dossier sans catégorie est-il toujours un dossier ordinaire, sans coordination avec une autre caisse ?
   Réponse :

64. Les réceptions sans dossier
   C'est quoi : une réception NOUVEAU doit ouvrir un dossier ; une réception ANCIEN, sur un dossier existant, n'en ouvre pas, et c'est normal.
   Objectif : dans T25 à T27, les demandes en instance au 31/12 doivent montrer les dossiers encore à traiter, qui entrent dans le calcul des prestations à payer (Kaya 2017). La réponse permet de sortir de ce stock les réceptions abandonnées ou refusées sans dossier, ou de les y garder si elles attendent une pièce : environ 1 132 en pensions fin 2025.
   Dans la base : des réceptions (RECEPTION_DOSSIER, RD_TYPE_RECEPTION) ne sont reliées à aucun dossier (DOSSIER).
   Aujourd'hui : elles restent en instance, sauf celles d'avant 2016 ; en accidents du travail et en prestations familiales, elles en sortent 24 mois après le dépôt (voir le point 21).
   Chiffre : en pensions en 2025, 4 671 réceptions pour 3 638 dossiers ouverts. Fin 2025, environ 1 132 réceptions de pension de 2021 à 2025 restent sans dossier.
   À qui : DSI, avec la DCP
   Question : une réception NOUVEAU qui n'a ouvert aucun dossier est-elle abandonnée, refusée sans saisie du refus, ou en attente d'une pièce ?
   Réponse :

65. La copie de la base
   C'est quoi : l'entrepôt de données de la BI ne garde que la dernière copie de la base.
   Objectif : dans tous les tableaux d'une année, les chiffres doivent compter les décès, radiations et notifications saisis en retard, jusqu'à la date d'arrêt retenue (voir le point 2). La réponse permet de calculer chaque année sur la bonne copie de la base, et non sur celle du 02/02/2026, à laquelle il manque des saisies de 2026 pour l'année 2025 (Proposition BI).
   Dans la base : la copie date du 02/02/2026.
   Aujourd'hui : pour 2025, il manque les décès, radiations et notifications saisis en février et mars 2026. Si l'annuaire est arrêté vers la fin mars (voir le point 2), chaque année se calculera sur la copie de la base arrêtée fin mars de l'année suivante.
   À qui : DSI
   Question : la copie peut-elle être rafraîchie fin mars de chaque année ?
   Réponse :

66. La vue des décédés (T57)
   C'est quoi : la DSI a fait la vue V_PRESTATAIRES_DECEDES_BI pour les décédés de T57.
   Objectif : la réponse ne change aucun chiffre de T57 : la BI compte les décédés sans lire cette vue. Elle sert à protéger les données : retirer de la vue le nom, le prénom, l'adresse et le téléphone des décédés, dont la BI n'a pas besoin pour compter (Proposition BI).
   Dans la base : elle prend toutes les prestations notifiées dont l'ayant droit a une date de décès, sans filtre de type ni d'année, avec le nom, le prénom, l'adresse et le téléphone des décédés.
   Aujourd'hui : la BI ne lit pas cette vue : T57 part des tables INDIVIDU, PRESTATION_ESP et DOSSIER.
   À qui : DSI
   Question : peut-on retirer le nom, le prénom, l'adresse et le téléphone de cette vue ?
   Réponse :

67. Les lieux de paiement
   C'est quoi : le lieu de paiement est l'endroit où une prestation est versée ; il dépend d'une direction régionale et d'un service.
   Objectif : la réponse ne change aucun chiffre : les tableaux rangent chaque dossier dans la direction régionale et l'agence du dossier, pas dans celles du lieu de paiement. Elle sert à fiabiliser une donnée de référence, utile si la CNSS veut un jour des chiffres par lieu de paiement (Proposition BI).
   Dans la base : dans LIEU_PAIEMENT, le lieu 201, « caisse tenkodogo », est rattaché à la DRB (DR_NO), sans service (SP_NO), alors que Tenkodogo dépend de la DRF.
   Aujourd'hui : la BI prend la direction régionale et l'agence du dossier, pas celles du lieu de paiement (voir le point 132) : ce rattachement ne change aucun chiffre.
   À qui : DSI
   Question : le rattachement du lieu 201 « caisse tenkodogo » à la DRB est-il une erreur ?
   Réponse :

68. Les dates de naissance approchées
   C'est quoi : une date approchée (« né vers 1930 ») est saisie quand on ne connaît pas le jour exact de la naissance.
   Objectif : dans T32 à T49, la tranche « 100 ans et plus » de la pyramide des âges doit montrer de vrais centenaires (Kaya 2017). La réponse permet de savoir si ces dates partagées sont des dates « né vers » ; si l'année est juste, aucun chiffre ne change : elle sert surtout à fiabiliser les âges.
   Dans la base : INDIVIDU, IND_DATE_NAISSANCE : parmi les personnes de 100 ans et plus, plusieurs nées la même année partagent la même date, rarement un 1er janvier.
   Aujourd'hui : elles restent dans la tranche « 100 ans et plus ». L'âge se calcule sur la seule année de naissance (voir le point 9) : une date approchée ne change rien si l'année est juste.
   À qui : DSI
   Question : ces dates partagées sont-elles des dates approchées (« né vers ») ?
   Réponse :

129. Les frais médicaux de maternité
   C'est quoi : les frais de grossesse que la CNSS prend en charge : accouchement, consultation, examens, hospitalisation, kit opératoire, pharmacie, radio. Le recueil des besoins veut une ligne « Frais médicaux de maternité » dans T27, et ces frais vont aussi dans les prestations servies de T56.
   Objectif : dans T27, la ligne « Frais médicaux de maternité » doit montrer les dossiers de ces frais de grossesse et leur délai de liquidation (recueil v7, l. 127) ; dans T56, leur montant entre dans les prestations familiales. La réponse permet de trouver où ces frais sont saisis, 25,0 M payés en 2024 selon l'annuaire (annuaire CNSS 2024, tableaux 40 et 41), pour remplir ces deux chiffres.
   Dans la base : la liste des types de prestation prévoit ces 7 frais, mais la base n'a aucun paiement (DEBOURS) de ces frais de 2020 à 2025.
   Aujourd'hui : cette ligne de T27 reste vide, et ces frais manquent dans les prestations familiales de T56.
   Chiffre : l'annuaire CNSS 2024 en publie 25,0 M payés en 2024 (tableaux 40 et 41).
   À qui : DSI, avec la DPC
   Question : où sont enregistrés les frais médicaux de maternité que la CNSS paie ?
   Réponse :

130. La date de validation (T27)
   C'est quoi : pour les prestations familiales, le recueil des besoins fait finir le délai de liquidation à une « date de validation », et non à la notification des droits comme l'atelier de Kaya en 2017.
   Objectif : dans T27 et l'indicateur du délai de liquidation des prestations familiales, la durée doit aller du dépôt du dossier complet à sa « date de validation » (recueil v7, l. 1097). Elle permet de comparer la durée moyenne à la norme CIPRES de 45 jours (Kaya 2017). La réponse permet de choisir, dans l'application, la date qui ferme ce délai.
   Dans la base : trois dates possibles : le calcul initial des droits, la signature de la notification imprimée (NOTIFICATION_PREST), l'approbation du premier paiement.
   Aujourd'hui : on prend la notification des droits (PE_DATE_NOTIFICATION), en attendant.
   À qui : DSI, avec la DES
   Question : la « date de validation » d'un dossier de prestations familiales est-elle le calcul initial, la signature de la notification, ou l'approbation du premier paiement ?
   Réponse :

131. Le délai des soins (T26)
   C'est quoi : le recueil des besoins mesure le délai des remboursements de soins d'accident du travail entre deux dates : la réception du certificat du médecin traitant, puis la convocation.
   Objectif : l'indicateur du délai moyen des remboursements de soins d'accident du travail doit aller de la réception du certificat du médecin traitant à la convocation (recueil v7, l. 1161). Il se suit par direction régionale et par agence (recueil v7, l. 1168). La réponse permet de trouver ces deux dates, sans lesquelles l'indicateur ne peut pas être calculé.
   Dans la base : ces deux dates n'ont pas été trouvées. Champs proches à tester : les pièces CME reçues au guichet (339 réceptions), la date de réception de l'avis du médecin conseil (DOS_DATE_REC_AVIS_MC), la date de rendez-vous de chaque réception.
   Aujourd'hui : pas d'indicateur. T26 mesure les soins de la déclaration au premier remboursement.
   À qui : DSI, avec la DP
   Question : dans quels champs de l'application sont saisies les deux dates du délai des soins ?
   Réponse :

132. L'agence d'un dossier
   C'est quoi : le recueil des besoins veut les délais, les taux et les effectifs par direction régionale et par agence.
   Objectif : dans tous les tableaux T25 à T57 et les indicateurs, chaque chiffre doit pouvoir se lire par direction régionale et par agence (recueil v7, l. 1102). Cela permet à chaque direction de suivre ses propres délais, taux et effectifs (Proposition BI). La réponse permet de confirmer qu'on range chaque dossier dans la bonne agence, alors que quatre agences citées par l'annuaire sont inactives dans la base.
   Dans la base : le service du dossier (DOSSIER, SP_NO), sinon celui de sa réception (RECEPTION_DOSSIER, SP_NO). La liste est SERVICE_PROVINCIAL : 15 agences et les 5 sièges des directions régionales.
   Aujourd'hui : on range chaque demande à l'agence de son dossier, sinon à celle de sa réception.
   Chiffre : l'annuaire CNSS 2024 cite 16 agences. Quatre d'entre elles sont inactives dans la base (SP_ACTIVE = N) : Tanghin, Sakaby, Diapaga, Tougan.
   À qui : DSI, avec la DPC
   Question : l'agence d'un dossier est-elle bien son service dans l'application (SP_NO) ?
   Réponse :

133. Le salaire moyen d'une pension
   C'est quoi : pour le taux de remplacement (voir le point 98), le recueil des besoins prend le salaire moyen des cinq meilleures années.
   Objectif : l'indicateur du taux de remplacement doit montrer la part du dernier revenu d'activité que la pension directe remplace (recueil v7, l. 1256). Il permet de juger si la pension suffit après la vie active (Proposition BI). La réponse permet de savoir si le salaire moyen gardé avec chaque pension est celui des cinq meilleures années, et donc de calculer ce taux, absent aujourd'hui.
   Dans la base : PRESTATION_ESP, PE_SAL_MOYEN, jamais étudié.
   Aujourd'hui : pas de taux de remplacement tant que ce champ n'est pas confirmé.
   À qui : DSI, avec la DCP
   Question : le salaire moyen enregistré avec chaque pension (PE_SAL_MOYEN) est-il bien le salaire moyen des cinq meilleures années ?
   Réponse :

134. Le montant calculé à la liquidation
   C'est quoi : si la base garde le montant calculé à la liquidation, on peut donner le montant liquidé de T54 et T55 (voir le point 117) sans attendre les derniers paiements.
   Objectif : dans T54 et T55, la colonne des montants doit montrer le montant liquidé de la maternité (Kaya 2017 ; recueil v7, l. 159). Elle permet de suivre le coût de la maternité d'une année à l'autre (Kaya 2017). La réponse permet, si la base garde le montant calculé à la liquidation, de le donner aussitôt, sans attendre les derniers paiements.
   Dans la base : on n'a pas trouvé ce montant. La table des étapes de calcul (ETAPE), au calcul initial (TEA_CODE = CIP), en porte peut-être un.
   Aujourd'hui : le montant liquidé est la somme des paiements de la prestation (DEBOURS, statut D ou P), quelle que soit leur date.
   À qui : DSI
   Question : un champ de la base garde-t-il le montant calculé à la liquidation d'une prestation ?
   Réponse :

135. La réception au statut A (T25)
   C'est quoi : le statut d'une réception est un code de l'application dont le sens n'est écrit nulle part.
   Objectif : dans T25, les demandes de pension en instance au 31/12 doivent montrer les dossiers encore à traiter, qui entrent dans le calcul des prestations à payer (Kaya 2017). La réponse permet de garder dans ce stock les 111 réceptions au statut A sans dossier si elles attendent, ou de les en sortir si elles sont annulées.
   Dans la base : RECEPTION_DOSSIER, RD_STATUT = A, pour une réception de pension reliée à aucun dossier.
   Aujourd'hui : on la compte en instance au 31/12 dans T25 ; annulée, elle en sortirait. Pour les autres réceptions sans dossier, voir le point 64.
   Chiffre : 111 réceptions de pension au statut A sans dossier depuis mai 2025.
   À qui : DSI
   Question : une réception de pension au statut A, sans dossier, est-elle en attente ou annulée ?
   Réponse :

136. La catégorie CNSS_AC
   C'est quoi : une catégorie de dossier, à côté de celles de la coordination, qu'on lit comme « dossier transféré à une autre caisse », qui le gère alors.
   Objectif : dans T25 à T27, les dossiers transférés à une autre caisse ne comptent pas parmi les demandes de la CNSS ; dans T32 et T34, la question touche 34 et 79 pensions fin 2025. Cela permet de ne compter que les dossiers que la CNSS gère (Proposition BI). La réponse confirme le sens de cette catégorie, sur lequel reposent ce retrait et le choix posé à la DCP.
   Dans la base : DOSSIER, DOS_CATEGORIE = CNSS_AC.
   Aujourd'hui : ces dossiers sont retirés de T25 à T27. Les pensions au 31/12 les gardent encore : ce choix est posé à la DCP (voir le point 101).
   Chiffre : fin 2025, 34 pensions de vieillesse (T32, dont 2 payées par la CNSS) et 79 pensions de veuves et de veufs (T34) sont dans des dossiers CNSS_AC.
   À qui : DSI
   Question : la catégorie CNSS_AC veut-elle bien dire que le dossier a été transféré à une autre caisse ?
   Réponse :

137. Les décès immédiats d'accident (T57)
   C'est quoi : un décès immédiat est la mort d'une victime le jour de son accident du travail, noté à la déclaration.
   Objectif : dans T57, la colonne des accidents du travail doit montrer les sinistrés décédés (Kaya 2017), à comparer aux décès immédiats publiés par l'annuaire (annuaire CNSS 2024, tableau 49). La réponse permet de retrouver les décès immédiats des accidents d'avant 2025, pour remplir les années passées de T57 si l'on retient la mort sur le coup (voir le point 56).
   Dans la base : DOSSIER, DOS_DECES_IMMEDIAT : rempli pour 2 % des accidents de 2024 et 65 % de ceux de 2025. TSINISTRE a un champ du même nom, pas encore lu.
   Aujourd'hui : un sinistré décédé est une victime morte dans l'année qui suit son accident (voir le point 56).
   Chiffre : pour les accidents de 2024, 9 victimes mortes dans l'année dans la base. L'annuaire CNSS 2024 compte autrement : la mort sur le coup, 14 décès immédiats (son tableau 49).
   À qui : DSI
   Question : où la base garde-t-elle les décès immédiats des accidents d'avant 2025 ?
   Réponse :

## Tous les tableaux, un par un

- T25, dossiers de pension par durée de liquidation (RPV, RPRA, RPI, RPSV, RPSF, RPSE, RPA, RAV, RAS, RASC, RASO, RASA, accessoires) : points 2, 5, 7, 13 à 15, 18 à 22, 58, 63 à 65, 72, 75 à 77, 127, 132, 135, 136.
- T26, dossiers d'accident du travail (ARI, ARSV, ARSF, ARSE, ARSA, AIJ, AAI, soins) : points 2, 7, 13 à 15, 18 à 26, 58, 63 à 65, 72 à 75, 78, 127, 131, 132, 136.
- T27, dossiers de prestations familiales et de maternité (FAF, MIJ, FAP, MAP, frais médicaux de maternité) : points 2, 7, 13 à 15, 18 à 22, 27, 28, 58, 63 à 65, 72, 75, 77, 127, 129, 130, 132, 136.
- T28, dossiers de pension payés et contrôlés a posteriori (RPV, RPRA sur sa propre ligne, RPI, RPSV, RPSF, RPSE, RPA, RAV, RAS, RASC, RASO, RASA, RRC ; accessoires avec leur pension) : points 2, 6, 7, 18, 29, 58, 65, 69, 70, 77, 79 à 85, 94, 127, 132.
- T29, dossiers d'accident du travail payés et contrôlés a posteriori (AIJ, ARI, ARSV, ARSF, ARSE, ARSA, AAI, rachat de rente RR, soins) : points 2, 6, 7, 18, 29, 58, 65, 69, 70, 77, 79 à 86, 94, 127, 132.
- T30, dossiers de prestations familiales payés et contrôlés a posteriori (FAP, MAP, MIJ, FAF, frais médicaux de maternité) : points 2, 6, 7, 18, 29, 58, 59, 65, 69, 70, 77, 79 à 85, 87, 94, 127, 132.
- T31, prestations payées, trop-perçus (TP, TP-CONV) et moins-perçus (MP, RC) par branche et par mois : points 1, 2, 6, 7, 17, 18, 30 à 33, 58 à 61, 65, 67, 88 à 95, 127, 132.
- T32, pensionnés de vieillesse (RPV), avec la retraite anticipée (RPRA) sur une ligne à part : points 1 à 3, 6, 8, 9, 11, 12, 18, 34 à 36, 43, 60, 63, 65, 68, 96 à 98, 101, 106, 127, 132, 133, 136.
- T33, pensionnés d'invalidité (RPI) : points 1 à 3, 6, 8, 9, 18, 35, 60, 65, 68, 96 à 98, 100, 127, 132, 133.
- T34, pensionnés veufs et veuves (RPSV, RPSF) : points 1 à 3, 6, 8, 9, 18, 35 à 37, 60, 65, 68, 71, 96, 101, 127, 132, 136.
- T35, pensionnés orphelins (RPSE) : points 1 à 3, 6, 8 à 10, 18, 35, 36, 60, 65, 68, 71, 96, 104, 105, 127, 132.
- T36, pensionnés ascendants (RPA) : points 1 à 3, 6, 8, 9, 18, 35, 60, 65, 68, 71, 96, 127, 132.
- T37, rentiers viagers, victimes d'accident du travail (ARI) : points 1 à 3, 6, 8, 9, 18, 35, 51, 60, 65, 68, 96, 127, 132.
- T38, rentiers veufs et veuves d'accident du travail (ARSV, ARSF) : points 1 à 3, 6, 8, 9, 18, 35, 51, 60, 65, 68, 71, 96, 103, 127, 132.
- T39, rentiers orphelins d'accident du travail (ARSE) : points 1 à 3, 6, 8, 9, 18, 35, 51, 60, 65, 68, 73, 96, 102, 105, 127, 132.
- T40, rentiers ascendants d'accident du travail (ARSA) : points 1 à 3, 6, 8, 9, 18, 35, 51, 60, 65, 68, 73, 96, 127, 132.
- T41, allocataires des prestations familiales, titulaires du dossier (FAF) : points 1, 2, 6, 8, 9, 18, 35, 38 à 42, 59, 60, 65, 68, 96, 99, 106, 127, 132.
- T42, nouveaux pensionnés de vieillesse et services passés (RPV, RPRA) : points 1, 2, 4, 6, 11, 16, 18, 44, 45, 60, 62, 63, 65, 68, 98, 106 à 112, 116, 127, 133.
- T43, nouveaux pensionnés veufs et veuves (RPSV, RPSF) : points 1, 2, 4, 6, 18, 45, 60, 65, 68, 71, 107, 108, 110, 113, 127.
- T44, nouveaux pensionnés orphelins (RPSE) : points 1, 2, 4, 6, 10, 18, 45, 60, 65, 68, 71, 107, 108, 110, 127.
- T45, nouveaux crédirentiers d'accident du travail, victimes et ayants droit (ARI, ARSV, ARSF, ARSE, ARSA) : points 1, 2, 4, 6, 18, 45, 46, 51, 60, 65, 68, 71, 107, 108, 127.
- T46, nouveaux crédirentiers orphelins (ARSE) : points 1, 2, 4, 6, 10, 18, 45, 46, 51, 60, 65, 68, 71, 107, 108, 127.
- T47, nouveaux crédirentiers ascendants, père ou mère d'une victime décédée (ARSA) : points 1, 2, 4, 6, 18, 45, 47, 51, 60, 65, 68, 71, 107, 108, 114, 127.
- T48, nouveaux bénéficiaires de rente viagère, la victime elle-même, et degré moyen d'incapacité (ARI) : points 1, 2, 4, 6, 18, 45, 48, 51, 60, 65, 68, 107, 108, 115, 127.
- T49, nouveaux crédirentiers veufs et veuves (ARSV, ARSF) : points 1, 2, 4, 6, 18, 45, 46, 51, 60, 65, 68, 71, 107, 108, 127.
- T50, allocations de vieillesse sur cinq ans, avec les services passés (RAV) : points 1, 2, 6, 11, 16, 18, 49, 50, 60, 62, 63, 65, 122, 126 à 128.
- T51, allocations de survivants : veufs et veuves, orphelins et ascendants (RASC, RASO, RASA ; RAS, l'ancien code, ne se liquide plus) : points 1, 2, 6, 18, 49, 50, 60, 65, 122, 126, 127.
- T52, indemnités journalières d'accident du travail (AIJ) : points 1, 2, 6, 18, 49 à 51, 60, 65, 122, 126, 127.
- T53, allocations d'incapacité (AAI) : points 1, 2, 6, 18, 49 à 51, 60, 65, 122, 126, 127.
- T54, indemnités de maternité (MIJ) : points 1, 2, 6, 18, 49, 50, 59, 60, 65, 117, 122, 126, 127, 134.
- T55, allocations prénatales (MAP, et FAP, l'ancien code des prestations familiales) : points 1, 2, 6, 18, 49, 50, 59, 60, 65, 117, 118, 122, 126, 127, 134.
- T56, prestations servies par branche (paiements, remboursements de soins et rachats de rente ; ligne « Autres » : action sanitaire et sociale) : points 1, 2, 6, 7, 18, 33, 51 à 53, 59 à 61, 65, 67, 88, 122 à 125, 127, 129.
- T57, assurés décédés par branche, avec un TOTAL (pension directe ; rentier et sinistré ; allocataire) : points 2, 7, 18, 54 à 57, 65, 66, 77, 119 à 122, 127, 137.

## Les points de chaque direction

- DCP, la Direction centrale des prestations : points 3 à 5, 7 à 14, 16 à 22, 30 à 37, 43, 45, 49, 50, 54, 56, 71, 72, 75, 77, 86 à 88, 92, 93, 96 à 98, 100, 101, 104, 108 à 113, 116 à 119, 126, 128.
- DP, le sigle du recueil pour les accidents du travail, à faire préciser : points 3, 13, 20, 21, 23 à 26, 35, 46 à 48, 72 à 75, 78, 96, 102, 103, 114, 115.
- DES, le sigle du recueil pour les prestations familiales, à faire préciser : points 13, 14, 20, 21, 27, 28, 35, 38, 39, 41, 72, 75, 96, 99.
- DR, les directions régionales : points 3, 13, 14, 20, 21, 23 à 28, 35, 38, 39, 41, 46 à 48, 72 à 75, 78, 96, 99, 102, 103, 114, 115.
- DAI, la Direction de l'audit interne : points 15, 29, 69, 70, 79 à 85, 89 à 91, 94, 125.
- DSI, la Direction des systèmes d'information : points 6, 51, 58 à 68, 129 à 137.
- DPC, la Direction de la prospective et de la coopération, qui fait l'annuaire : points 1, 2, 40, 42, 44, 52, 53, 55, 57, 76, 105 à 107, 120 à 124, 127.
- DCR, la Direction centrale du recouvrement : point 95.
