# Les règles des tableaux CIPRES

Prestations (T25 à T57) et GRH (T58 à T65). État au 09/11/2026.

Voici les règles que la BI applique pour produire chaque tableau. Elles s'appliquent dès maintenant. Une direction qui veut une autre règle le demande, et la BI la change partout où elle sert.
Les lignes « À faire » disent ce que la CNSS doit saisir ou fournir pour que les tableaux soient justes.
Entre parenthèses, on note d'où vient la règle dans les documents de la BI.

## Pour tous les tableaux

1. Les directions font les corrections de l'année N dans l'application avant la fin de janvier N+1. (point 2)
2. À la fin de janvier N+1, la BI reprend la photo de la base. Cette photo fait foi pour tous les tableaux de l'année N. (remplace R13)
3. Seul compte ce qui est saisi dans l'application. Une correction faite à côté (fichier, mission en région, compte de résultat) n'entre pas dans les tableaux. (proposition BI)
4. Un événement compte à sa date (un décès à la date du décès), s'il est saisi avant la photo officielle. Saisi plus tard, il n'entre que dans les photos suivantes. (T50-T57 R13)

## Prestations (T25 à T57)

### Pour tous les tableaux des prestations

5. La branche est celle du dossier : V pour les pensions, A pour les accidents du travail et maladies professionnelles, F pour les prestations familiales, M pour la maternité (comptée avec les PF). (R1)
6. La ligne d'un tableau vient du code de la prestation dans l'application. (R2)
7. Un paiement est un débours de paiement payé, au statut D (par un titre de paiement) ou P (sans titre). On prend son montant, jamais le net à payer du titre. (R3)
8. Un paiement compte dans le mois qu'il paie, pas dans celui où il est saisi. Un rappel versé en février pour octobre compte en octobre. (R4)
9. Les accessoires d'une pension (bonification ou majoration pour enfant, charges du ménage) ne font ni une demande ni un bénéficiaire de plus. (A1)
10. Quand un tableau compte des personnes, il compte celle qui a droit (l'orphelin, pas son tuteur), une fois par tableau. On n'additionne jamais deux tableaux. (R5)
11. Une pension de coordination payée par l'autre caisse compte comme bénéficiaire, sans montant : seule la part payée par la CNSS entre dans les montants. (R3)
12. L'âge est l'année du calcul moins l'année de naissance, sans regarder le jour. Le sexe est 1 pour les hommes, 2 pour les femmes. (R8)
13. T25 à T49 se sortent au mois, au trimestre et à l'année (T28 à T30 : trimestre, semestre, année). Ce qui arrive dans la période s'additionne, sauf le montant des nouveaux (règle 64). Un stock se lit en fin de période. Un taux se recalcule sur les sommes. (N1, T28-T31 N6)
14. Chaque chiffre se découpe par direction régionale et par agence : celles du dossier, à défaut celles de la réception. (T25-T27 N2)
15. Un indicateur qui a une norme porte la mention « dans les normes » ou « hors normes ». Les normes sont des paramètres. (T25-T27 N7)
16. La BI garde le détail de chaque chiffre (une ligne par demande, bénéficiaire, paiement, contrôle ou décès), sans nom ni numéro d'assuré. Chacun le voit selon son profil. (N6, N12, N13)

### T25 à T27 : les dossiers (T25 pensions, T26 AT/MP, T27 PF et maternité)

17. On compte des demandes : un dossier porte une demande par ligne du tableau, par exemple la pension puis la réversion, ou les IJ puis la rente. (D2)
18. On garde les dossiers de coordination reçus par la CNSS. On retire les dossiers transférés à une autre caisse. (D1)
19. Chaque étape compte dans sa période : le dépôt à la réception, la liquidation à la notification, le rejet à sa date. (H2)
20. Déposé : la demande reçue au guichet. Sans réception, on prend la date de la demande. (D3)
21. Liquidé : à la première notification des droits. Le statut du dossier ne sert pas. (D4)
22. Rejeté : un refus enregistré dans le module des rejets, compté l'année du rejet. (D7)
23. En instance : déposée, ni liquidée ni rejetée à la fin de la période. On écarte les dossiers fermés jamais notifiés, et les réceptions d'avant 2016 jamais reliées à un dossier. (D6)
24. Durée de liquidation : les jours de calendrier entre le dépôt et la première notification (0 si elle est négative), en moyenne et en médiane par ligne. Norme : une moyenne de moins de 45 jours. (D5)
25. Le délai des indicateurs porte sur toutes les demandes liquidées : moyenne, écart type et part liquidée en 45 jours au plus. (N5)
26. Taux de dossiers restant à traiter = en instance à la fin ÷ dossiers reçus dans la période × 100. Norme : 15 % au plus. (N4)
27. Le Total compte toutes les demandes, même celles qui n'ont pas de ligne. Sa durée moyenne se recalcule sur toutes les demandes. (D8)
28. T25 : vieillesse, retraite anticipée (sur sa ligne), invalidité, survivants, allocation de vieillesse, allocation de survivant. Un dossier qui n'a qu'un accessoire va en « Autres prestations ». (R2, A1)
29. T26 : la déclaration d'accident ouvre le dossier et dépose les IJ et les soins. La rente, l'allocation d'incapacité ou les rentes de survivants se déposent plus tard sur ce dossier. (D9)
30. T26 : une IJ ne fait qu'une demande, même si l'arrêt est prolongé. Sa durée part de la déclaration. (D10)
31. T26 : les soins vont en « Autres prestations », une demande par dossier, liquidée au premier remboursement saisi. (D11)
32. T26 et T27 : une demande sans suite sort du stock 24 mois après son dépôt. Une déclaration d'accident sans suite compte dans les déposés et le Total. (D6, D12)
33. T26 : taux de variation des déclarations = (année n − année n−1) ÷ année n−1 × 100, par direction régionale et par agence, sur les dossiers AT/MP ouverts. (N8)
34. T27 : chaque enfant ajouté au dossier d'un allocataire est une demande de plus. (D13)
35. T27 : une grossesse fait deux demandes, l'IJ de maternité et l'allocation prénatale, mais une seule (l'IJ) dans le délai des PF. (D14)
36. T27 : la dernière ligne s'appelle « Frais médicaux de maternité ». Elle reste vide tant que la source n'est pas fournie. (N3)
- À faire (DCP et DR) : enregistrer chaque refus dans le module des rejets, et chaque liquidation par sa notification. (D6, D7)
- À faire (DSI) : dire où sont gardés les frais médicaux de maternité. (N3)

### T28 à T31 : les contrôles et les indus

37. T28 à T30 : une ligne par type de prestation, et un TOTAL. La retraite anticipée a sa ligne en T28, le rachat de rente et les soins en T29, les frais médicaux de maternité en T30. (N1, N2, N3)
38. Dossier payé : au moins un paiement dans la période, compté du 1er janvier à la fin de la période, une fois par ligne et une fois au TOTAL. Un dossier payé par une autre caisse ne compte pas. (N4, N6)
39. Contrôle a posteriori : un contrôle enregistré par la DAI sur un dossier déjà payé. La coche « vérifié » d'un paiement n'en est pas un. (C1, N5)
40. Un contrôle compte dans le trimestre de sa date. Dans le taux, un dossier contrôlé compte une fois. (N5)
41. Taux de contrôle = dossiers contrôlés ÷ dossiers payés × 100. Norme : 5 % au moins. (C1)
42. Le résultat d'un contrôle n'est visible que par les profils autorisés. (N14)
43. T31 : un trop-perçu (l'indu) est un ajustement TP ou TP-CONV, un moins-perçu un ajustement MP ou RC. Les deux sont montrés ; seuls les trop-perçus font le taux. (I1, N8)
44. T31 : ne sont pas des indus le remboursement d'un trop-perçu, les avances et leur reprise, les mois gratuits, les dons COVID, les rachats de rente et les oppositions. (fiche T31, règle 5)
45. T31 : un ajustement rejeté ou annulé (statut R) ne compte pas. Un indu compte une fois, le mois où il est établi. (fiche T31, règles 6 et 7)
46. T31 : taux d'indus = trop-perçus ÷ prestations payées × 100, par branche, avec 3 décimales. Norme : 2 % au plus, lue sur l'année. (fiche T31, règle 8)
47. T31 : les indus viennent de l'application. Le fichier Excel de la DAI sert seulement à comparer. (N10)
- À faire (DAI et DSI) : la DAI enregistre chaque contrôle, la DSI les met dans la vue V_BI_CONTROLE_POSTERIORI. (N11)

### T32 à T41 : les bénéficiaires à une date d'arrêté

48. Les tableaux : T32 vieillesse (RPV), T33 invalidité (RPI), T34 veuves et veufs (RPSV, RPSF), T35 orphelins (RPSE), T36 ascendants (RPA), T37 victimes (ARI), T38 veuves et veufs AT/MP (ARSV, ARSF), T39 orphelins AT/MP (ARSE), T40 ascendants AT/MP (ARSA), T41 allocations familiales (FAF). (R2)
49. Bénéficiaire : sa prestation est en cours à la date d'arrêté (commencée avant, ni radiée ni suspendue, personne en vie), qu'elle ait été payée ou non. La date d'arrêté est chaque fin de mois, le 31 décembre compris. (R6, N1)
50. Orphelins (T35, T39) : seulement ceux de moins de 25 ans. (R6)
51. Montant : ce qui est payé dans la période aux bénéficiaires comptés. L'année est la somme des mois. Montant moyen = montant ÷ effectif. (R3, N1)
52. Une date de naissance par défaut (1900) va dans « 100 ans et plus ». (R8)
53. T32 : la retraite anticipée (RPRA) va sur une ligne à part, hors des tranches d'âge. Le montant de T32 ne prend pas les accessoires. (N4, N13)
54. T33 : un invalide reste dans T33 après l'âge de la retraite, tant que l'application ne convertit pas sa pension. (N5)
55. T41 : un enfant compte s'il a droit à la date d'arrêté (21 ans au plus, en vie) et s'il a été payé dans les 12 mois d'avant (l'année civile au 31 décembre). Seules les allocations familiales comptent. (R6, N10)
56. T41 : l'allocataire est le titulaire du dossier, compté une fois s'il a au moins un enfant compté. À côté, un tableau des enfants par âge et par sexe. (N2, N6)
57. Rapport démographique : les retraités sont les pensions directes en cours (T32 avec l'anticipée, et T33), chaque personne une fois, pensions de coordination comprises. Le taux de remplacement porte sur toutes les pensions directes en cours. (N7)
58. Un dossier transféré à une autre caisse reste compté. (N11)
59. Contrôle de chaque tableau : stock de fin = stock d'avant + nouveaux − décès − autres sorties. (N8)
60. On dit « rentiers », et non plus « crédirentiers » (T38 à T40, T45 à T49). (N9)
- À faire (DSI) : le module qui passe les invalides en vieillesse à l'âge de la retraite. (N5)

### T42 à T49 : les nouveaux bénéficiaires

61. Les tableaux : T42 vieillesse et retraite anticipée, T43 veuves et veufs, T44 orphelins, T45 toutes les rentes AT/MP, T46 orphelins AT/MP, T47 ascendants AT/MP, T48 victimes, T49 veuves et veufs AT/MP. (R2)
62. Nouveau : la toute première prestation du tableau saisie dans la période. Une révision n'est pas une entrée. Un nouveau mort avant la fin de la période compte quand même. (R7)
63. On compte l'ayant droit. « Idem. Tab 41 » veut seulement dire « même périodicité ». (N4)
64. Montant : ce qui est payé pour les mois de la période. Le rappel des mois d'avant se montre à part. Les montants des mois ne s'additionnent pas pour faire l'année. (R3, N8, N3)
65. Un âge négatif, vide, ou de 120 ans et plus va en « âge inconnu ». (R8)
66. T42 : services passés = mois cotisés de la dernière pension saisie, de 1 à 600, sans les mois cotisés dans l'autre caisse en coordination. (R9)
67. T48 : degré moyen d'IPP = moyenne des taux d'incapacité, pris sur la dernière rente saisie. (N7)

### T50 à T57 : l'évolution sur cinq ans

68. Les tableaux : T50 allocation de vieillesse (RAV), T51 allocations de survivants (RASC, RASO, RASA), T52 IJ AT/MP (AIJ), T53 allocation d'incapacité (AAI), T54 IJ de maternité (MIJ), T55 allocations prénatales (MAP, FAP). (R2)
69. T50 à T55 : une personne compte l'année où une prestation du tableau lui est notifiée, une fois par an. (D4, R5)
70. T54 et T55 : le montant est celui des prestations liquidées dans l'année, quelle que soit la date des paiements. (N1)
71. T50 : services passés moyens = mois cotisés de la dernière allocation liquidée (de 1 à 600, part CNSS), en moyenne par sexe. La ligne TOTAL donne la moyenne des cinq ans. (R9, N10)
72. T56 : prestations servies par branche = paiements, soins remboursés et rachats de rente, lus dans l'application. Les accessoires vont dans l'AVS. (N4, N6, A1)
73. T56 : la ligne « Autres » reçoit l'action sanitaire et sociale. Elle reste vide tant que le montant n'est pas fourni. (N5)
74. T57 : chaque décédé porte sa qualité, assuré ou ayant droit ; par défaut, le tableau montre les assurés. L'assuré mort en activité n'est pas compté. (N2)
75. T57 : colonnes PF, AT/MP, AVS et TOTAL, et une ligne TOTAL des cinq ans. (N3)
76. T57 : un sinistré est une victime morte dans l'année qui suit son accident. (N9)
77. Les quatre années d'avant reprennent les chiffres de leur photo officielle. Une correction se signale par une note. (N7)
- À faire (DPC) : fournir le montant de l'action sanitaire et sociale. (N5)

## GRH (T58 à T65)

### T58 : l'effectif, et ce qui vaut pour T59, T60, T61 et T65

78. Seuls comptent les agents de la CNSS, pas ceux de la fonction publique ni les agents affectés. (G1)
79. La nature, permanent ou temporaire, est celle de la dernière situation à la date d'arrêté. Il y a un tableau des permanents et un des temporaires. (G1)
80. Un permanent est présent s'il est entré au plus tard à la date d'arrêté (embauche, à défaut prise de service) et pas encore parti à cette date. (G2)
81. Un départ est une décision 02 retraite, 34 décès, 35 fin de contrat, 36 démission, 52 rupture ou 54 licenciement. Annulée par une 40, elle ne compte pas. Sans décision, on prend la date de décès. (G2)
82. Un temporaire est présent si la date d'arrêté tombe dans un de ses contrats (engagement 50). Sans fin de contrat connue, il reste compté. (G7)
83. Un agent en disponibilité à la date d'arrêté sort de l'effectif. (QG6)
84. La date d'arrêté est le 31 décembre ; les fins de mois sont en option. (périodicité)
85. La catégorie est celle de la dernière situation : 01 et 02 employés, 03 maîtrise, 04 et 05 cadres moyens, 06 et HC cadres supérieurs. (G3)
86. L'âge est en années pleines à la date d'arrêté ; un âge inconnu est compté, sans tranche. Le sexe, M ou F, se lit sur la fiche. (G4)
- À faire (DRH) : saisir la fin de chaque contrat temporaire (décision 35), et la date de décès des agents. (G7, QG3)

### T59 : l'effectif par domaine d'activité

87. Le domaine est celui de l'unité de l'agent, lu dans le fichier des unités tenu par la CNSS. Une unité sans domaine donne « Domaine non renseigné ». (G12)
- À faire (CNSS) : donner le domaine de chaque unité dans ce fichier (132 unités). (G12)

### T60 : les mouvements

88. Début = les présents au 31/12 d'avant ; fin = les présents au 31/12. Début + entrées − sorties = fin. (G14)
89. Une entrée est, dans cet ordre : un détachement (décision 37), un recrutement (entré dans l'année), une remise en activité, sinon « autre ». (G8)
90. Une sortie est le départ qui a fait sortir l'agent (retraite, décès, fin de contrat, démission, licenciement), sinon une mise en disponibilité, sinon « autre ». (G9)
91. Les « autres » se détaillent par code de mouvement de la CNSS et par motif. (G15)
92. Un agent entré et sorti dans la même année n'est pas compté. (G16)
93. Les mouvements internes (affectations, mutations, nominations) restent hors de T60. (G17)
- À faire (DRH) : saisir chaque détachement et sa fin. (QG7)

### T61 : l'ancienneté

94. L'ancienneté se compte en années pleines depuis la prise de service (à défaut l'embauche), moins les périodes de disponibilité et les départs suivis d'un nouvel engagement. (G5)

### T62 : la formation

95. Un formé est un agent permanent inscrit à une formation commencée dans l'année, compté une fois, par catégorie et par sexe. La finalité est le perfectionnement ou le diplôme ; « avec promotion » s'il change de classement après la formation. (G10)

### T63 et T64 : les finances

96. Les montants viennent de la comptabilité, par la vue V_BI_BALANCE_COMPTABLE. Un taux ne se calcule que pour un exercice clos. (T63-T64)
97. T63 = frais de personnel (comptes 65) ÷ charges de fonctionnement (comptes 61 à 66). Norme : 85 % au plus. (T63-T64)
98. T64 = dépenses de formation (comptes 63, 86 et 87) ÷ masse salariale (comptes 651 et 652). Norme : de 1 à 5 %. On montre aussi la formation ÷ le budget. (T63-T64)
- À faire (DSI et DFC) : alimenter la vue V_BI_BALANCE_COMPTABLE depuis Oracle GL. (T63-T64)

### T65 : le taux d'encadrement

99. Taux d'encadrement = cadres (catégories 04, 05, 06 et HC) ÷ effectif, au 31/12 des cinq derniers exercices. Norme : 10 % au plus. (G6)
