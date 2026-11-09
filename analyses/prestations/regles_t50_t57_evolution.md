# T50 à T57 : l'évolution des prestations sur cinq ans, les prestations servies par branche et les assurés décédés — les règles en clair

## En bref

- **Ce que comptent ces tableaux.** T50 à T55 suivent six prestations sur cinq ans : combien de personnes les ont reçues, combien d'argent elles ont touché (allocations de vieillesse et de survivants, indemnités journalières et allocations d'incapacité des accidents du travail, indemnités de maternité, allocations prénatales). T56 additionne l'argent versé par branche. T57 compte les assurés décédés par branche.
- **Ce que le recueil v7 change.** Le montant de T54 et T55 devient le « montant liquidé », et non plus ce qui a été payé dans l'année (N1). T57 remplace sa colonne « Autres » par un « TOTAL », se détaille par type de personne et reçoit un filtre « assuré ou ayant droit » (N2, N3). T51 compte aussi les ascendants ; les services passés de T50 se comptent en mois. T56 n'a aucune ligne dans le recueil.
- **Ce que l'annuaire CNSS 2024 confirme.** Une personne compte l'année où sa prestation est liquidée, c'est-à-dire accordée et notifiée : 831 allocataires de vieillesse pour 833 publiés (D4) ; 3 890 mères indemnisées pour 3 893, sans que ce tableau-là départage la date. L'orphelin compte, pas son tuteur (R5). La maternité est dans les prestations familiales (R1). L'annuaire tranche aussi deux points : le rachat de rente est une prestation servie (N4) ; la ligne « Autres » de T56 est l'action sanitaire et sociale (N5).
- **Ce qui reste en écart.** Accidents du travail : la base n'a que 13 % des soins publiés en 2024, et T56 y est à −28,9 %. Indemnités de maternité : −4,2 % en montant. Rentiers décédés : 16 pour 24 publiés. Retraités décédés en 2024 : 619 dans la base d'aujourd'hui pour 447 publiés, mais environ 444 si l'on ne garde que les décès déjà saisis fin mars 2025 (R13).
- **La date d'un paiement.** Le rythme mois par mois de l'annuaire suit le jour où le paiement est saisi, pas le mois qu'il paie. Sur l'année, les deux dates restent proches (branche vieillesse : +1,3 % ou +1,8 %). La décision revient à la CNSS (Q1).
- **Les décisions attendues de la CNSS, les plus importantes :** Q1 (l'année d'un paiement), Q2 (l'annuaire est-il arrêté fin mars ?), Q3 (le sens du « montant liquidé »), Q4 (l'assuré mort en activité compte-t-il dans T57 ?), Q5 (T56 se lit-il dans la comptabilité ?). Aucune ne bloque : chaque réglage a une valeur par défaut.
- **Ce qui reste à faire.** Exécuter les trois requêtes proposées (jamais lancées) ; écrire et lancer les contrôles P115 à P119 ; après le visa de la CNSS, revoir T57 (TOTAL, qualité, type de personne) et T56 (rachats, ligne « Autres »).

**Ce que couvre la famille.** Huit tableaux de l'Annuaire CIPRES, faits en fin d'exercice. Six tableaux d'évolution sur cinq ans (N−4 à N), une prestation chacun : T50 les allocations de vieillesse (avec le nombre moyen de services passés), T51 les allocations de survivants, T52 les indemnités journalières des accidents du travail et maladies professionnelles (AT/MP), T53 les allocations d'incapacité, T54 les indemnités de maternité, T55 les allocations prénatales. Puis deux récapitulatifs sur cinq ans : T56 les prestations servies par branche (des montants), T57 les décédés par branche (des personnes). S'y ajoute le « ratio des ressources de cotisations allouées aux prestations » du recueil v7, dont T56 donne les montants.

**État au 08/11/2026.** Tout est **proposé**, rien n'est encore validé par la CNSS. Aucune réunion avec la CNSS n'a encore tranché un point de ces tableaux ; la question de la date des paiements (R4) a été posée au point du 02/11/2026, sans réponse à ce jour. La requête proposée n'a jamais été exécutée : les chiffres cités viennent des lots 8 et 9 et sont **vus au 02/02/2026** (date de la copie de la base).

**D'où viennent les règles.** Dans l'ordre de force : une décision de réunion (aucune ici), le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire statistique de la CNSS 2024 sert à comparer les chiffres, et tranche la pratique quand les sources plus fortes se taisent. Les modèles de ratios de la CNSS (classeur « Evaluation des Ratios CIPRES 2025 », feuilles 03 à 06) ne servent presque pas ici : ils visent T25 à T31.

**Ce qui existe déjà, laissé tel quel.** La requête [t50_t57_requete_proposee.sql](t50_t57_requete_proposee.sql) (01/11/2026 : requête 1 pour T50 à T55, requête 2 pour T56, requête 3 pour T57) ; le lot [lot8_evolution_5_ans.sql](lot8_evolution_5_ans.sql) et le lot 9 ; les règles R1 à R5, R9 et R13 du [README](README.md) ; la règle A1 et les faits B5, B9, B16, B22 à B24 du schéma `regles_prestations` ; la fiche de validation T50-T57 (`excalidraw/schemas/prestations/tableaux cipres/`, règles 1 à 8, questions Q1 à Q5) ; la fiche de lecture ; [points_a_valider](points_a_valider.md) (points 1, 2, 16, 49 à 57, 62, 66). Ce document les reprend avec leurs identifiants, et **propose** les règles nouvelles N1 à N11 sans rien modifier. Une règle de la fiche sans autre identifiant s'écrit ici « Fiche 6 », « Fiche 7 » (règles 6 et 7 de la fiche T50-T57).

**Le schéma associé :** `excalidraw/schemas/prestations/T50 T57 evolution/regles_t50_t57_evolution.excalidraw`, fait par `excalidraw/sources/prestations/famille_t50_t57_evolution.py`. Il dit la même chose en plus court, avec les mêmes identifiants.

**Les mots de ce document.** *Allocation de vieillesse* (ou *de survivants*) : une somme versée en une seule fois, à la place d'une pension. *Liquidé* : les droits sont calculés et notifiés à la personne (la *notification*). *Payé* : un *débours* (un ordre de paiement de l'application) au statut D, payé par un *effet* (un titre de paiement), ou P, payé sans effet. Un débours a deux dates : le *mois payé* (le mois que le paiement couvre) et la *saisie* (le jour où il est enregistré, quelques semaines avant le versement). Un *rappel* paie d'un coup des mois en retard. Un *remboursement* (débours RB) rembourse des soins. Un *ajustement* (débours AJ) corrige un paiement : par exemple un *rappel de moins-perçu* (une somme due qui n'avait pas été versée) ou un *rachat de rente* (débours d'ajustement RR : un capital versé à la place d'une rente). Le *montant liquidé* : ce qui est dû pour les prestations liquidées dans l'année, même payé plus tard. L'*assuré* : le travailleur qui a cotisé ; l'*ayant droit* : sa veuve, son veuf, son orphelin ou son ascendant (père, mère), qui reçoit une prestation de son chef. Un *rentier* (ou *crédirentier*) : le titulaire d'une rente d'accident du travail, victime ou survivant. Un *sinistré* : la victime d'un accident du travail. *AVS* : assurance vieillesse et survivants, que la CNSS appelle *PVID* (pensions de vieillesse, d'invalidité et de décès, la « branche pension ») ; *AT/MP* : accidents du travail et maladies professionnelles, que la CNSS appelle aussi *RP* (risques professionnels) ; *PF* : prestations familiales, maternité comprise. *IJ* : indemnité journalière. *ASS* : action sanitaire et sociale. *DR* : direction régionale. *Charges techniques* : les prestations, vues par la comptabilité. Le *cliché* : la copie de la base sur laquelle on calcule. La *corrélation* : un chiffre de −1 à 1 qui dit si deux suites de chiffres montent et descendent ensemble (1 : tout à fait ensemble ; 0 : sans lien). Les *réglages* : les choix écrits dans le bloc `parametres` en tête de chaque requête (une « CTE » en langage SQL) ; on les change sans réécrire la requête. *Pnn*, *lot n* : le résultat numéroté d'une requête de contrôle déjà lancée sur la base. Les sigles des directions sont expliqués en partie 6.

---

## Ce que disent les sources

| Source | Ce qu'elle dit pour ces tableaux |
|---|---|
| Recueil des besoins v7 (14/01/2026 et 20/01/2026) | T50 (l. 319 et 320) : les services passés en mois ; annuel. T51 (l. 326 et 327) : veufs et veuves, orphelins **et ascendants** ; annuel. T52 et T53 (l. 236, 242) : annuel. T54 (l. 159 et 160) : « Montant : Montant liquidé » ; annuel. T55 (l. 166) : « Idem Tab. 54 ». T57 (l. 172 à 176) : « Assurés décédés », « Autres » devient « TOTAL », détail par type de bénéficiaire, filtre « assuré ou ayant droit », annuel. T56 : aucune ligne. Le ratio des ressources allouées aux prestations (l. 1129 à 1141, 1210 à 1214, 1287 à 1294) compte les prestations notifiées (l. 1133). Les tableaux financiers T66 et T75 lisent les prestations en comptabilité (l. 361, 455, 464) |
| Réunions avec la CNSS | Aucune décision sur T50 à T57 à ce jour. La date des paiements (R4) a été posée au point du 02/11/2026 ; les questions de la fiche T50-T57 et de points_a_valider attendent leur réponse |
| Atelier de Kaya (2017) | Tableau 1 : T54 et T55, une ligne « total » et des prestations « liquidées et non payées » (l. 129, 130, 139, 140) ; T57 renommé « récapitulatif des assurés décédés déclarés par branche », une colonne et une ligne de total, les personnes visées (pension directe ; rentier et sinistré ; allocataire) et le détail par type de bénéficiaire (l. 150 à 156) ; T29, la modalité « achat de rente » (l. 173) ; T76 (ASS) fait sous Excel puis chargé dans la BI (l. 395). Rien à signaler pour T50 à T53 (« RAS ») ; rien sur T56. Tableau 2 : la même note sur les prestations ordonnancées (l. 898) |
| Annuaire CIPRES | Description p. 16 ; maquettes p. 46 à 48 ; glossaire p. 62 (allocation, bénéficiaire), p. 63 (décédé), p. 65 (prestation ; rapport de charges brut et net), p. 66 (services passés). T50 : hommes et femmes, effectif, montant et nombre moyen de services passés, avec une ligne TOTAL. T51 à T53 : hommes, femmes, total, sans ligne TOTAL. T54 et T55 : une colonne FEMMES. T56 : PF, AT/MP, AVS, AUTRES, TOTAL. T57 : PF, AT/MP, AVS, AUTRES, sans total |
| Annuaire CNSS 2024 | Contrôles directs : allocations de vieillesse liquidées (tableau 24) ; IJ de maternité, effectifs et montants sur cinq ans (tableaux 35 et 39) ; montants de 2024 par prestation (tableaux 28 à 34, 40 à 46) ; montants AT/MP sur cinq ans (tableaux 82 à 99) ; prestations servies par branche sur cinq ans (tableau 106) ; décès de pensionnés (26) et de rentiers (81) ; décès immédiats d'accidents (49). Ne publie pas les effectifs de T51, T52, T53 et T55, ni les services passés, ni les décès d'allocataires |
| Modèles de ratios de la CNSS (2025) | Feuilles 03 à 06 : rien de direct pour T50 à T57 ; elles nomment les trois mêmes branches que le tableau 106 (PF, risques professionnels, pensions), sans ligne maternité à part. La feuille 02 (« taux de redistribution »), qui porterait le rapport entre prestations et cotisations, n'a pas été lue (consigne) |
| Données de la base | Lot 8 (P92 à P101), lot 9 (P104, P105), lot 5 (P78), lot 2 (P20, P30), lot 7 (P87) ; comparaison A02 (dossier `comparaison_annuaire_cnss_2024`) |

---

## 0. Les tableaux de la famille

| Tableau | Ce qu'il compte (Annuaire CIPRES) | Page | Ce qu'on a déjà |
|---|---|---|---|
| T50 | Les bénéficiaires d'allocation de vieillesse de chaque année, de N−4 à N, le montant servi et le nombre moyen de services passés (le total des périodes de travail salarié, glossaire p. 66). Deux blocs HOMMES et FEMMES (effectif, montant, services passés), une ligne TOTAL, pas de colonne hommes + femmes. L'allocation est versée en une seule fois (glossaire p. 62) ; c'est la prestation d'un assuré qui n'a pas assez cotisé pour une pension (explication BI, à faire confirmer) | p. 16 ; maquette p. 46 ; glossaire p. 62 et 66 | Requête 1 de `t50_t57_requete_proposee.sql` (pas exécutée) ; fiche T50-T57, règles 1 à 5 ; lot 8 (P92, P94) |
| T51 | Les bénéficiaires d'allocation de survivants et les montants, de N−4 à N. L'Annuaire CIPRES écrit « veufs/veuves et orphelins » ; le recueil v7 ajoute les ascendants. Blocs HOMMES, FEMMES, TOTAL ; pas de ligne TOTAL | p. 16 ; maquette p. 46 | Requête 1 ; lot 8 (P92, P93) |
| T52 | Les bénéficiaires d'IJ des AT/MP (versées à la victime pendant son arrêt, glossaire p. 64) et les montants. Même forme que T51 | p. 16 ; maquette p. 46 ; glossaire p. 64 | Requête 1 ; lot 8 (P92, P93) |
| T53 | Les bénéficiaires d'allocation d'incapacité et les montants. L'allocation va à la victime dont l'incapacité est trop faible pour une rente : aucune rente viagère n'a un taux sous 15 % (lot 7, P87), l'allocation couvre donc sans doute les taux plus bas (à confirmer). Même forme que T51 | p. 16 ; maquette p. 47 | Requête 1 ; lot 8 (P92) |
| T54 | Les bénéficiaires d'indemnités de maternité (versées à la salariée pendant son congé, glossaire p. 64) et les montants. Une colonne FEMMES (effectif, montant), pas de ligne TOTAL ; Kaya 2017 en ajoute une et veut les indemnités liquidées | p. 16 ; maquette p. 47 ; glossaire p. 64 | Requête 1 ; lot 8 (P92) |
| T55 | Les bénéficiaires d'allocations prénatales et les montants. L'allocation va à la salariée enceinte ou à l'épouse enceinte d'un salarié (glossaire p. 62). Même forme que T54 ; Kaya 2017 ajoute une ligne TOTAL et veut les allocations liquidées | p. 16 ; maquette p. 47 ; glossaire p. 62 | Requête 1 ; lot 8 (P92, P93) |
| T56 | Toutes les prestations servies par branche, sur cinq ans (des montants). Lignes PF, AT/MP, AVS, AUTRES, TOTAL ; une colonne par année. Une prestation est un droit en espèces ou en nature (glossaire p. 65) : les soins en font partie | p. 16 ; maquette p. 48 ; glossaire p. 65 | Requête 2 ; fiche T50-T57, règle 6 ; lot 8 (P95, P98), lot 9 (P104) |
| T57 | Tous les décédés par branche, sur cinq ans (des personnes). Une ligne par année ; colonnes PF, AT/MP, AVS, AUTRES ; pas de total. Décédé : personne dont la mort est constatée par un acte d'état civil (glossaire p. 63). Kaya 2017 l'a renommé « assurés décédés déclarés par branche » | p. 16 ; maquette p. 48 ; glossaire p. 63 | Requête 3 ; fiche T50-T57, règles 7 et 8 ; lot 8 (P96, P97, P99, P100) |

Plus bas, « requête 1 », « requête 2 » et « requête 3 » désignent les trois parties de `t50_t57_requete_proposee.sql`.

---

## 1. Ce que demande le recueil des besoins v7

Toutes les lignes du recueil qui touchent T50 à T57. Les numéros de ligne sont ceux de `besoins_v7.txt`. Les formules des indicateurs sont des équations Word, absentes du texte : elles ont été relues dans le document Word (extraction `style_grh/travail/scratchpad/recueil_out/document_dump.txt`).

| Réf. | Ce que dit le recueil | En clair | Où c'est pris en compte | Statut |
|---|---|---|---|---|
| Prestation #28 (T50), l. 319 | « Service passé : total des périodes d'activités salarié en nombre de mois » | Les services passés d'un allocataire se comptent en mois de travail salarié ; T50 en donne la moyenne | R9 ; requête 1 ; fiche, règle 5. Moyenne de la ligne TOTAL : N10 | pris en compte |
| Prestation #28 (T50), l. 320 | « Périodicité : Annuelle » | T50 sort une fois par an | Requête 1, annuelle, de N−4 à N | pris en compte |
| Prestation #29 (T51), l. 326 | « Allocation de survivants : veufs/veuves, orphelins et ascendants » | T51 compte aussi les ascendants, que l'Annuaire CIPRES (p. 16) ne cite pas | R2 : parts du conjoint, de l'orphelin et de l'ascendant (codes RASC, RASO, RASA) | pris en compte |
| Prestation #29 (T51), l. 327 (« Idem. Tab 52 ») | La périodicité annuelle de T52 | T51 sort une fois par an | Requête 1 | pris en compte |
| Prestation #16 (T52), l. 236 | « Périodicité : Annuelle » | T52 sort une fois par an | Requête 1 | pris en compte |
| Prestation #17 (T53), l. 242 (« Idem. Tab 52 ») | La périodicité annuelle de T52 | T53 sort une fois par an | Requête 1 | pris en compte |
| Prestation #5 (T54), l. 159 | « Montant : Montant liquidé » | Le montant de T54 est ce qui est dû pour les indemnités liquidées dans l'année, pas ce qui a été payé dans l'année | N1 ; réglage `montant_t54_t55` (à créer) ; sens exact à confirmer (Q3). Déjà relevé : remarque B15 du recueil | à intégrer |
| Prestation #5 (T54), l. 160 | « Périodicité : Annuelle » | T54 sort une fois par an | Requête 1 | pris en compte |
| Prestation #6 (T55), l. 166 (« Idem Tab. 54 », renvoi à l. 159) | Le montant liquidé | Même règle pour les allocations prénatales | N1, comme T54 ; sens exact à confirmer (Q3) | à intégrer |
| Prestation #6 (T55), l. 166 (renvoi à l. 160) | La périodicité annuelle | T55 sort une fois par an | Requête 1 | pris en compte |
| Prestation #7 (T57), l. 172 | « Assurés décédés » | T57 compte des assurés décédés. Kaya 2017 en fait la liste : pension directe, rentier et sinistré, allocataire. Reste à savoir si l'assuré mort en activité, avant toute pension, en fait partie | Fiche 7, N2 ; Q4 | question |
| Prestation #7 (T57), l. 173 | « Remplacer « Autres » par « TOTAL » » | Colonnes PF, AT/MP, AVS et TOTAL ; « Autres » disparaît | N3 : la requête 3 sort encore une colonne AUTRES (toujours 0) | à intégrer |
| Prestation #7 (T57), l. 174 | « Désagréger les branches par type de bénéficiaires » | Chaque branche se détaille : retraité, invalide, veuve, orphelin… en AVS ; rentier, sinistré, survivant en AT/MP ; allocataire en PF | N2 ; la requête 3 ne garde que la branche | à intégrer |
| Prestation #7 (T57), l. 175 | « Ajouter un autre filtre : Assuré ou ayant droit » | Pouvoir montrer les assurés décédés, les ayants droit décédés, ou les deux. Ligne surlignée en jaune dans le document | N2 ; réglage `qualite_deces` (à créer). Qui est ayant droit en PF : Q10. Répond en partie à la remarque B26 du recueil | à intégrer |
| Prestation #7 (T57), l. 176 | « Périodicité : Annuelle » | T57 sort une fois par an | Requête 3 | pris en compte |
| Liste du domaine Prestation, l. 115 à 335 | Aucune ligne pour le tableau 56 | Le recueil ne dit rien de T56, pas plus que de T26, T39 et T40 : l'atelier de Kaya ne les avait pas examinés | Requête 2 faite d'après l'Annuaire CIPRES ; remarque B38 du recueil ; Q21 | question |
| Prestation #4 (T41), l. 152 | « Allocataire : Titulaire du dossier (assuré) » | L'allocataire est le titulaire du dossier d'allocations familiales | Fiche 7, branche PF : le titulaire du dossier dont une allocation familiale (code FAF) est en cours au décès | pris en compte |
| Prestation #10 (T38), l. 194 | « Crédirentiers : rentiers » | Pour la CNSS, le titulaire d'une rente de survivant est aussi un « rentier » | Fiche 7 : la branche AT/MP compte déjà les rentes de survivants ; N2 les range parmi les ayants droit | pris en compte |
| Prestation #25 (T42), l. 298 | « Pour les pensions de coordination, prendre uniquement la partie CNSS » | Pour un assuré qui a cotisé dans deux caisses, on ne garde que les mois cotisés à la CNSS ; la remarque B06 du recueil l'étend à T50 | R9 : mois cotisés moins les mois de l'autre caisse (`PE_COT_AUTRE`) ; 0 à 4 allocations par an (P94). Le sens de `PE_COT_AUTRE` est encore une question de la famille T42-T49 (T42-T49 Q10), où la même phrase est « question » | question (T42-T49 Q10) |
| Prestation #20 (T32), l. 263 | « Pour les pensions de coordination, les montants concernent uniquement la partie CNSS » | Les montants ne comptent que ce que la CNSS paie elle-même | T56 ne prend que les paiements de la CNSS ; les pensions payées par une autre caisse n'en ont aucun (lot 5, P78) | pris en compte |
| Prestation #3 (T31), l. 142 à 144 | « PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale » | Les noms que la CNSS donne à ses trois branches | R1. Proposition : écrire « AVS (PVID) » et « AT/MP (RP) » dans T56 et T57, pour que la CNSS s'y retrouve | à intégrer |
| Prestation #1 (T27), l. 127 | « Autres prestations de la branche : Renommer par “Frais médicaux de maternité” » | Les frais médicaux de maternité sont une prestation de la branche des PF | R1 ; Fiche 6 : ils vont dans la ligne PF de T56. La base n'en paie aucun (remarque B27 du recueil) ; Q17 | question |
| Indicateurs PF, l. 1102 | « Par type de prestations (Allocations familiales et Indemnités Journalière de Maternité) » | Le recueil range lui-même les IJ de maternité avec les PF | R1 (T54 et la ligne PF de T56) | pris en compte |
| Indicateurs PF, l. 1129 à 1141 (note l. 1133) ; AT/MP l. 1210 à 1214 ; pensions l. 1287 à 1294 | « Le ratio des ressources de cotisations allouées aux prestations » : prestations ordonnancées de la période ÷ cotisations ordonnancées × 100 ; « le montant total des prestations ordonnancées au titre de la période est l'ensemble des prestations ayant fait l'objet d'une notification » ; par type, DR et agence | La part des cotisations consommée par les prestations. Les prestations comptent quand elles sont notifiées : ni à la saisie, ni au mois payé | N11 ; formule lue dans le document Word ; avec ou sans les soins : Q20. La remarque B37 du recueil lisait déjà « ordonnancées = notifiées » | à intégrer |
| Indicateurs PF, l. 1137 à 1139 | Le ratio des ressources des PF : « Trimestriel / Annuel » | Les montants de la branche PF doivent aussi se lire par trimestre | N8 : garder le mois de chaque paiement dans le détail | à intégrer |
| Indicateurs pensions, l. 1251 | « Le nombre de retraités est le nombre de bénéficiaires de pensions directes (Pension vieillesse, Pension anticipé, pension d'invalidité) » | Un retraité touche une pension de vieillesse, de retraite anticipée ou d'invalidité | Fiche 7, branche AVS : les mêmes codes (RPV, RPRA, RPI) | pris en compte |
| Indicateurs ASS, l. 1318 à 1337 | Charges administratives de l'ASS sur ses charges totales et sur celles de la CNSS ; taux de couverture des dépenses de l'ASS ; annuel ; structure DCG | L'ASS a ses propres indicateurs, suivis par la DCG | N5 : le montant d'ASS de la ligne « Autres » de T56 doit être celui de ces indicateurs ; associer la DCG au choix (Q18) | question |
| Prestation #30 (T76), l. 333 et 334 | « Ratio : en pourcentage (2 chiffres après la virgule) » ; annuel | T76 est le tableau des dépenses et recettes de l'ASS, hors T50 à T57 | Lien seulement : la ligne « Autres » de T56 (N5) prend le même montant d'ASS. Kaya 2017 : T76 vient d'un fichier Excel | hors base |
| Finance #1 (T66), l. 361 (repris pour T67 et T68, l. 372 et 378) | « Charges techniques (ct) : 601xxx, 602xxx, 603xxx, 605xxx » | En finance, les prestations se lisent en comptabilité, dans ces comptes | N6 : option de lire T56 dans la comptabilité ; Q5 | question |
| Finance #11 (T74), l. 445 à 449 | Rapport de charges : « Voir la DSI pour les formules » ; « Norme : inférieur strictement à 100% » | Prestations ÷ cotisations, côté finance | Même numérateur que N11 ; T74 se fait dans le domaine Finance | pris en compte |
| Finance #12 (T75), l. 455, 461 et 464 | « Réserves en années de prestations » ; « Pour chaque branche : PF, AT/MP, AV (PVID) » (l. 461) ; « Prestation de l'année N-1 (par type) / Reserve (par type) » | T75 a besoin des prestations de l'année précédente par branche | T56 les donne ; N6 : même source pour T56 et T75 | pris en compte |
| Termes de référence, l. 4 à 10 | « Cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada N'gourma, Dédougou » | « Par DR » veut dire ces cinq directions | N8 | à intégrer |
| Indicateurs, l. 1065 | « De manière globale, donner la possibilité de désagréger l'indicateur » | Chaque chiffre se découpe | N8 | à intégrer |
| Attentes, l. 29 | « Toutes les données des rapports doivent pouvoir être décomposées, traquées jusqu'au niveau de granularité le plus élémentaires » | Chaque chiffre se déplie jusqu'à la prestation, au paiement ou au décès | N8 | à intégrer |
| Attentes et caractéristiques, l. 31 et 74 | Présentation multidimensionnelle ; une dimension ajoutée après coup vaut pour tous les indicateurs | Les mêmes axes (branche, code, DR, agence, mois, sexe) pour toute la famille | N8 | à intégrer |
| Attentes, l. 35 | « Production des rapports et des états de restitution à des fréquences adaptées » | Chaque tableau à sa fréquence | Toute la famille est annuelle (l. 160, 166, 176, 236, 242, 320, 327) | pris en compte |
| Attentes et caractéristiques, l. 28, 34, 75 | Données financières « sans interventions ou avec très peu d'intervention manuelle » ; restitutions automatisées | Pas de saisie à la main | Requêtes sur la base ; pèse contre une ligne « Autres » recopiée d'un fichier Excel, en faveur d'une source automatique (N5, N6) | pris en compte |
| Caractéristiques, l. 53 à 55 et 60 | Chargements depuis des fichiers et des bases | L'outil sait charger un fichier Excel | Possible pour l'ASS (N5) si aucune source automatique n'est retenue | pris en compte |
| Caractéristiques, l. 58 | Règles de transcodage (passage d'un code à une ligne) écrites et rejouables | La correspondance entre codes de prestation et tableaux est écrite | R2 (table de correspondance du README) | pris en compte |
| Caractéristiques, l. 72 | Contrôle de l'accès aux données par profil | Les données des décédés sont personnelles | T57 ne lit pas la vue `V_PRESTATAIRES_DECEDES_BI`, qui contient nom, adresse et téléphone (Q23) | pris en compte |
| Attentes et caractéristiques, l. 24 et 78 | Profilage de la qualité des données | Mesurer les trous de la base | Lots 8 et 9 ; points d'attention de la partie 7 | pris en compte |
| Général, l. 15 et 92 à 94 | La source : l'application de gestion des prestations | Les tableaux se calculent sur la copie de cette application | Toutes les règles ; copie de la base (schéma `USER_DWH`) au 02/02/2026 | pris en compte |
| Indicateur 1, l. 1075 à 1084 | « La proportion des clients satisfaits des services rendus » ; biennal ; par DR et par type de prestation ; DES, DCP, DR ; « Données non disponibles en base » | Une enquête de satisfaction auprès des assurés, pas un calcul sur la base. Il porte sur toutes les branches : ce document, qui couvre les prestations par branche, le range ici | Le recueil le dit lui-même : la donnée n'est pas dans la base. La BI pourra charger les résultats de l'enquête s'ils sont donnés dans un fichier fixe | hors base |
| Indicateur ASS, l. 1297 à 1304 | « La proportion des clients pris en charge selon les normes et protocole en vigueur en matière sanitaire » ; annuel ; par SMI ; DP | Les SMI sont les centres de santé maternelle et infantile de la CNSS. L'indicateur porte sur les soins donnés dans ces centres | Proposition BI : hors base. L'application des prestations paie des prestations ; elle ne suit pas les soins des centres. L'annuaire CNSS 2024 publie leur activité à part (tableaux 115 à 117) | hors base |
| Indicateur ASS, l. 1308 à 1316 | « Le taux de réussite au CQP » (certificat de qualification professionnelle) ; « Source : Excel » ; annuel ; par DR (Ouaga, Bobo) | Les résultats d'une formation suivie par la CNSS, tenus dans un fichier Excel | Le recueil donne lui-même un fichier Excel pour source | hors base |
| Nouvelles prestations, l. 1340 à 1356 | « Proportion de nouvelles prestations promues » ; « A confirmer avec DAI et DPC » ; annuel ; DCP | La CNSS ne sait pas encore elle-même ce que l'indicateur compte | Rien à calculer tant que la CNSS ne l'a pas défini (Q24) | question (Q24) |
| Réclamations, l. 1358 à 1368 | « La variation du nombre de réclamations (plainte & réclamations) » ; « Source : Excel (les DR) » ; différence entre la période n+1 et la période n ; trimestriel, annuel ; par DR et agence | Le nombre de réclamations d'une période moins celui de la période d'avant, tiré des fichiers Excel des directions régionales | Le recueil donne lui-même les fichiers Excel des DR pour source | hors base |
| Document de référence, l. 80 et 81 | « Document de référence des tableaux de la CNSS » | Un document annoncé par le recueil, ni nommé ni joint | Il vaut pour tous les tableaux ; ce document le range ici avec les autres lignes générales. À obtenir de la CNSS (Q25) | question (Q25) |

**Quels tableaux n'ont aucune ligne.** T56 n'a aucune ligne dans la liste des tableaux du recueil (ni dans le tableau 1 de Kaya 2017). T53 et T55 n'ont qu'un renvoi (« Idem. Tab 52 », « Idem Tab. 54 »). T50, T51, T52, T54 et T57 ont leurs propres lignes.

**Les lignes générales qui ne changent rien ici :** l. 32 (alertes : T50 à T57 n'ont pas de norme), indicateur de paiement par virement (l. 1085 à 1091 : il porte sur le mode de paiement des paiements de T50 à T56, mais se calcule hors de cette famille ; N8 garde chaque paiement, donc de quoi le calculer), taux de remplacement (l. 1256 à 1264 : familles T32-T41 et T42-T49), ratios CIPRES respectés (l. 1978 à 1989 : familles T25-T27 et T28-T31), colonnes « Priorité » vides (l. 120 et 121). Six lignes qu'aucune autre famille ne classe sont rangées dans le tableau ci-dessus, avec leur statut : satisfaction, normes sanitaires des SMI, CQP, nouvelles prestations, réclamations, document de référence (l. 80 et 81). Les autres familles y renvoient.

---

## 2. Les règles, en clair

### R1 — Les branches

**Règle.** Une prestation suit la branche de son dossier : V = assurance vieillesse et survivants (AVS, que la CNSS appelle PVID : pensions et allocations) ; A = accidents du travail et maladies professionnelles (AT/MP, ou RP) ; F et M = prestations familiales (PF), maternité (M) comprise.

**Exemple.** Une indemnité de maternité (code MIJ, dossier M) compte dans la ligne PF de T56. Les frais médicaux de maternité aussi, quand on saura où ils sont payés (Q17).

**Sources.** Recueil v7, l. 1102 (les IJ de maternité avec les allocations familiales), l. 127 (les frais médicaux de maternité, prestation de la branche de T27) et l. 142 à 144 (les trois branches et leurs noms) ; Annuaire CNSS 2024, tableau 40 (IJ, prénatales et frais médicaux dans les PF) et tableau 106 ; Modèles CNSS feuilles 03 à 06 (trois branches, sans ligne maternité : cohérent) ; README, R1 ; Données P02, P05, P95 (aucun paiement hors des quatre codes de branche).

**État.** Confirmée par l'annuaire CNSS 2024.

### R2 — Les prestations de chaque tableau (fiche, règle 1)

**Règle.** La ligne vient du code de prestation de l'application. T50 = RAV (allocation de vieillesse). T51 = RASC, RASO, RASA (les parts du conjoint, de l'orphelin, de l'ascendant ; RAS, l'ancien code global, ne se liquide plus). T52 = AIJ (IJ des AT/MP). T53 = AAI (allocation d'incapacité). T54 = MIJ (IJ de maternité). T55 = MAP et FAP (allocations prénatales de la maternité et, ancien code, des PF).

**Exemple.** En 2024, T51 compte 81 parts de conjoint, 472 d'orphelin et 18 d'ascendant.

**Sources.** Recueil v7, l. 326 (T51 : veufs et veuves, orphelins et ascendants) ; Annuaire CIPRES p. 16 (ne cite que veufs et orphelins) ; README, table de correspondance ; Données P93 (RAS : 0 en 2024 ; MAP 3 857 personnes, FAP 56).

**État.** Proposée. Les ascendants dans T51 : tranché par le recueil v7. La liste des codes reste à faire viser (points_a_valider n° 18).

### D4 — Liquidé : le bénéficiaire de l'année (fiche, règle 2)

**Règle.** Dans T50 à T55, une personne compte l'année où une prestation du tableau lui est liquidée, c'est-à-dire notifiée (date de notification de la prestation, `PE_DATE_NOTIFICATION`). Elle compte une fois dans l'année, même avec plusieurs prestations ; elle peut compter plusieurs années. Ni le paiement ni la saisie ne comptent.

**Exemple.** Allocation de vieillesse demandée en 2023 et notifiée le 10/03/2024 : bénéficiaire de 2024. Une victime qui a une IJ notifiée en 2023 et une autre en 2024 compte dans T52 de 2023 et dans celui de 2024.

**Sources.** Kaya 2017, tableau 1, l. 130 et 140 (T54 et T55 : « liquidée et non … payée ») ; Annuaire CNSS 2024, tableau 24 (titre « allocations de vieillesse liquidées » : 831 personnes pour 833) et tableau 35 (IJ de maternité : 3 890 pour 3 893 en 2024, à moins de 1 % de 2020 à 2024 sauf 2022) ; règle D4 de T25-T27 ; Données P92 (les payées s'écartent de 24 à 29 % pour T54) ; pour T55, l'allocation prénatale de maternité est bien notifiée (fait B23 du schéma des règles, P66).

**État.** Confirmée par l'annuaire CNSS 2024 pour T50 (tableau 24). Pour T54, le tableau 35 tombe juste (les payées, non), mais il ne départage pas la liquidation d'une date proche : il précise, comme dans la famille T25-T27. Pour T51 à T53 et T55, l'annuaire ne publie pas d'effectif : appliquée par analogie (Q8). Même sens que D4 de T25-T27 (liquidé = notifié) ; ici, la date est celle de chaque prestation du tableau, pas celle du dossier.

### R5 — La personne comptée (fiche, règle 3)

**Règle.** On compte l'ayant droit de la prestation (`IND_ID_PREST`), une fois par tableau et par année, avec son sexe. Pour un orphelin, on compte l'orphelin, pas son tuteur qui touche l'argent. T54 et T55 n'ont que des femmes.

**Exemple.** Une victime qui a trois IJ liquidées en 2024, parce que son arrêt a été prolongé deux fois, compte une fois dans T52 (371 IJ pour 195 victimes en 2024).

**Sources.** Annuaire CIPRES, glossaire p. 62 (le bénéficiaire est « toute personne » qui perçoit une prestation : une personne, pas une prestation) et maquettes p. 46 et 47 ; Annuaire CNSS 2024, tableau 20 (orphelins de 0 à 24 ans, garçons et filles : la CNSS compte l'orphelin) ; README, R5 ; Données P93, P92 (aucun homme en T54 et T55).

**État.** Proposée. « Une personne » : tranché par le glossaire CIPRES, qu'aucune source plus forte ne contredit (points_a_valider n° 50 ; fiche T50-T57 Q5). L'orphelin plutôt que le tuteur : confirmé par l'annuaire CNSS 2024 (tableau 20).

### R3 — Ce qu'est un paiement

**Règle.** Un paiement est un débours de type PM (`DEB_TYPE`) au statut D (payé par un effet) ou P (payé sans effet) (`DEB_STATUT`). On prend son montant (`DEB_MONTANT`), jamais le net à payer de l'effet, qui se répète sur chaque débours.

**Exemple.** Un débours validé mais jamais décaissé (statut V) ne compte pas.

**Sources.** README, R3 ; fait B5 du schéma des règles ; Données P20, P95 ; Annuaire CNSS 2024, tableau 106 (le total des trois branches retombe à moins de 0,6 % de 2021 à 2024).

**État.** Confirmée par l'annuaire CNSS 2024 (README).

### R4 — L'année d'un paiement (montants de T50 à T53 et de T56)

**Règle.** Aujourd'hui, un paiement compte pour l'année du mois qu'il paie (`DEB_DATE_EFFET`). La CNSS semble le compter l'année de sa saisie (`DEB_DATE_INSERT` : sa préparation, environ trois semaines avant l'échéance, lot 2, P20), rappels compris.

**Exemple.** Un rappel de 15 mois saisi en mars 2024 : au mois payé, 12 mois vont en 2023 et 3 en 2024 ; à la saisie, tout va en 2024.

**Sources.** Annuaire CNSS 2024, tableaux 28, 41 et 90 (le rythme mois par mois suit la saisie : corrélation de 0,75 à 0,92, contre −0,12 à 0,26 au mois payé ; P105) et tableau 34 (allocations de vieillesse 2024 : +0,02 % à la saisie, +3,9 % au mois payé) ; Données P92, P98 (la saisie fait mieux sur 11 lignes sur 13 ; le mois payé fait mieux sur la pension de vieillesse, +0,68 % contre +1,53 %, et de peu sur la rente viagère, −12,22 % contre −12,57 %) ; README, R4.

**État.** À revoir. Réglage `date_montant` ('MOIS' aujourd'hui ; proposition BI : 'SAISIE'). Question posée au point du 02/11/2026 (README ; TODO, tâche 18 ; points_a_valider n° 1) : Q1. Ni le recueil ni Kaya 2017 ne donnent cette date : le recueil date les prestations « ordonnancées » à la notification (l. 1133), ce qui vise le montant liquidé (N1, N11), pas la saisie.

### N1 — Le montant de T54 et T55 : le montant liquidé

**Règle.** Pour les IJ de maternité et les allocations prénatales, le montant est le montant liquidé : tous les paiements des prestations liquidées dans l'année, quelle que soit leur date ; ou le montant calculé à la liquidation, si la DSI trouve où il est gardé (Q7).

**Exemple.** IJ de maternité liquidée en décembre 2024, payée en décembre 2024 et en février 2025 : les deux paiements vont dans T54 de 2024. Aujourd'hui, seul celui de décembre y va.

**Sources.** Recueil v7, l. 159 (T54 : « Montant : Montant liquidé ») et l. 166 (T55 : « Idem Tab. 54 ») ; Kaya 2017, tableau 1, l. 130 et 140 (les liquidées) ; remarque B15 du recueil ; Données P92 : aujourd'hui, avec le réglage 'MOIS', 2 745 737 842 en 2024 (2 749 474 700 à la saisie), pour 2 865 630 056 publiés au tableau 35, qui dit « montants perçus ». Le chiffre liquidé reste à mesurer (P116).

**État.** Nouvelle, imposée par le recueil v7 (il l'emporte sur le titre du tableau 35 de l'annuaire). Sens exact à confirmer (Q3). Réglage `montant_t54_t55` (à créer).

### R9 — Les services passés moyens de T50 (fiche, règle 5)

**Règle.** Pour chaque allocataire de vieillesse de l'année, on prend les mois cotisés (`PE_MOIS_COTISATION`) de sa dernière allocation liquidée dans l'année, de 1 à 600 mois. En coordination, on ne garde que la part CNSS : le total moins les mois cotisés dans l'autre caisse (`PE_COT_AUTRE`). On en fait la moyenne par sexe.

**Exemple.** 2024 : 831 personnes (690 hommes, 141 femmes, P92), 95,5 mois en moyenne pour la part CNSS (95,7 avant de retirer l'autre caisse), soit environ 8 ans ; médiane 98 mois. La moyenne par sexe reste à mesurer.

**Sources.** Recueil v7, l. 319 (« en nombre de mois ») et l. 298 (coordination : la part CNSS, pour T42 ; la remarque B06 du recueil l'étend à T50) ; Annuaire CIPRES p. 46 et glossaire p. 66 ; README, R9 ; Données P94 (aucun mois vide, nul ou au-dessus de 600 ; 0 à 4 allocations de coordination par an).

**État.** Proposée ; l'unité (le mois) est confirmée par le recueil v7. Le retrait de `PE_COT_AUTRE` suppose que ce champ donne bien les mois cotisés dans l'autre caisse : c'est la question T42-T49 Q10, posée une seule fois pour T42 et T50. À vérifier avant de publier : une allocation de vieillesse peut avoir beaucoup de mois cotisés (489 au plus, lot 2, P30 ; fait B16 du schéma des règles ; points_a_valider n° 62) : Q9.

### N10 — Les lignes TOTAL de T50 à T55

**Règle.** La ligne TOTAL de T50 donne aussi le nombre moyen de services passés : la moyenne sur les cinq années, chaque année pesant selon son nombre d'allocataires. T54 et T55 ont une ligne TOTAL (Kaya 2017). T51 à T53 n'en ont pas dans l'Annuaire CIPRES : la requête 1 en ajoute une, sans gêne.

**Exemple.** De 2020 à 2024 : 585, 594, 707, 738 et 831 allocataires, avec 84,9, 88,2, 89,3, 93,9 et 95,5 mois (part CNSS) : ligne TOTAL d'environ 90,8 mois (recalcul sur les moyennes arrondies de P94).

**Sources.** Annuaire CIPRES, maquette T50 p. 46 (la ligne TOTAL couvre toutes les colonnes) ; Kaya 2017, tableau 1, l. 129 et 139 ; Données P94.

**État.** Nouvelle (petite correction : la requête 1 laisse vide la colonne des services passés sur la ligne TOTAL).

### A1 — Les accessoires de pension dans T56

**Règle.** La bonification et les majorations pour enfant (RBEC, RME, RME-COL) et la contribution aux charges du ménage (RCCM) s'ajoutent aux pensions : leur montant va dans l'AVS de T56.

**Exemple.** 2024 : 539 376 000 de bonification pour enfant (RBEC) saisis dans la base ; 537 343 000 de « majoration pour enfant en charge » publiés.

**Sources.** Annuaire CNSS 2024, tableaux 28 et 34 (la majoration est rangée avec la pension de retraite vieillesse) ; README, A1 ; Données P34, P98.

**État.** Confirmée par l'annuaire CNSS 2024.

### Fiche 6 — T56 aujourd'hui : les paiements par branche (règle 6 de la fiche)

**Règle.** La requête 2 additionne les paiements (débours PM) et les remboursements de soins (débours RB) de l'année, par branche, sans les ajustements (débours AJ, des corrections de paiement) ; la ligne « Autres » reste vide ; une ligne TOTAL.

**Exemple.** 2024, au mois payé : AVS 47,24 Md (46,63 publiés, +1,3 %), PF 10,75 Md (11,18, −3,8 %), AT/MP 0,76 Md (1,07, −28,9 %). Les trois branches ensemble : −0,2 %.

**Sources.** Annuaire CIPRES p. 16, 48 et glossaire p. 65 (une prestation est un droit en espèces ou en nature) ; fiche T50-T57, règle 6 ; Données P95, P98, P104.

**État.** À revoir : les rachats de rente (N4), la ligne « Autres » (N5), la source comptable (N6), les frais médicaux de maternité dans la ligne PF (Q17), les rappels de moins-perçus (Q19).

### N4 — Les rachats de rente dans T56

**Règle.** Le rachat d'une rente AT/MP (un capital versé à la place de la rente) est une prestation servie de la branche AT/MP. Dans la base, il est versé par un débours d'ajustement de type RR : on l'ajoute à T56.

**Exemple.** 2024 : 21 519 361 versés en rachats dans la base ; 25 366 909 publiés.

**Sources.** Annuaire CNSS 2024, tableaux 90, 96 et 97 (les rachats parmi les prestations en espèces des AT/MP) ; Kaya 2017, tableau 1, l. 173 (« achat de rente », type de prestation AT/MP) ; README, lot 9 ; Données P104.

**État.** Nouvelle, tranchée par l'annuaire CNSS 2024 et Kaya 2017 (le recueil se tait) : le point 52 de points_a_valider est réglé pour les rachats ; restent les rappels (Q19). La fiche de lecture (terme T56) les laisse de côté. Réglage `types_debours_t56`.

### N5 — La ligne « Autres » de T56 : l'action sanitaire et sociale

**Règle.** La ligne « Autres » reçoit l'action sanitaire et sociale (ASS), comme le tableau 106 de la CNSS. Ce montant ne vient pas de la base des prestations ; il doit être le même que celui des indicateurs de l'ASS et de T76.

**Exemple.** 2024 : ASS 650 700 099 au tableau 106. Le même annuaire donne 1 571 655 315 de dépenses de l'ASS (tableau 114 et fiche signalétique).

**Sources.** Annuaire CNSS 2024, tableau 106 et fiche signalétique p. xx (« trois branches … complétées par une action sanitaire et sociale ») ; Annuaire CIPRES, glossaire p. 62 (l'ASS sert des prestations en nature) ; Kaya 2017, tableau 1, l. 395 (T76 fait sous Excel) ; Recueil v7, l. 1318 à 1337 (indicateurs de l'ASS, structure DCG) et l. 28 (peu d'intervention manuelle).

**État.** Nouvelle. Le contenu de la ligne est tranché par la pratique de la CNSS (le recueil se tait ; points_a_valider n° 53) ; le montant reste à choisir (Q18). Ligne vide en attendant (réglage `ligne_autres_t56`).

### N6 — La source de T56 : l'application ou la comptabilité

**Règle.** La CNSS publie comme prestations servies les charges techniques de sa comptabilité. T56 peut se lire dans la comptabilité (comptes 601, 602, 603 et 605), par la vue `V_BI_BALANCE_COMPTABLE` ; les paiements de l'application deviennent alors un contrôle. La répartition des comptes par branche est à demander à la DFC.

**Exemple.** 2024, AT/MP : charges techniques 1 074 113 823, soins compris ; paiements de la base 763 397 429.

**Sources.** Annuaire CNSS 2024, démarche p. xiv (les données sont mises en conformité avec le compte de résultat) et tableaux 103 à 105 (charges techniques égales au tableau 106, cinq ans sur cinq) ; Recueil v7, l. 361 (T66 : charges techniques = comptes 601, 602, 603, 605), l. 455 et 464 (T75 : prestations de N−1 par branche) et l. 28 (source automatique).

**État.** Nouvelle, option à décider (Q5). Valeur par défaut : l'application (réglage `source_t56`). Avec la comptabilité, T56 devrait retomber sur le tableau 106, si la BI répartit les comptes comme la DFC.

### N11 — Ce que T56 donne au ratio des ressources allouées aux prestations

**Règle.** Le recueil demande : prestations ordonnancées de la période ÷ cotisations ordonnancées de la période × 100, par branche, type, DR et agence ; les prestations ordonnancées sont celles qui ont été notifiées. C'est une troisième façon de dater, à la notification, proche du montant liquidé de N1. T56 ne donne ce numérateur qu'avec deux réglages : la date de notification, et peut-être sans les soins (Q20). Pour les PF, le ratio se lit aussi par trimestre.

**Exemple.** Repère pour 2024, recalculé (l'annuaire ne publie pas ce ratio) : charges techniques ÷ produits techniques des tableaux 103 à 105, soit pensions 45,96 %, PF 25,91 %, risques professionnels 9,82 % ; avec les cotisations du tableau 17 : 47,07 %, 26,42 % et 10,19 %. Le ratio du recueil sera un peu différent : il compte les prestations notifiées.

**Sources.** Recueil v7, l. 1129 à 1141, 1210 à 1214 et 1287 à 1294 (formule lue dans le document Word), note l. 1133, trimestre l. 1137 à 1139 ; Kaya 2017, tableau 2, l. 898 (la même note) ; Annuaire CIPRES, glossaire p. 65 (rapport de charges brut : prestations en espèces ÷ cotisations encaissées ; net : après les frais de fonctionnement) ; Annuaire CNSS 2024, tableaux 17 et 103 à 105 ; remarque B37 du recueil.

**État.** Nouvelle, à valider avec la DAI et la DPC. L'indicateur est hors des tableaux T50 à T57, mais il dépend des montants de T56.

### Fiche 7 — T57 aujourd'hui : qui compter (règle 7 de la fiche)

**Règle.** La requête 3 compte, à la date du décès (`IND_DATE_DECES`) : en AVS, les titulaires d'une pension directe (vieillesse RPV, retraite anticipée RPRA, invalidité RPI) en cours à leur décès (radiée au plus tôt 31 jours avant) ; en AT/MP, les titulaires d'une rente en cours (viagère ARI, ou de survivant ARSV, ARSF, ARSE, ARSA) et les victimes mortes dans l'année qui suit leur accident ; en PF, les titulaires d'un dossier dont une allocation familiale (FAF) est en cours.

**Exemple.** 2024, base d'aujourd'hui : 622 titulaires de pension directe, 16 rentiers (15 de rente viagère, 1 de rente de survivant), 10 victimes, 194 allocataires.

**Sources.** Kaya 2017, tableau 1, l. 150 à 155 (« assurés décédés déclarés » : pension directe ; rentier et sinistré ; allocataire) ; Recueil v7, l. 1251 (retraités = pensions directes), l. 152 (allocataire = titulaire du dossier), l. 194 (crédirentiers = rentiers) ; Données P96, P97, P100.

**État.** À revoir avec le recueil v7 (N2, N3). Elle mélange assurés et ayants droit : l'AT/MP compte les rentes de survivants, l'AVS écarte les pensions de survivants. Elle ne compte pas l'assuré mort en activité, avant toute pension (Q4).

### N2 — T57 : la qualité du décédé (assuré ou ayant droit) et le détail par type

**Règle.** Chaque décédé porte sa qualité et son type. Assuré : retraité de droit direct (vieillesse, anticipée, invalidité), rentier victime, sinistré, allocataire. Ayant droit : veuve, veuf, orphelin ou ascendant titulaire d'une pension ou d'une rente de survivant. Chaque branche se détaille par type ; un filtre montre les assurés, les ayants droit ou les deux.

**Exemple.** Une veuve pensionnée qui meurt : absente avec le filtre « assuré » ; comptée en AVS, type « veuve », avec le filtre « ayant droit ».

**Sources.** Recueil v7, l. 174 et 175 ; Kaya 2017, tableau 1, l. 156 (« désagréger par type de bénéficiaires ») ; Annuaire CNSS 2024, tableaux 26 et 81 (décès par type, survivants compris) ; Données P97 (2024 : 622 titulaires de pension directe, 747 pensionnés de tous types).

**État.** Nouvelle, imposée par le recueil v7. Par défaut, le filtre montre les assurés, comme le titre du tableau (réglage `qualite_deces`). La question « titulaires d'une pension directe seulement, ou tous les pensionnés décédés ? » (fiche T50-T57 Q4 ; points_a_valider n° 54) est tranchée par le recueil : les deux, au choix du filtre. Restent ouverts : l'assuré mort en activité (Q4) et les ayants droit des PF (Q10).

### N3 — T57 : les colonnes et le TOTAL

**Règle.** Colonnes PF, AT/MP, AVS et TOTAL : « Autres » est remplacé par « TOTAL ». Une ligne TOTAL additionne les cinq années.

**Exemple.** Ligne 2024 : les décès PF, AT/MP, AVS, puis leur total.

**Sources.** Recueil v7, l. 173 ; Kaya 2017, tableau 1, l. 151 (une colonne et une ligne de total).

**État.** Nouvelle, imposée par le recueil v7. La requête 3 sort encore une colonne AUTRES (toujours 0) avant TOTAL. Le TOTAL est une somme de branches : une personne présente dans deux branches compte deux fois (Q11).

### N9 — Le sinistré de T57

**Règle.** Un sinistré est une victime d'accident du travail morte de son accident. La CNSS compte les « décès immédiats » notés à la déclaration ; la requête 3 prend toute mort dans l'année qui suit l'accident.

**Exemple.** 2024 : 14 décès immédiats publiés ; 9 victimes d'un accident de 2024 mortes dans l'année dans la base.

**Sources.** Annuaire CNSS 2024, tableau 49 (et 57, 64, 66, 69 : ligne « décès immédiats », 13, 11, 13, 11, 14 de 2020 à 2024) ; Kaya 2017, tableau 1, l. 154 (les sinistrés) ; Données P97 et P100 (par année de l'accident : 20, 17, 24, 16, 9 ; le champ `DOS_DECES_IMMEDIAT` n'est rempli que pour 2 % des accidents de 2024 et 65 % de ceux de 2025).

**État.** Nouvelle, définition à décider (Q12). Réglage `delai_sinistre_jours` (à créer). Piste à mesurer : la victime dont la date du décès est celle de l'accident (P118).

### R13 — Le cliché du calcul (fiche, règle 8)

**Règle.** L'exercice N se calcule sur la copie de la base de fin mars N+1, date à laquelle la CNSS arrête sans doute son annuaire. Un décès compte à sa date, s'il est déjà saisi à ce moment-là.

**Exemple.** Retraité mort le 12/06/2024 : décès saisi en février 2025, il est dans T57 de 2024 ; saisi en mai 2025, il n'y est pas.

**Sources.** Annuaire CNSS 2024, tableau 26 ; README, R13 ; Données P99 : la base n'a pas de date de déclaration du décès (P96) ; deux indices le remplacent. D'après la dernière modification de la pension (`PE_DATE_UPDATE`) avant avril 2025, environ 444 retraités morts en 2024, pour 447 publiés (5 mois exacts sur 12, les autres à 3 près) ; d'après la dernière modification de la fiche de la personne (`IND_DATE_UPDATE`), 404 (−9,6 %). La base d'aujourd'hui en compte 619.

**État.** Proposée ; indice fort, à confirmer par la DPC (Q2 ; points_a_valider n° 2). Elle répond aussi à points_a_valider n° 55 (« l'année du décès ou celle de sa déclaration ? ») : l'année du décès, pour les décès déjà saisis fin mars N+1 ; la réponse suit donc Q2. Le seul cliché disponible aujourd'hui porte la copie du 02/02/2026.

### N7 — Les années passées d'un tableau sur cinq ans

**Règle.** Les années N−4 à N−1 reprennent les chiffres déjà produits, gardés à chaque exercice ; une correction se signale par une note.

**Exemple.** IJ de maternité 2020 : 1,53 Md publiés en 2024, alors qu'un recalcul sur la base d'aujourd'hui donne 2,19 Md (saisie).

**Sources.** Annuaire CNSS 2024, tableau 114 (donnée 2023 « modifiée », avec une note) et tableau 35 (année 2020) ; Données P92. Le chiffre de 2020 a-t-il été repris tel quel d'un annuaire précédent ? À vérifier dans l'annuaire 2020 ou 2023.

**État.** Nouvelle, proposition (Q6). L'entrepôt ne garde qu'un cliché : il faudrait garder le résultat de chaque exercice.

### N8 — Le détail par mois, direction régionale, agence, code et sexe

**Règle.** Garder pour chaque bénéficiaire, paiement et décès : le mois, la DR du dossier, l'agence, le code de prestation et le sexe. On peut alors descendre jusqu'à la prestation, sortir un trimestre (N11) et refaire les vues de la CNSS.

**Exemple.** Allocations de vieillesse liquidées en 2024 : 833, dont 507 à la direction régionale de Ouagadougou (tableau 24).

**Sources.** Recueil v7, l. 4 à 10, 29, 31, 74, 1065 et 1137 à 1139 ; Annuaire CNSS 2024, tableaux 24, 29 à 34, 39 à 46, 81, 83 à 88, 91 à 96 et 98 (par mois et par DR) ; Données A02 (la DR du dossier tombe près du publié pour les pensions : 0 à 4 % pour la retraite vieillesse, sauf les veuves).

**État.** Nouvelle ; les requêtes sortent aujourd'hui des totaux annuels. L'agence suit la règle N2 de T25-T27 (service du dossier).

---

## 3. Les cas

| Cas | Ce que fait la règle | Pourquoi |
|---|---|---|
| 1. Une allocation de vieillesse liquidée le 10/03/2024, payée en avril 2024 | T50 de 2024 : compté dans sa colonne de sexe ; le paiement d'avril va dans le montant de 2024 ; ses mois cotisés entrent dans la moyenne des services passés | Compté (D4, R9, R4) |
| 2. Une allocation de vieillesse liquidée le 20/12/2024, payée par un débours saisi le 15/01/2025 pour le mois de décembre | Effectif : T50 de 2024. Montant : 2024 avec `date_montant` = 'MOIS' ; 2025 avec 'SAISIE' | Selon un réglage (D4 ; R4 ; Q1) |
| 3. Un assuré meurt en activité, avant toute pension ; sa veuve et ses deux enfants reçoivent chacun une part d'allocation de survivants (RASC, RASO), liquidées en 2024 | T51 de 2024 : trois bénéficiaires, la veuve en femmes, chaque enfant dans son sexe (l'enfant, pas son tuteur). L'assuré lui-même n'est pas dans T57 aujourd'hui | Compté (R2, R5) ; T57 : question (Q4) |
| 4. Un ouvrier blessé reçoit trois IJ d'accident liquidées en 2024 (arrêt prolongé deux fois) | T52 de 2024 : compté une fois ; le montant comprend les trois IJ | Compté une fois (R5) |
| 5. Une victime arrêtée en octobre 2024 dont l'IJ n'est notifiée qu'en mai 2025 (les IJ AT/MP attendent souvent 7 mois, fait B24 du schéma des règles) | Absente de T52 de 2024 ; comptée dans T52 de 2025 | Pas compté en 2024 (D4) |
| 6. Une salariée : IJ de maternité liquidée en décembre 2024, payée en décembre 2024 et en février 2025 | T54 de 2024 : comptée. Montant : aujourd'hui seul le paiement de décembre ; avec le « montant liquidé » du recueil, les deux | Selon un réglage (N1 ; `montant_t54_t55` ; Q3) |
| 7. L'épouse non salariée d'un assuré attend un enfant ; elle a droit à l'allocation prénatale, pas à l'IJ | T55 : comptée (FEMMES) ; absente de T54, l'IJ étant réservée aux salariées. Code à vérifier : sans doute FAP (allocation prénatale des PF, 56 personnes en 2024) plutôt que MAP, qui va presque toujours avec une IJ | Compté (R2) ; code à vérifier (Q22) |
| 8. Un accidenté reçoit en 2024 un capital à la place de sa rente (rachat), versé par un débours d'ajustement RR | T56 AT/MP : pas compté aujourd'hui ; à ajouter (25,4 M de rachats publiés en 2024) | À surveiller (Fiche 6, N4) |
| 9. La CNSS paie à une clinique les soins d'un accidenté du travail | T56 AT/MP : compté seulement si le paiement est un remboursement (débours RB) de la base : 18,8 M en 2024 pour 139,7 M de soins publiés | Question (Fiche 6 ; N6 ; Q16) |
| 10. Un retraité mort le 12/06/2024, décès saisi en février 2025 | T57 de 2024, AVS : compté, car la copie de fin mars 2025 connaît son décès | Compté (R13, Fiche 7) |
| 11. Un retraité mort le 10/11/2024, décès saisi en mai 2025 | Absent de T57 de 2024 calculé fin mars 2025, comme de l'annuaire. Il apparaîtrait si l'on recalculait 2024 plus tard (colonne N−1 du T57 de 2025) | À surveiller (R13, N7 ; Q6) |
| 12. Une veuve titulaire d'une pension de veuve (RPSV) meurt en 2024 | Filtre « assuré » : pas comptée. Filtre « ayant droit » : comptée en AVS, type « veuve ». L'annuaire la compte (103 veuves décédées en 2024) | Selon un réglage (N2 ; `qualite_deces`) |
| 13. Un ouvrier meurt le jour de son accident du travail, le 03/02/2024 | T57 de 2024, AT/MP, type « sinistré » : compté. La CNSS le compterait parmi ses décès immédiats (14 en 2024) | Compté (Fiche 7, N9) |
| 14. Une victime blessée en mars 2024 meurt de ses blessures en août 2024, sans rente | La requête 3 la compte comme sinistrée ; ce n'est pas un décès immédiat de l'annuaire | Question (N9 ; `delai_sinistre_jours` ; Q12) |
| 15. Un rentier dont la rente viagère a été radiée en 2022 (plus de certificat de vie) meurt en 2024 | Pas compté dans T57 : sa rente n'était plus en cours. L'annuaire compte plus de rentiers décédés que la base (24 pour 16 en 2024) | À surveiller (Fiche 7 ; `marge_radiation_jours` ; Q14) |
| 16. Un retraité qui touche encore des allocations familiales meurt en 2024 | Compté en AVS et en PF (une fois par branche) ; le TOTAL de la ligne 2024 le compte deux fois | Question (N3 ; Q11) |
| 17. Une allocation de vieillesse de 2024 avec 489 mois cotisés (40 ans) | Entre telle quelle dans la moyenne des services passés de T50 (on ne coupe qu'au-dessus de 600 mois) | Question (R9 ; Q9) |
| 18. Un assuré salarié meurt en 2024 sans aucune prestation ouverte à son nom ; ses survivants touchent une pension ou une allocation | Absent de T57 aujourd'hui, en AVS comme ailleurs ; au sens littéral du recueil, c'est un « assuré décédé » | Question (Fiche 7, N2 ; `actifs_decedes` ; Q4) |

---

## 4. Face à l'annuaire CNSS 2024

| Tableau de l'annuaire | Ce qu'on compare | Nous | Publié | Écart | Verdict | Ce que ça change |
|---|---|---|---|---|---|---|
| Tableau 24 (p. 19) | Allocations de vieillesse liquidées en 2024 (effectif de T50) | 831 personnes (832 allocations) ; 873 saisies ; 753 payées (P92) | 833 (DRO 507, DRB 201, DRN 53, DRF 46, DRD 26) | −2 (−0,24 %) ; saisies +4,8 % ; payées −9,6 % | confirme | D4 : le titre dit « liquidées ». La CNSS donne aussi le mois et la DR : les garder (N8) |
| Tableaux 34 et 28 (p. 28 et 22) | Montant des allocations de vieillesse 2024 (T50) | Saisie 497 818 548 ; mois payé 517 334 647 (P92) | 497 726 931 | Saisie +0,02 % ; mois payé +3,9 % | précise | Appuie fortement `date_montant` = 'SAISIE' pour cette ligne (Q1) |
| Tableaux 34 et 28 | Montant des allocations de survivants 2024 (T51) | Saisie 190 149 232 (RASC 49 843 507, RASO 127 925 146, RASA 12 380 579) ; mois payé 187 083 247 | 197 906 991 | Saisie −3,9 % ; mois payé −5,5 % | précise | La saisie fait mieux ; 4 % restent à expliquer. L'effectif de T51 (571 en 2024) n'est pas publié |
| Tableaux 28, 41 et 90 (p. 22, 33, 71) | Le rythme mois par mois des montants de 2024 (R4) | Corrélation avec le publié : vieillesse −0,12 au mois payé, 0,75 à la saisie ; PF 0,26 et 0,92 ; AT/MP −0,08 et 0,84 (P105) | Profils mensuels | Sur l'année : pension de vieillesse +0,68 % au mois payé, +1,53 % à la saisie ; branche AVS +1,3 % et +1,8 % | précise | Penche nettement pour la saisie (la préparation du paiement), sans trancher : le niveau de l'année va dans l'autre sens pour les plus grosses lignes. Vérifier l'approbation mois par mois (P115) |
| Tableau 35 (p. 30) | IJ de maternité, effectifs 2020 à 2024 (T54) | Liquidées : 3 438, 3 810, 3 834, 4 069, 3 890 ; payées : 4 421 à 5 125 | 3 428, 3 810, 3 973, 4 050, 3 893 | +0,3 %, 0, −3,5 %, +0,5 %, −0,1 % ; payées +24 à +29 % | précise | Compter les IJ liquidées (D4, comme le voulait Kaya 2017) retombe à 1 % près, sauf 2022 ; compter les IJ payées, non. Même verdict que la famille T25-T27 pour ces chiffres : la date de la liquidation ne se départage pas d'une autre date proche (2022, 2023) |
| Tableau 35 (p. 30) | IJ de maternité, montants 2020 à 2024 (T54) | Saisie : 2 193 049 270 ; 2 357 360 490 ; 2 552 489 880 ; 2 757 274 340 ; 2 749 474 700 | 1 530 554 843 ; 2 357 429 788 ; 2 560 989 546 ; 2 817 073 092 ; 2 865 630 056 | Saisie : +43,3 %, −0,003 %, −0,3 %, −2,1 %, −4,1 % ; mois payé : +42,5 %, −0,5 %, +1,1 %, −2,0 %, −4,2 % | écart à expliquer | 2021 et 2022 tombent juste ; 2023 et 2024 restent 2 à 4 % sous le publié, quelle que soit la date ; 2020 publié semble incomplet. Mesurer le montant liquidé (N1, P116) avant de conclure |
| Tableau 39 (p. 32) | IJ de maternité 2024 par mois et par DR (T54) | 3 890 sur l'année ; pas encore au mois | 3 893 (DRO 2 822, DRB 575, DRN 246, DRF 165, DRD 85) | −0,1 % sur l'année | précise | La somme des mois fait l'effectif de l'année : une mère compte sans doute le mois de la liquidation, à vérifier mois par mois. Garder mois et DR (N8) |
| Tableau 40 (p. 32) | Allocations prénatales 2024, montant (T55) | Saisie 52 149 000 (MAP 50 876 500, FAP 1 272 500) ; mois payé 50 677 500 | 52 789 000 | Saisie −1,2 % ; mois payé −4,0 % | précise | La saisie fait mieux. L'effectif de T55 (3 912 en 2024) n'est pas publié |
| Tableau 40 (p. 32) | Total de la branche PF 2024 (ligne PF de T56) | Mois payé 10 750 873 342 ; saisie 10 802 063 700 ; aucun frais médical de maternité | 11 180 191 425 (dont frais médicaux 25 030 869) | Mois payé −3,8 % ; saisie −3,4 % (−378 M : allocations familiales −236 M, soit −2,9 % ; IJ de maternité −116 M ; frais médicaux −25 M ; prénatales −0,6 M) | écart à expliquer | Les allocations familiales font 62 % de l'écart ; les frais médicaux de maternité manquent dans la base (Q17). Voir N6 |
| Tableau 26 (p. 21) | Retraités décédés en 2024 : la date d'arrêt (R13) | 619 aujourd'hui ; environ 444 d'après la pension modifiée avant avril 2025 ; 404 d'après la fiche de la personne (P99) | 447 (vieillesse) | Aujourd'hui +38,5 % ; avec la pension −0,7 % (5 mois exacts) ; avec la personne −9,6 % | précise | Indice fort pour R13 : calculer N sur le cliché de fin mars N+1. La base n'a pas de date de déclaration ; à confirmer par la DPC (Q2) |
| Tableau 26 (p. 21) | Qui est compté parmi les pensionnés décédés (N2) | 2024, base d'aujourd'hui : pension directe 622 ; tous les pensionnés 747 (P97) | 648 : droits directs 451 (vieillesse 447, invalidité 3, anticipée 1) ; ayants droit 197 (veuves 103, veufs 3, orphelins 2, ascendants 89) | Droits directs +37,9 % ; tous +15,3 % (pas encore à la date d'arrêt) | précise | La CNSS détaille par type et compte les ayants droit : c'est ce que veut le recueil (N2). 89 ascendants décédés pour 59 pensions d'ascendant en cours : impossible (Q15) |
| Tableau 81 (p. 62) | Rentiers décédés en 2024 (T57, AT/MP) | 16 : 15 rentes viagères, 1 rente de survivant (P97) | 24 : rentes viagères 21, veuves et veufs 2, ascendants 1 (DRO 10, DRB 13, DRD 1) | −8 (−33 %) ; viagères −6 (−28,6 %) | écart à expliquer | À l'inverse des pensions, la base trouve moins de décès : la condition « rente en cours » écarte peut-être des rentiers ; mesurer sans elle (P117 ; Q14) |
| Tableau 49 (p. 39 ; et 57, 64, 66, 69) | Décès immédiats d'accidents du travail, 2020 à 2024 (sinistrés de T57) | Victimes mortes dans l'année, par année de l'accident : 20, 17, 24, 16, 9 (P100) | 13, 11, 13, 11, 14 (tous des hommes en 2024, tableau 57) | +7, +6, +11, +5, −5 | précise | La CNSS ne compte que la mort sur le coup ; la requête 3 compte aussi les morts plus tardives (N9 ; Q12, Q13) |
| Tableau 97 (p. 74) | IJ AT/MP, montants 2020 à 2024 (T52) | Saisie : 84 625 089 ; 83 914 536 ; 62 724 081 ; 93 389 501 ; 51 859 178. 2024 au décaissement : 60 773 127 (P98) | 94 182 006 ; 94 190 676 ; 69 008 587 ; 81 867 247 ; 65 420 082 | Saisie : −10,1 %, −10,9 %, −9,1 %, +14,1 %, −20,7 % ; mois payé −29,7 % en 2024 ; décaissement −7,1 % en 2024 | écart à expliquer | Pour les IJ, une partie de l'écart peut venir de la date retenue : le décaissement fait bien mieux en 2024 (à mesurer sur cinq ans, P115). Effectif non publié (195 en 2024 chez nous) |
| Tableau 97 (p. 74) | Allocations d'incapacité, montants 2020 à 2024 (T53) | Saisie : 25 902 000 ; 33 278 400 ; 18 457 200 ; 29 631 600 ; 17 812 800 | 25 558 532 ; 34 091 560 ; 19 188 366 ; 34 496 568 ; 20 034 111 | Saisie : +1,3 %, −2,4 %, −3,8 %, −14,1 %, −11,1 % | écart à expliquer | La saisie fait mieux 4 années sur 5 ; l'écart est plus grand en 2023 et 2024. Effectif non publié (53 en 2024 chez nous) |
| Tableaux 90, 96 et 97 (p. 71 et 74) | Rachats de rente, 2020 à 2024 | 21 519 361 en 2024, versés par débours d'ajustement RR ; hors de la requête 2 (P104) | 22 314 632 ; 24 935 514 ; 37 970 784 ; 38 896 780 ; 25 366 909 | 2024 : −15,2 % | tranche | Le rachat est une prestation servie : l'ajouter à T56 (N4) ; points_a_valider n° 52 réglé pour les rachats |
| Tableaux 82, 88, 89 (p. 63, 69, 70 ; et 98, 99) | Soins des AT/MP, 2020 à 2024 (T56 AT/MP) | Remboursements (débours RB) : 26 779 244 ; 35 445 200 ; 39 322 757 ; 32 481 449 ; 18 787 945 | 141 230 596 ; 184 562 935 ; 171 679 077 ; 142 926 750 ; 139 668 695 | La base couvre 19 %, 19 %, 23 %, 23 %, 13 % | écart à expliquer | Les soins sont payés hors des débours de la base : T56 AT/MP restera 13 à 29 % sous le publié (24 à 29 % en 2023 et 2024) tant que la source manque (Q16) ou qu'on ne lit pas la comptabilité (N6) |
| Tableau 106 (p. 80) | Prestations servies par branche, 2020 à 2024 (T56) | AVS 35,66 / 38,00 / 40,85 / 44,17 / 47,24 Md ; PF 8,17 / 8,56 / 9,01 / 10,36 / 10,75 Md ; AT/MP 0,80 / 0,81 / 0,80 / 0,81 / 0,76 Md (mois payé) | Pensions 35,23 à 46,63 Md ; PF 7,41 à 11,18 Md ; RP 0,92 à 1,07 Md | AVS +1,2 %, +0,04 %, +0,4 %, +1,3 %, +1,3 % ; PF +10,2 %, −0,06 %, −1,3 %, −5,9 %, −3,8 % ; AT/MP −13,2 % à −28,9 % ; total des trois branches de +2,5 % à −0,6 % | écart à expliquer | Le total retombe (−0,2 % en 2024), mais parce que des écarts de sens contraire se compensent (AVS +608 M, PF −429 M, AT/MP −311 M). Chaque écart a sa propre cause ; rien ne prouve qu'ils soient liés |
| Tableau 106 et fiche signalétique (p. 80 et xx) ; tableau 114 (p. 85) | La 4e ligne de T56 : l'action sanitaire et sociale | Ligne vide | ASS 1 164 507 418 ; 390 469 266 ; 286 565 735 ; 468 824 723 ; 650 700 099. Dépenses de l'ASS 2024 : 1 571 655 315 (tableau 114 et fiche signalétique) | Ligne entière | tranche | Dans la pratique de la CNSS, la ligne « Autres » est l'ASS (N5). Mais deux montants pour 2024 dans le même annuaire : le choix revient à la CNSS (Q18) |
| Tableaux 103 à 105 (p. 79) et démarche (p. xiv) | Les charges techniques par branche | Paiements de la base (voir le tableau 106) | Charges techniques 2024 : PF 11 180 191 425 ; RP 1 074 113 823 ; pensions 46 629 625 556 | Celui du tableau 106 | précise | Les prestations servies de l'annuaire sont les charges techniques de la comptabilité. Option N6 (Q5) |
| Tableaux 17 et 103 à 105 (p. 14 et 79) | Prestations ÷ cotisations, 2024 (N11) | Pas calculé | Ratio non publié ; recalculé (charges techniques ÷ produits techniques, tableaux 103 à 105) : pensions 45,96 % ; PF 25,91 % ; RP 9,82 %. Avec les cotisations du tableau 17 : 47,07 % ; 26,42 % ; 10,19 % | — | précise | Repère pour le ratio des ressources du recueil, qui compte toutefois les prestations notifiées, pas payées |
| Tableau 114 et tableau 35, année 2020 (p. 85 et 30) | Les années passées d'un tableau sur cinq ans (N7) | Recalcul de 2020 : 2 193 049 270 (saisie) | 1 530 554 843 | +43 % si l'on recalcule 2020 | précise | La CNSS corrige parfois une année passée avec une note (tableau 114). Le chiffre de 2020 du tableau 35 a-t-il été repris tel quel ? À vérifier dans l'annuaire 2020 ou 2023 (Q6) |
| Tableau 20 (p. 17) | Les orphelins pensionnés par âge et par sexe | — | Moins de 25 ans : 9 478 (exactement les orphelins du tableau 18) | — | confirme | R5 : la CNSS compte l'orphelin, garçon ou fille, et non son tuteur |
| Tableaux 29 à 33 (p. 23 à 27) | Assurance vieillesse servie chaque mois de 2024, une DR par tableau | Pas calculé | DRO 32 173 124 168 … DRD 723 818 987 | — | précise | Modèle du détail par DR et par mois (N8) ; permet de tester la date des montants (R4) direction par direction |
| Tableaux 91 à 95 (p. 71 à 73) | Prestations en espèces AT/MP chaque mois de 2024, une DR par tableau | Pas calculé | DRO 621 802 712 … DRD 21 297 717 | — | précise | IJ et allocations d'incapacité par DR et par mois (T52, T53, N8) : pour repérer où manquent les paiements AT/MP |
| Tableaux 83 à 87 (p. 64 à 68) | Soins AT/MP chaque mois de 2024, une DR par tableau | Pas calculé | DRO 96 737 361 … DRD 6 087 089 | — | précise | Pour repérer par DR les soins absents de la base (Q16) |
| Tableaux 78 et 80 (p. 61 et 62 ; et 75) | Les rentes en cours fin 2024, face aux rentiers décédés | Pas calculé | 1 255 rentes viagères, 977 rentes d'ayants droit ; 44 nouvelles rentes viagères en 2024 (tableau 75) ; décès 2024 : 21 et 3 (tableau 81) | Flux des viagères : 1 246 (fin 2023) + 44 − 21 = 1 269 pour 1 255 publiées (14 autres sorties) | précise | Le stock à mettre face aux décès de rentiers (Fiche 7, Q14) ; les 14 autres sorties sont peut-être des rachats |
| Tableaux 82, 88 et 89, ligne « Funéraires » (p. 63, 69, 70) | Frais funéraires des AT/MP, 2020 à 2024 | Pas calculé | 0 ; 600 000 ; 0 ; 700 000 ; 900 000 | — | non comparable | Peut-être un indice des sinistrés décédés de T57 (N9) : à faire expliquer, à vérifier |
| Aucun tableau | Effectifs de T51, T52, T53 et T55 ; services passés de T50 ; sexe des bénéficiaires de T50 à T53 ; décès d'allocataires (PF) ; décès sur cinq ans | 2024 : T51 571, T52 195, T53 53, T55 3 912 ; 95,5 mois ; 194 allocataires décédés | rien | — | pas publié | Ces chiffres se valident avec la CNSS (fiche T50-T57), pas avec l'annuaire |

**En clair, ce que l'annuaire confirme :**
- une personne compte l'année où sa prestation est liquidée (D4) : 831 allocations de vieillesse pour 833 (tableau 24). Les IJ de maternité tombent aussi juste (3 890 pour 3 893, tableau 35), mais ce tableau ne départage pas la date : il précise ;
- l'orphelin compte, pas son tuteur (R5) ;
- la maternité est dans les PF (R1), et un accessoire suit la pension (A1) : ce sont les cartes des familles T25-T27 et T28-T31 (tableaux 28, 34, 40 et 41), rappelées ici sans ligne dans le tableau ci-dessus.

**Ce qu'il tranche, faute de source plus forte :** le rachat de rente est une prestation servie de T56 (N4) ; la ligne « Autres » de T56 est l'action sanitaire et sociale (N5).

**Ce qu'il précise sans trancher :** les IJ de maternité liquidées (tableau 35, D4) ; la date d'un paiement (le rythme suit la saisie, le niveau reste partagé : R4, Q1) ; la date d'arrêt de l'annuaire (R13, indice fort) ; qui est compté parmi les décédés (N2) ; le décès immédiat (N9) ; la source comptable de T56 (N6).

**Ce qu'il faut expliquer :** les soins AT/MP (13 % dans la base) et les IJ AT/MP ; les montants d'IJ de maternité de 2023 et 2024 (−2 à −4 %) ; les allocations familiales (−2,9 %) ; les rentiers décédés (16 pour 24) ; les 89 ascendants décédés.

**Ce qu'il ne publie pas :** les effectifs de T51, T52, T53 et T55, les services passés, le sexe, les allocataires décédés, les décès sur cinq ans.

---

## 5. Les réglages

Ils s'écrivent dans la CTE `parametres`, le bloc de réglages en tête de chaque requête. Changer une valeur ne demande pas de réécrire la requête. Une décision attendue de la CNSS ne bloque rien : la valeur retenue s'applique en attendant. « À créer » : le choix est aujourd'hui écrit en dur dans la requête, ou n'existe pas.

| Réglage | Requête | Valeur retenue | Autre valeur | Ce que ça change | Qui décide |
|---|---|---|---|---|---|
| `annee_n` | requêtes 1, 2 et 3 | l'exercice calculé, donné au lancement (2024 par exemple) | — | Fixe les cinq années N−4 à N | la BI, à chaque lancement |
| `date_montant` | requête 1 (T50 à T53) | 'MOIS' : le mois payé (`DEB_DATE_EFFET`), comme aujourd'hui | 'SAISIE' : l'année de la saisie du paiement (`DEB_DATE_INSERT`), rappels compris (proposition BI) | 2024 : T50 517 334 647 ('MOIS') ou 497 818 548 ('SAISIE') pour 497 726 931 publiés. Le même réglage existe dans les requêtes T31, T32-T41 et T42-T49 : décider une fois pour tout le domaine | DPC, avec la DCP (Q1) |
| `date_montant` | requête 2 (T56) | 'MOIS' | 'SAISIE' | 2024 : AVS 47 237 434 040 (+1,3 %) ou 47 486 010 786 (+1,8 %) ; PF 10 750 873 342 (−3,8 %) ou 10 802 063 700 (−3,4 %) | même décision (Q1) |
| `montant_t54_t55` (à créer) | requête 1 (T54, T55) | 'LIQUIDE' : tous les paiements des prestations liquidées dans l'année, quelle que soit leur date (le recueil) | 'PAYE' : les paiements de l'année, comme aujourd'hui | Suit le recueil v7. Chiffre à mesurer (P116). Le montant d'une année n'est complet qu'après ses derniers paiements | tranché par le recueil (l. 159, 166) ; la DCP précise le sens (Q3) |
| `mois_cotises_max` (à créer ; en dur : 600) | requête 1 (T50) | 600 | le nombre de mois au-delà duquel une pension, et non une allocation, est due (à faire donner par la CNSS) | Écarte des services passés les allocations aux carrières trop longues pour une allocation (489 mois au plus en 2024) | DCP (Q9) |
| `qualite_deces` (à créer) | requête 3 (T57) | 'ASSURE' : retraités de droit direct, rentiers victimes, sinistrés, allocataires (comme le titre et Kaya 2017) | 'AYANT_DROIT' ou 'TOUS'. Mieux : une colonne QUALITE que l'utilisateur filtre dans le rapport | 2024, AVS, base d'aujourd'hui : 622 décès (assurés) ou 747 (tous les pensionnés) | l'utilisateur du rapport (filtre demandé par le recueil, l. 175) ; la valeur par défaut avec la DPC |
| `actifs_decedes` (à créer) | requête 3 (T57) | NON : l'assuré mort en activité n'est pas compté (Kaya 2017 ne le cite pas) | OUI : les assurés dont le décès ouvre une pension ou une allocation de survivants sans pension directe à leur nom | À mesurer (P117) | DCP, avec la DPC (Q4) |
| `delai_sinistre_jours` (à créer ; en dur : 366) | requête 3 (T57, AT/MP) | 366 : mort dans l'année qui suit l'accident | 0 : mort le jour de l'accident (date du décès = date de l'accident), comme les « décès immédiats » | Par année de l'accident : 20, 17, 24, 16, 9 avec 366 jours, pour 13, 11, 13, 11, 14 publiés ; 0 jour : à mesurer (P118) | DCP (service AT/MP) (Q12) |
| `marge_radiation_jours` (à créer ; en dur : 31) | requête 3 (T57) | 31 : pension ou rente radiée au plus tôt un mois avant le décès | aucune condition : toute pension ou rente commencée avant le décès | Rentiers morts en 2024 : 16 avec la marge, 24 publiés ; sans condition : à mesurer (P117) | la BI après la mesure, puis la DCP (Q14) |
| `types_debours_t56` (à créer ; en dur : PM et RB) | requête 2 (T56) | paiements (PM), remboursements (RB) et rachats de rente (débours d'ajustement RR) | sans les rachats, comme aujourd'hui ; ou avec aussi les rappels de moins-perçus | AT/MP 2024 : +21 519 361 avec les rachats. Tous les ajustements porteraient l'AVS de +1,3 % à +2,6 % | rachats : tranché par l'annuaire (N4) ; rappels : DCP, avec la DPC (Q19) |
| `nature_t56` (à créer) | requête 2 (T56) | OUI : soins compris (une prestation est un droit en espèces ou en nature) | NON : prestations en espèces seules, pour le ratio des ressources (rapport de charges brut) | 2024 : 18 787 945 de soins AT/MP dans la base | DAI et DPC (Q20) |
| `ligne_autres_t56` (à créer) | requête 2 (T56) | vide | l'ASS, montant fourni hors de la base des prestations | 2024 : +650 700 099 avec le tableau 106, +1 571 655 315 avec le tableau 114 | DPC, avec la DPASS et la DCG (Q18) |
| `source_t56` (à créer) | requête 2 (T56) | 'DEBOURS' : les paiements de l'application | 'COMPTA' : les charges techniques par branche (comptes 601, 602, 603, 605), lues par `V_BI_BALANCE_COMPTABLE` | 'COMPTA' devrait retomber sur le tableau 106 et sur T66 à T68, si la répartition par branche est celle de la DFC ; 'DEBOURS' donne AVS +1,3 %, PF −3,8 %, AT/MP −28,9 % en 2024 | DPC et DFC ; la DSI pour l'accès (Q5) |
| Le cliché (valeur donnée au lancement, pas un réglage de `parametres`) | requêtes 1, 2 et 3 | le dernier cliché de l'entrepôt (092026, copie du 02/02/2026) | le cliché de fin mars N+1 (R13) | T57, retraités morts en 2024 : 619 aujourd'hui, environ 444 fin mars 2025, 447 publiés. L'entrepôt ne garde qu'un cliché : lancer le calcul de N fin mars N+1 et garder le résultat | la DPC confirme la date (Q2) ; puis la BI planifie |
| Les années passées (façon de produire, pas un réglage de `parametres`) | requêtes 1, 2 et 3 | recalculées sur le cliché lu, comme aujourd'hui | reprises des résultats gardés à chaque exercice (N7) | IJ de maternité 2020 : 2,19 Md recalculés, 1,53 Md publiés | DPC (Q6) |

---

## 6. Les questions ouvertes

Chaque question a une valeur par défaut dans la requête : la réponse change un réglage, pas le travail. Les sigles des directions viennent de l'annuaire CNSS 2024 : DCP (Direction centrale des prestations), DPC (Direction de la prospective et de la coopération, dont le Service études et statistique fait l'annuaire), DSI (Direction des systèmes d'information), DFC (Direction financière et comptable), DAI (Direction de l'audit interne), DPASS (Direction de la prévention et de l'action sanitaire et sociale). DCG : sigle du recueil, non développé (sans doute la Direction de la planification et du contrôle de gestion, DPCG : à faire préciser). Les cinq premières questions sont celles qui pèsent le plus.

### Q1 — Dans votre annuaire, un paiement est-il rangé dans le mois et l'année qu'il paie, dans le mois où il est saisi (rappels compris), ou dans le mois où il est versé ?
**À qui :** DPC (Service études et statistique), avec la DCP.
**En attendant :** l'année du mois payé (`date_montant` = 'MOIS') ; proposition BI : passer à 'SAISIE'.
**Pourquoi on la pose :** le rythme mois par mois des tableaux 28, 41 et 90 suit la saisie (corrélation 0,75 à 0,92), pas le mois payé (−0,12 à 0,26). Mais sur l'année, la pension de vieillesse tombe mieux au mois payé (+0,68 % contre +1,53 %). Le choix change les montants de T31 à T56 de 1 à 4 %.
**Déjà posée :** au point du 02/11/2026, sans réponse ; README, R4 ; points_a_valider n° 1 ; fiche T50-T57 Q1 ; TODO, tâche 18. Même question que T28-T31 Q24 et T32-T41 Q2, avec la même valeur en attendant : une seule réponse vaut pour tout le domaine. La proposition de passer à 'SAISIE' est propre à cette famille.

### Q2 — Votre annuaire de l'année N est-il arrêté vers la fin mars N+1 ?
**À qui :** DPC.
**En attendant :** la requête lit le dernier cliché ; prévoir de lancer le calcul de l'exercice N fin mars N+1.
**Pourquoi on la pose :** les retraités morts en 2024 dont la pension a été modifiée pour la dernière fois avant avril 2025 sont environ 444, pour 447 publiés ; la base d'aujourd'hui en compte 619. La base n'a pas de date de déclaration du décès : ce n'est qu'un indice.
**Déjà posée :** README, R13 ; points_a_valider n° 2 ; fiche T50-T57 Q2 ; même question que T32-T41 Q3.

### Q3 — Pour T54 et T55, le « montant liquidé » est-il la somme due pour les indemnités et allocations liquidées dans l'année, même payée l'année suivante ?
**À qui :** DCP.
**En attendant :** oui (`montant_t54_t55` = 'LIQUIDE'), dès que la requête est revue ; aujourd'hui, la requête prend les paiements de l'année.
**Pourquoi on la pose :** le recueil demande le montant liquidé ; votre annuaire publie des montants « perçus » (tableau 35). En 2024, les paiements de la base font 2,75 Md pour 2,87 Md publiés (−4 %).
**Déjà posée :** remarque B15 du recueil.

### Q4 — Un assuré mort en activité, dont le décès ouvre une pension ou une allocation de survivants, compte-t-il dans T57 (branche AVS, qualité « assuré ») ?
**À qui :** DCP, avec la DPC.
**En attendant :** non (`actifs_decedes` = NON) : seuls les titulaires d'une pension directe comptent en AVS, comme le dit Kaya 2017.
**Pourquoi on la pose :** le recueil dit « assurés décédés » ; un assuré mort en activité en est un, et son décès ouvre une pension ou une allocation à ses survivants (fait B9 du schéma des règles), mais il n'a pas de pension à son nom, et la requête ne le voit pas.
**Déjà posée :** README (« bénéficiaires décédés, ou assurés décédés qui ouvrent un droit aux survivants ? »).

### Q5 — Pour T56, faut-il prendre les paiements de l'application des prestations, ou les charges techniques de la comptabilité (comptes 601, 602, 603, 605), comme votre tableau 106 ?
**À qui :** DPC et DFC (la DSI pour l'accès).
**En attendant :** les paiements de l'application (`source_t56` = 'DEBOURS').
**Pourquoi on la pose :** votre annuaire met ses données en accord avec le compte de résultat ; ses prestations servies sont les charges techniques (tableaux 103 à 105). Les paiements de la base retombent sur le total à 0,2 % en 2024, mais s'écartent par branche (+1,3 %, −3,8 %, −28,9 %).
**Déjà posée :** nouvelle.

### Q6 — Dans un tableau sur cinq ans, les années passées reprennent-elles les chiffres déjà publiés ?
**À qui :** DPC.
**En attendant :** les années N−4 à N−1 sont recalculées sur le cliché lu.
**Pourquoi on la pose :** un recalcul fait bouger les années passées (décès, radiations et rappels saisis tard). Votre tableau 114 signale une correction par une note ; IJ de maternité 2020 : 1,53 Md publiés, 2,19 Md dans la base.
**Déjà posée :** nouvelle (signalée en tête de `t50_t57_requete_proposee.sql`).

### Q7 — Quel champ de la base garde le montant calculé à la liquidation d'une prestation ?
**À qui :** DSI.
**En attendant :** montant liquidé = somme des paiements (statut D ou P) de la prestation, quelle que soit leur date.
**Pourquoi on la pose :** pour calculer le montant liquidé de T54 et T55 sans attendre les derniers paiements ; la table des étapes (étape du calcul initial, CIP) en porte peut-être un.
**Déjà posée :** nouvelle.

### Q8 — Pour T51, T52 et T53, compte-t-on aussi les personnes dont la prestation est liquidée dans l'année, comme pour T50 et T54 ?
**À qui :** DCP.
**En attendant :** oui : la personne qui a une prestation du tableau notifiée dans l'année.
**Pourquoi on la pose :** Kaya 2017 le dit pour T54 et T55, votre annuaire le fait pour T50 et T54 ; T51 à T53 n'ont pas de chiffre publié. Compter les payées changerait l'effectif de 2024 : T51 420 pour 571 (−26 %), T52 155 pour 195 (−21 %), T53 56 pour 53 (+6 %).
**Déjà posée :** README (question T50 à T53) ; points_a_valider n° 49.

### Q9 — Au-delà de combien de mois cotisés un assuré a-t-il droit à une pension plutôt qu'à une allocation de vieillesse ?
**À qui :** DCP.
**En attendant :** tous les mois de 1 à 600 entrent dans la moyenne des services passés de T50 (`mois_cotises_max` = 600).
**Pourquoi on la pose :** une allocation de vieillesse va à une carrière trop courte pour une pension ; pourtant une allocation a 489 mois cotisés (40 ans). Ces valeurs pèsent sur la moyenne de T50.
**Déjà posée :** README (« une allocation de vieillesse peut-elle correspondre à 489 mois cotisés ? ») ; points_a_valider n° 62 ; fait B16 du schéma des règles.

### Q10 — Avec le filtre « ayant droit » de T57, faut-il compter dans les PF les conjoints et les enfants d'allocataires décédés ?
**À qui :** DCP, avec la DPC.
**En attendant :** PF = allocataires seulement (titulaires du dossier).
**Pourquoi on la pose :** en AVS et en AT/MP, les ayants droit sont clairs (veuves, orphelins, ascendants pensionnés ou rentiers) ; en PF, votre annuaire nomme aussi des conjoints (39 856) et des enfants (268 481) parmi les bénéficiaires (tableau 37).
**Déjà posée :** remarque B26 du recueil. (points_a_valider n° 54, « pension directe ou tous les pensionnés ? », est tranché par le recueil : voir N2.)

### Q11 — Dans T57, une personne comptée dans deux branches (un retraité aussi allocataire) compte-t-elle deux fois dans le TOTAL ?
**À qui :** DPC.
**En attendant :** oui : TOTAL = somme des branches.
**Pourquoi on la pose :** le recueil et Kaya 2017 demandent un total ; une somme compte deux fois la même personne, un total de personnes non.
**Déjà posée :** nouvelle.

### Q12 — Un sinistré décédé est-il seulement la victime morte sur le coup (vos « décès immédiats »), ou aussi celle qui meurt de son accident des mois après ?
**À qui :** DCP (service AT/MP).
**En attendant :** toute victime morte dans l'année qui suit son accident (`delai_sinistre_jours` = 366).
**Pourquoi on la pose :** par année de l'accident, la base trouve 20, 17, 24, 16 et 9 victimes mortes dans l'année, pour 13, 11, 13, 11 et 14 décès immédiats publiés (tableau 49).
**Déjà posée :** points_a_valider n° 56 ; README (question T50 à T53).

### Q13 — Où la base garde-t-elle les « décès immédiats » de vos tableaux d'accidents du travail pour les années d'avant 2025 ?
**À qui :** DSI.
**En attendant :** la règle de l'année qui suit l'accident.
**Pourquoi on la pose :** le champ `DOS_DECES_IMMEDIAT` de la table `DOSSIER` n'est rempli que pour 2 % des accidents de 2024 et 65 % de ceux de 2025. La table `TSINISTRE` (plus remplie depuis décembre 2024) a un champ du même nom, pas encore lu.
**Déjà posée :** points_a_valider n° 66 (en partie) ; README, questions à la DSI.

### Q14 — Pour vos 24 rentiers décédés de 2024 (tableau 81), la rente devait-elle être en cours au décès ?
**À qui :** DPC, avec la DCP.
**En attendant :** rente en cours au décès, radiée au plus tôt 31 jours avant (`marge_radiation_jours` = 31).
**Pourquoi on la pose :** la base trouve 16 rentiers décédés en 2024 (15 rentes viagères pour 21 publiées), alors que pour les pensions elle en trouve plus que l'annuaire.
**Déjà posée :** nouvelle.

### Q15 — D'où viennent les 89 décès de pensionnés ascendants de 2024 (tableau 26), pour 59 pensions d'ascendant en cours ?
**À qui :** DPC.
**En attendant :** sans effet par défaut : avec le filtre « assuré », les ascendants ne comptent pas.
**Pourquoi on la pose :** 54 pensions d'ascendant fin 2023, 7 nouvelles, 89 décès et 59 fin 2024 (tableaux 18, 23 et 26) : le chiffre est impossible s'il s'agit de titulaires d'une pension d'ascendant. Il pèse sur les 197 ayants droit décédés publiés.
**Déjà posée :** README (question T57) ; points_a_valider n° 57.

### Q16 — Où sont payés les soins des accidents du travail (139,7 M en 2024) et la part des rentes et IJ AT/MP absente des paiements de la base ?
**À qui :** DSI, avec la DFC.
**En attendant :** T56 AT/MP = paiements et remboursements de la base (0,76 Md pour 1,07 Md publiés en 2024).
**Pourquoi on la pose :** les remboursements de la base couvrent 13 à 23 % des soins publiés ; les rentes viagères restent 12 % sous le publié quelle que soit la date.
**Déjà posée :** README, questions à la DSI ; points_a_valider n° 51 ; fiche T50-T57 Q3 ; même question que T32-T41 Q21 et T42-T49 Q5, ici avec les soins.

### Q17 — Où sont payés les frais médicaux de maternité (25,0 millions FCFA en 2024) ?
**À qui :** DSI, avec la DFC et la DPC.
**En attendant :** absents de la ligne PF de T56.
**Pourquoi on la pose :** le recueil en fait une prestation de la branche (l. 127) ; votre tableau 40 les met dans les PF ; la base prévoit ces frais mais n'en paie aucun (remarque B27 du recueil).
**Déjà posée :** T25-T27 Q15 (la même réponse vaut pour les deux familles).

### Q18 — Pour la ligne « Autres » de T56 (l'action sanitaire et sociale, comme votre tableau 106), quel montant prendre ?
**À qui :** DPC, avec la DPASS et la DCG.
**En attendant :** ligne vide (`ligne_autres_t56`).
**Pourquoi on la pose :** pour 2024, votre annuaire donne 650,7 M (tableau 106) et 1 571,7 M de dépenses (tableau 114 et fiche signalétique). Troisième lecture, à vérifier : les dépenses moins les charges de fonctionnement de l'ASS (802,8 M en 2024), qui tombent juste en 2021 seulement (390 469 266). Le montant doit être le même que celui des indicateurs de l'ASS (DCG).
**Déjà posée :** points_a_valider n° 53 (le contenu, aujourd'hui tranché).

### Q19 — Les rappels de moins-perçus versés par ajustement sont-ils des prestations servies, à compter dans T56 ?
**À qui :** DCP, avec la DPC.
**En attendant :** non : les ajustements restent hors de T56, sauf les rachats de rente (N4).
**Pourquoi on la pose :** 408 M de rappels versés par ajustement en 2024 (lot 9) ; avec tous les ajustements, l'AVS passe de +1,3 % à +2,6 % face à l'annuaire.
**Déjà posée :** points_a_valider n° 52 ; README (question T56) ; fiche T50-T57 Q3 (les ajustements) ; même question que T28-T31 Q23, ici pour T56.

### Q20 — Le ratio des ressources de cotisations allouées aux prestations compte-t-il aussi les soins (prestations en nature), ou seulement les prestations en espèces ?
**À qui :** DAI, avec la DPC.
**En attendant :** soins compris, comme T56 (`nature_t56` = OUI) ; prestations datées à la notification, comme le dit le recueil (l. 1133).
**Pourquoi on la pose :** le recueil ne le dit pas ; le « rapport de charges brut » de l'Annuaire CIPRES (glossaire p. 65) ne prend que les prestations en espèces.
**Déjà posée :** nouvelle (la remarque B37 du recueil lisait déjà « ordonnancées = notifiées »).

### Q21 — T56 n'a aucune ligne dans le recueil : reste-t-il à produire ?
**À qui :** DPC (comité du projet).
**En attendant :** produit selon l'Annuaire CIPRES (requête 2).
**Pourquoi on la pose :** T26, T39, T40 et T56 n'ont aucun commentaire dans le recueil ; votre annuaire publie déjà l'équivalent de T56 (tableau 106).
**Déjà posée :** remarque B38 du recueil (voisine de T25-T27 Q12, pour T26).

### Q22 — L'épouse non salariée d'un assuré reçoit-elle l'allocation prénatale sous l'ancien code des prestations familiales (FAP) plutôt que sous celui de la maternité (MAP) ?
**À qui :** DCP.
**En attendant :** T55 compte les deux codes : la réponse ne change pas le tableau.
**Pourquoi on la pose :** en 2024, la MAP touche moins de personnes que l'IJ de maternité (3 857 contre 3 890) et 99,4 % des dossiers de maternité portent les deux (fait B22 du schéma des règles) ; la FAP touche 56 personnes. Savoir qui touche quoi aide à lire T55.
**Déjà posée :** nouvelle.

### Q23 — Peut-on retirer le nom, l'adresse et le téléphone de la vue `V_PRESTATAIRES_DECEDES_BI` ?
**À qui :** DSI.
**En attendant :** la BI ne lit pas cette vue : T57 part des tables `INDIVIDU`, `PRESTATION_ESP` et `DOSSIER`.
**Pourquoi on la pose :** la vue, faite pour la BI, contient des données personnelles inutiles ; elle prend aussi tous les décédés, sans les filtres de T57.
**Déjà posée :** points_a_valider n° 66 ; README, questions à la DSI.

### Q24 — Que doit compter l'indicateur « proportion de nouvelles prestations promues » : quelles prestations sont « nouvelles », et rapportées à quoi ?
**À qui :** DCP, avec la DAI et la DPC.
**En attendant :** l'indicateur n'est pas calculé.
**Pourquoi on la pose :** le recueil (l. 1340 à 1356) donne le nom de l'indicateur, sa fréquence (annuelle) et sa structure (DCP), mais pas de formule ; il écrit lui-même « A confirmer avec DAI et DPC ».
**Déjà posée :** nouvelle (le recueil la laisse ouverte).

### Q25 — Le « document de référence des tableaux de la CNSS » annoncé par le recueil existe-t-il, et pouvez-vous nous le donner ?
**À qui :** DPC (comité du projet).
**En attendant :** les sources restent le recueil v7, Kaya 2017, l'Annuaire CIPRES et l'annuaire CNSS 2024.
**Pourquoi on la pose :** le recueil le cite (l. 80 et 81) sans le nommer ni le joindre ; s'il définit des tableaux, il pourrait passer avant l'Annuaire CIPRES.
**Déjà posée :** nouvelle. Elle vaut pour toutes les familles.

---

## 7. Les points d'attention

1. **L'annuaire CNSS 2024 ne publie ni les effectifs de T51, T52, T53 et T55, ni les services passés, ni le sexe, ni les allocataires décédés, ni les décès sur cinq ans.** Ces chiffres ne se valident qu'avec la CNSS (fiche T50-T57).
2. **Deux règles de montant dans la même famille.** T50 à T53 et T56 : ce qui est payé dans l'année ; T54 et T55 : le montant liquidé (recueil). L'effectif (liquidé) et le montant (payé) d'une même ligne ne portent pas toujours sur les mêmes personnes : ne pas en tirer un montant moyen.
3. **Le réglage `date_montant` existe aussi dans les requêtes T31, T32-T41 et T42-T49.** Changer sa valeur change ces montants (1 à 2 % sur les grosses lignes) : décider une fois pour tout le domaine.
4. **Avec 'SAISIE', les montants deviennent plus irréguliers d'une année à l'autre** (lots de rappels) : T55 2021 = 78,2 M saisis pour 33,8 M au mois payé ; T52 2023 = 93,4 M pour 69,3 M. Prévoir une note sur ces années.
5. **L'entrepôt ne garde que le dernier cliché.** R13 impose de lancer le calcul de l'exercice N fin mars N+1 et d'en garder le résultat ; sinon les années passées dérivent (N7). Pas de refonte du modèle de l'entrepôt sans visa métier.
6. **T56 tiré de l'application ne retombera pas exactement sur l'annuaire :** la CNSS met ses chiffres en accord avec le compte de résultat. Le total des trois branches est à −0,2 % en 2024, mais parce que des écarts contraires se compensent : ne pas chercher l'égalité branche par branche.
7. **Les chiffres 2020 de l'annuaire semblent incomplets** (IJ de maternité : 1,53 Md publiés pour 2,19 Md payés), et les tableaux AT/MP ont des écarts d'arrondi de 1 F : ne valider aucune règle sur 2020 ni sur un écart de 1 F.
8. **Les maquettes.** T50 demande la moyenne des services passés sur la ligne TOTAL (vide dans la requête 1, N10) ; T51 à T53 n'ont pas de ligne TOTAL dans l'Annuaire CIPRES (la requête 1 en ajoute une) ; T54 et T55 n'affichent que la colonne FEMMES.
9. **La requête 3 mélange assurés et ayants droit :** l'AT/MP compte les rentes de survivants, l'AVS écarte les pensions de survivants. Avec le filtre du recueil, chaque décédé doit porter sa qualité (N2).
10. **Le TOTAL de T57 est un comptage de couples (personne, branche) :** une personne dans deux branches compte deux fois (Q11).
11. **La condition « en cours au décès » ne regarde que la radiation, pas la suspension ;** et l'annuaire compte plus de rentiers décédés que la base (24 pour 16). Mesurer avant de durcir ou d'assouplir (P117).
12. **L'écart des montants AT/MP en espèces s'est creusé depuis 2023** (paiements de la branche face au tableau 97 : −1,0 %, −7,9 %, −5,6 %, −15,5 %, −20,3 % de 2020 à 2024). À suivre avec la DSI.
13. **IJ de maternité 2022 :** 3 834 liquidées pour 3 973 publiées (−3,5 %), seule année hors de 1 % ; les IJ saisies (3 962) y retombent mieux. À regarder si la CNSS a une explication.
14. **La vue `V_PRESTATAIRES_DECEDES_BI` contient des données personnelles** (nom, adresse, téléphone) : ne pas la lire ni la recopier dans l'entrepôt.
15. **T51 garde les ascendants** (RASA, 18 en 2024) : le recueil les ajoute à l'Annuaire CIPRES, qui ne citait que veufs et orphelins.
16. **La maquette d'origine de T57.** Le recueil change le tableau CIPRES lui-même (« Autres » devient « TOTAL ») ; faut-il aussi garder la maquette d'origine pour la CIPRES ? C'est la question T25-T27 Q22, qui vaut aussi pour T57.
17. **La feuille 02 de la grille des ratios (« taux de redistribution ») n'a pas été lue** (consigne : feuilles 03 à 06 seulement). Elle porte peut-être la formule du ratio des ressources (N11) : à lire avec l'accord de l'utilisateur.
18. **Les documents existants, laissés tels quels, ont des erreurs à corriger le moment venu :**
    - le README (lot 8) et le lot 8 (P99) écrivent « 444 pour 447, six mois exacts » : il y en a cinq (janvier, avril, juin, août, novembre), les autres à 3 près ;
    - le README (R4), le TODO (tâche 18), le lot 8, la fiche T50-T57, l'en-tête de la requête et points_a_valider (n° 1) écrivent « 11 lignes sur 16 » : P98 compare 13 lignes, la saisie fait mieux sur 11, le mois payé sur 2 (la pension de vieillesse et, de peu, la rente viagère) ;
    - points_a_valider (n° 2) écrit que les décès « saisis avant avril 2025 » se lisent dans `IND_DATE_DECES` : ce champ est la date du décès ; P99 se sert de la dernière modification de la pension ;
    - le README et la requête 3 écrivent que `DOS_DECES_IMMEDIAT` est « rempli depuis 2024 » : il ne l'est que pour 2 % des accidents de 2024 ;
    - P98 compare la seule pension de vieillesse (RPV) à la ligne « PRV » de l'annuaire, alors que son en-tête dit « RPV et RPRA » ; avec la retraite anticipée : +1,0 % au mois payé et +1,9 % à la saisie. On ne sait pas si les 38,24 Md publiés la contiennent ;
    - la fiche de lecture (terme T56) laisse de côté les rachats de rente, que l'annuaire et Kaya 2017 font entrer dans T56 (N4).
19. **Deux familles de numéros en B, et une en N.** Les remarques B01 à B41 du recueil (`remarques_besoins_v7`) n'ont rien à voir avec les faits B1 à B25 du schéma `regles_prestations` : ce document écrit toujours « remarque Bnn du recueil » ou « fait Bnn du schéma des règles ». `reference_cipres_T25_T57.md` utilise N1 à N9 pour les constats du lot 1 : hors de ce document, on écrit « T50-T57 N3 ».

---

## 8. Comment c'est vérifié

- **Sources relues pour ce document :** `besoins_v7.txt`, l. 4 à 35, 72 à 81, 115 à 335, 355 à 470, 1085 à 1142, 1205 à 1264, 1314 à 1340 ; la formule du ratio des ressources dans le document Word (extraction `recueil_out/document_dump.txt`, l. 2041 à 2043, 2165, 2287) ; `kaya.txt`, l. 118 à 160, 164 à 176, 240 à 262, 368 à 398, 898 ; `annuaire_cipres.txt`, maquettes T50 à T57 (l. 2205 à 2350) et glossaire (décédé, prestation, rapport de charges, services passés) ; l'annuaire CNSS 2024, démarche (p. xiv) et fiche signalétique (p. xx) ; les en-têtes et la CTE `parametres` des trois parties de `t50_t57_requete_proposee.sql` ; les questions du README ; points_a_valider n° 1, 2, 6, 11, 16, 18, 49 à 57, 62, 66 ; la fiche T50-T57 (règles 1 à 8, Q1 à Q5).
- **Recalculs :** la moyenne pondérée des services passés de 2020 à 2024 (N10 : environ 90,8 mois) ; les décès de retraités de 2024 d'après les deux indices de P99 (444 et 404, cinq mois exacts) ; les victimes mortes dans l'année par année de l'accident (P100 : 20, 17, 24, 16, 9) et le remplissage de `DOS_DECES_IMMEDIAT` (25 accidents sur 1 192 en 2024, 759 sur 1 170 en 2025) ; les payées face aux liquidées de 2024 (P92) ; les écarts du tableau 106 en francs (AVS +607 808 484, PF −429 318 083, AT/MP −310 716 394) ; l'écart PF à la saisie (−378 127 725) ; l'IJ AT/MP au décaissement (−7,1 %) ; l'ASS hors charges de fonctionnement (802 758 809 en 2024 ; 390 469 266 en 2021). Les autres chiffres de l'annuaire et des lots viennent du dossier de la famille, recalculés par sa contre-vérification (sommes, totaux et écarts justes).
- **Lots utilisés :** 8 (P92 à P101), 9 (P104, P105), 5 (P78), 2 (P20, P30), 7 (P87) ; comparaison A02. Aucune requête nouvelle n'a été lancée ; aucune table DTM n'a été lue ; STG_GL n'a pas été lue ; le classeur des ratios n'a été lu que pour ses feuilles 03 à 06, sans aucune donnée personnelle.
- **Corrections reprises de la contre-vérification :**
  - *erreur* : R4 ne cite plus le recueil ni Kaya 2017 ; la note du recueil (« ordonnancées = notifiées ») date à la notification, proche du montant liquidé (N1, N11) ; la formule du ratio est bien dans le recueil (document Word) ; la remarque B37 est citée pour ce qu'elle dit.
  - *manque* : l'assuré mort en activité (statut « question » pour la ligne l. 172, cas 3 complété, cas 18, Q4, réglage `actifs_decedes`, P117) ; les allocations de vieillesse aux mois cotisés nombreux (cas 17, Q9, réglage `mois_cotises_max`) ; les frais médicaux de maternité (R1, Fiche 6, Q17) ; R1 cite d'abord le recueil (l. 1102 et l. 127), les modèles CNSS deviennent « cohérents » ; le ratio des ressources : glossaire p. 65, deux réglages, trimestre (N11, `nature_t56`, Q20) ; la troisième lecture du montant de l'ASS (Q18) ; T55 avec la ligne TOTAL et les liquidées de Kaya 2017 (partie 0, N10).
  - *imprécision* : R13 et le tableau 26 avec les deux indices (444 et 404) et le verdict « précise » au lieu de « confirme » (« indice fort ») ; R4 et les tableaux 28, 41, 90 : « la CNSS semble compter à la saisie », verdict « précise » au lieu de « tranche », écart de niveau laissé en clair ; « 11 lignes sur 13 » ; tableau 106 : « chaque écart a sa propre cause » ; tableau 40 : détail de l'écart PF, allocations familiales comprises ; tableau 97 : le décaissement des IJ (−7,1 %) et « des paiements manquent » réservé aux rentes et aux soins ; cas 7 : code à vérifier (Q22) ; T50 : « versée en une seule fois » (glossaire) séparé de l'explication BI ; R5 : tableau 20 pour l'orphelin, glossaire pour « une personne » ; D4 : une fois dans l'année, plusieurs années possibles ; R9 : 95,5 mois pour la part CNSS, 831 personnes ; Q8 : chiffres de chaque tableau ; `DOS_DECES_IMMEDIAT` : 2 % et 65 % ; N9 : comparaison par année de l'accident (+7, +6, +11, +5, −5) et piste « date du décès = date de l'accident » ; écart AT/MP « creusé depuis 2023 » ; N7 : « à vérifier dans l'annuaire 2020 ou 2023 » ; « tableaux 24 et 39 de l'annuaire CNSS 2024 » (et non T24, T39) ; renvois « remarque Bnn du recueil » et « fait Bnn du schéma des règles » ; N1 : 2 745 737 842 au mois payé et 2 749 474 700 à la saisie ; N6 : « répartition par branche à demander à la DFC », « devrait retomber » ; tableau 39 : « sans doute le mois de la liquidation, à vérifier » ; T53 : seuil de 15 % déduit, pas mesuré ; N8 : A02 « proche, sauf les veuves ».
  - *demandes du recueil oubliées*, toutes reprises en partie 1 : l. 127 (frais médicaux de maternité), l. 1102, l. 1137 à 1139, l. 194 (crédirentiers), l. 142 à 144 (noms des branches), l. 28, l. 1318 à 1337 (indicateurs de l'ASS, DCG).
  - *tableaux de l'annuaire oubliés*, repris en partie 4 : tableaux 29 à 33, 91 à 95, 83 à 87, 20, 78 et 80, ligne « Funéraires » des tableaux 82, 88 et 89, fiche signalétique (p. xx).
- **Points où ce document va plus loin que le dossier ou sa contre-vérification :**
  - **le verdict du tableau 34 (allocations de vieillesse, montant)** : le dossier disait « confirme ». La règle R4 en vigueur compte au mois payé ; ce chiffre appuie l'autre valeur ('SAISIE'). Il ne confirme donc pas notre règle : verdict « précise » ;
  - **la ligne « Autres » de T56** : le dossier gardait la question « est-ce l'ASS ? ». Le recueil se tait, et l'annuaire (tableau 106, fiche signalétique) tranche la pratique : seule reste la question du montant (Q18), et points_a_valider n° 53 est dit tranché ;
  - **les noms des branches** : la ligne l. 142 à 144 passe en « à intégrer », avec la proposition d'écrire « AVS (PVID) » et « AT/MP (RP) » ;
  - **le montant de T54 et T55** : le réglage `montant_t54_t55` retient 'LIQUIDE' par défaut, parce qu'un réglage suit le recueil ; le dossier gardait 'PAYE' ;
  - **deux réglages ajoutés** : `actifs_decedes` (Q4) et `mois_cotises_max` (Q9), pour que les deux nouvelles questions aient leur valeur par défaut ; et `nature_t56` (Q20) ;
  - **une règle ajoutée, N11** : le dossier rangeait le ratio des ressources parmi les demandes « à intégrer » sans règle ; la contre-vérification demandait d'en écrire les conditions.
- **Relecture (exactitude et clarté), 08/11/2026 :** toutes les lignes du recueil sur T50 à T57 ont été relevées à nouveau (grep dans `besoins_v7.txt`, demandes F5 et « transverse » du relevé, REPRISE parties 5 et 6, `recueil_oublie`) : rien ne manque ; les numéros de ligne sont justes. Les chiffres ont été recalculés depuis l'annuaire CNSS 2024 (tableaux 18, 20, 23, 24, 26, 34, 35, 36, 37, 39, 40, 49, 75, 78 à 81, 89, 90, 96, 97, 103 à 106, 114, fiche signalétique) et les lots 8 et 9 (P92 à P100, P104) : tous justes. Corrections :
  - *exactitude* : le repère « prestations ÷ cotisations » de N11 et de la partie 4 n'est pas publié par l'annuaire : il est recalculé (charges techniques ÷ produits techniques des tableaux 103 à 105) ; ajout du calcul avec les cotisations du tableau 17 (47,07 %, 26,42 %, 10,19 %). Verdict « précise » gardé : le chiffre reste un repère ;
  - *exactitude* : tableaux 82, 88, 89 : T56 AT/MP est 13 à 29 % sous le publié de 2020 à 2024 (24 à 29 % en 2023 et 2024), et non « 25 à 29 % » ;
  - *exactitude* : Q18 ne renvoie plus à la fiche T50-T57 Q3, qui porte sur les soins et les ajustements ; Q19 y renvoie (les ajustements) ;
  - *identifiants* : D4 garde le sens de T25-T27 (liquidé = notifié), dit dans son état ; la fiche T50-T57 Q4 et points_a_valider n° 54 sont dits tranchés par le recueil (N2) et retirés de Q10 ; la fiche Q5 est rattachée à R5 ; points_a_valider n° 55 est rattaché à R13 ;
  - *sources* : l. 461 ajoutée à la ligne T75 ; tableau 75 (44 nouvelles rentes viagères) cité pour le flux des rentes ; tableaux 18 et 23 cités dans Q15 ; la ligne du virement (l. 1085 à 1091) dit pourquoi elle ne change rien ici ;
  - *clarté* : « Les mots de ce document » explique aussi remboursement, ajustement, rappel de moins-perçu, rachat de rente et corrélation ; « pondérée » (N10) et « modèle DTM » (point 5) remplacés par des mots simples ; cas 13 au conditionnel ; « En bref » parle d'allocataires et de mères, pas de prestations. Le schéma reprend ces corrections.
- **Choix de forme :** la ligne « aucun tableau » de la partie 4 porte le verdict « pas publié » plutôt que « non comparable » (l'annuaire ne publie pas ces chiffres), comme dans le document T25-T27. Les règles 6 et 7 de la fiche sont appelées « Fiche 6 » et « Fiche 7 » ; les autres règles de la fiche gardent leur identifiant du README (R2 = règle 1, D4 = règle 2, R5 = règle 3, R4 = règle 4, R9 = règle 5, R13 = règle 8). La question « la ligne Autres est-elle l'ASS ? » (dossier) et la question du montant de l'ASS ont été fusionnées (Q18) ; la question « frais médicaux de maternité » renvoie à T25-T27 Q15.
- **Critique d'exhaustivité et de cohérence entre les cinq familles, 08/11/2026.** Le recueil v7 (l. 115 à 335 et indicateurs l. 1018 à 1378) a été relu à nouveau face aux cinq documents de famille, à la liste du recueil (`besoins_v7_prestations.md`) et au document sur l'annuaire (`annuaire_cnss_2024_prestations.md`). Changements ici : six lignes ajoutées à la partie 1, qui en compte maintenant 48 : satisfaction, normes des SMI, CQP, réclamations (hors base), nouvelles prestations et document de référence (questions nouvelles Q24 et Q25) ; aucune famille ne leur donnait de statut. La ligne l. 298 reprise pour T50 passe de « pris en compte » à « question (T42-T49 Q10) » : R9 retire `PE_COT_AUTRE`, dont le sens est encore cette question. Tableau 35 (effectifs des IJ de maternité) : « précise » au lieu de « confirme », comme la famille T25-T27 pour les mêmes chiffres ; D4, l'« En bref » et « En clair » suivent. Q1 prend la formulation de T28-T31 Q24 (le mois payé, la saisie ou le versement) ; Q1, Q2, Q16 et Q19 disent quelle question d'une autre famille elles reprennent. La ligne l. 166 cite Q3. R1 et A1, rappelés comme confirmés, renvoient aux cartes de T25-T27 et T28-T31.
- **Numéros des contrôles à écrire, 08/11/2026.** Trois familles réservaient P107 pour des contrôles différents. Les numéros sont maintenant distincts : P107 à P114 pour le lot 10 de T42-T49, contrôles (a) à (h) ; P115 à P119 pour T50-T57 (avant : P107 à P111) ; P120 et suivantes pour T28-T31 (avant : P107 et suivantes). Le document de l'annuaire suit (P115 à P117).

---

## 9. Ce qui reste à faire

| Tâche | Qui | État |
|---|---|---|
| Exécuter les trois parties de `t50_t57_requete_proposee.sql` sur le cliché de l'entrepôt, puis sur un cliché de fin mars N+1 quand il existera (R13), et comparer à l'annuaire CNSS 2024 | toi (exécution), puis Claude | à faire |
| Écrire P115 : montants de 2024 mois par mois, par branche et pour RPV, RAV, MIJ, AIJ, ARI, datés par l'approbation, la saisie et le décaissement (IJ AT/MP sur cinq ans), face aux tableaux 28, 41, 90 et 97 | Claude, puis toi | à faire |
| Écrire P116 : montant liquidé de 2020 à 2024 pour MIJ, MAP et FAP (et RAV, RASC, RASO, RASA, AIJ, AAI), face aux tableaux 35, 40, 34 et 97 | Claude, puis toi | à faire |
| Écrire P117 : T57 par qualité et par type, 2020 à 2024, avec les ayants droit pensionnés ou rentiers, les rentiers sans la condition « en cours », et les assurés morts en activité (titulaires de dossiers de survivants sans pension directe), face aux tableaux 26 et 81 | Claude, puis toi | à faire |
| Écrire P118 : le champ `DOS_DECES_IMMEDIAT` de `TSINISTRE` et les victimes mortes le jour de l'accident (date du décès = date de l'accident), par année d'accident, face au tableau 49 | Claude, puis toi | à faire |
| Écrire P119 : T56 AT/MP avec les rachats de rente (débours RR) de 2020 à 2024, face au tableau 97 | Claude, puis toi | à faire |
| Mesurer les allocations de vieillesse de 2020 à 2024 aux mois cotisés nombreux, et leur poids dans la moyenne de T50 (Q9) | Claude, puis toi | à faire |
| Comparer mois par mois et par DR les IJ de maternité liquidées (tableau 39), les allocations de vieillesse liquidées (tableau 24) et les montants des tableaux 29 à 33 et 91 à 95 | Claude, après exécution | à faire |
| Vérifier dans l'annuaire 2020 ou 2023 si les chiffres de 2020 ont été repris tels quels (N7) | toi (document), puis Claude | à faire |
| Demander à la DSI et à la DFC l'accès aux comptes 601, 602, 603 et 605 par branche (`V_BI_BALANCE_COMPTABLE`) et leur répartition par branche, pour tester T56 = charges techniques (N6) | toi | à faire |
| Poser les questions Q1 à Q25 (DPC, DCP, DSI, DFC, DAI, DPASS, DCG), en commençant par Q1 à Q5 | CNSS, via toi | à poser |
| Après validation, revoir la requête 3 (T57) : colonnes PF, AT/MP, AVS, TOTAL ; colonnes QUALITE et TYPE ; ayants droit ; réglages `qualite_deces`, `actifs_decedes`, `delai_sinistre_jours`, `marge_radiation_jours` | Claude | proposé, à faire après visa |
| Après validation, revoir la requête 2 (T56) : rachats, ligne « Autres », `nature_t56`, `source_t56` ; et la requête 1 : montant liquidé de T54 et T55, moyenne des services passés sur la ligne TOTAL, mois et DR gardés dans le détail (N8) | Claude | proposé, à faire après visa |
| Proposer, avec visa et sans refonte du modèle, un stockage des résultats de chaque exercice, pour figer les années passées et lancer le calcul fin mars N+1 (R13, N7) | Claude, puis accord de la DSI | à faire |
| Lire, avec l'accord de l'utilisateur, la feuille « 02_taux de redistribution » de la grille des ratios (N11) | Claude, sur accord | non commencé |
| Si l'utilisateur le décide : reporter ces règles et corrections dans le README, le TODO, la fiche T50-T57, la fiche de lecture, `regles_prestations` et points_a_valider (point d'attention 18) | Claude, sur demande | non commencé (rien n'a été modifié) |
