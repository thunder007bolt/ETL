# T32 à T41 : les bénéficiaires au 31 décembre (pensionnés, rentiers, allocataires) — les règles en clair

## En bref

- **Ce que comptent ces tableaux.** Les personnes qui touchent une prestation le 31 décembre, par tranche d'âge et par sexe, avec le montant payé dans l'année. T32 à T36 : les pensionnés (vieillesse, invalidité, veuves et veufs, orphelins, ascendants). T37 à T40 : les rentiers des accidents du travail (la victime, ses veuves et veufs, ses orphelins, ses ascendants). T41 : les allocataires des prestations familiales.
- **Ce que le recueil v7 demande de nouveau.** Sortir les tableaux à la fin de chaque mois et de chaque trimestre, pas seulement au 31/12 (N1). Dans T41, l'allocataire est le titulaire du dossier, compté une fois (N2). T35 perd sa tranche « 25 ans et plus ». On dit « rentiers » au lieu de « crédirentiers » (N9). Pour une pension de coordination, seul ce que paie la CNSS entre dans le montant. Le rapport démographique et le taux de remplacement portent sur les pensions directes (N7). Un module de l'application doit passer les invalides en vieillesse à l'âge de la retraite : c'est un travail de l'informatique, pas de la BI (N5). T39 et T40 n'ont aucune ligne dans le recueil.
- **Ce que l'annuaire CNSS 2024 confirme.** La façon de compter un bénéficiaire (R6). En 2024 : T32 31 093 pour 30 808 publiés (+0,9 %), T36 exact, T37 −1,5 %, T38 +1,1 %, T41 98 048 allocataires pour 97 378 (+0,7 %). Il tranche aussi trois choix : la retraite anticipée à part (N4), les dates de naissance « en 1900 » dans « 100 ans et plus » (R8), l'argent des accessoires (bonification pour enfant…) hors du montant de T32 (N13). Il confirme la limite des orphelins à moins de 25 ans, déjà demandée par le recueil (R6).
- **Ce qui reste en écart.** En 2024, il manque 1 079 veuves (−5,0 %) et 1 508 orphelins (−15,9 %). Les rentes d'orphelins d'accident sont à −6,4 %. Les montants des rentes d'accident sont de 12 à 25 % sous le publié, ceux des allocations familiales de 3 à 7 %. T41 dépasse l'annuaire de 5 à 11 % de 2020 à 2023, sans explication. T32 a plus de jeunes retraités et moins de retraités âgés que le tableau 19.
- **Les décisions attendues de la CNSS.** Q1 : quel montant (ce qui est payé aux personnes comptées, ou tout ce qui est payé dans l'année) ? Q2 : un paiement se range-t-il dans le mois qu'il paie, le mois où il est saisi, ou le mois où il est versé (déjà posée le 02/11) ? Q3 : l'annuaire est-il arrêté fin mars ? Q4 : pourquoi manque-t-il des veuves ? Q12 : jusqu'à quel âge une rente d'orphelin est-elle versée ? Aucune ne bloque : chaque réglage a une valeur par défaut.
- **Ce qui reste à faire.** Exécuter la requête t32_t41 (jamais exécutée) et les trois comparaisons avec l'annuaire déjà écrites (A06 à A08). Rédiger un lot de contrôle : âge des allocataires, montants mois par mois face à l'annuaire (pour trancher Q2), décès des veuves, enfants par âge. Ajouter les réglages (le bloc `parametres` en tête de la requête) après visa. Choisir avec Jules la table de l'entrepôt qui gardera ces tableaux, prêts à lire.

**Ce que couvre la famille.** Dix tableaux de l'Annuaire CIPRES, sur la même grille : à la fin de l'exercice, combien de personnes touchent une prestation, par tranche d'âge de 5 ans et par sexe, et combien elles ont reçu dans l'année. Ils servent à tracer la pyramide des âges, à suivre la pension moyenne d'une année à l'autre et à faire les études actuarielles. Les pensions de la branche vieillesse, invalidité et décès (T32 à T36), les rentes des accidents du travail et maladies professionnelles (T37 à T40), les allocations familiales (T41). S'y ajoutent deux indicateurs du recueil v7 qui en sortent : le rapport démographique et le taux de remplacement.

**État au 08/11/2026.** Tout est **proposé**, rien n'est encore validé par la CNSS. Aucune réunion n'a tranché un point de ces tableaux ; la question de la date des paiements (R4) a été posée au point du 02/11/2026, sans réponse à ce jour. La requête n'a jamais été exécutée : les chiffres cités viennent des lots 5 à 8, **vus au 02/02/2026** (date de la copie de la base).

**D'où viennent les règles.** Dans l'ordre de force : une décision de réunion (aucune ici), le recueil des besoins v7, l'atelier de Kaya (2017), l'Annuaire CIPRES. L'annuaire statistique de la CNSS 2024 sert à comparer les chiffres, et tranche la pratique quand les sources plus fortes se taisent. Les modèles de ratios de la CNSS (classeur « Evaluation des Ratios CIPRES 2025 », feuilles 03 à 06) ne visent pas ces tableaux.

**Ce qui existe déjà, laissé tel quel.** La requête [t32_t41_requete_proposee.sql](t32_t41_requete_proposee.sql) (31/10, revue le 01/11, pas exécutée) ; règles R2 à R6, R8 et R13 du [README](README.md) ; carte A1 (les accessoires) et hypothèses H8, H9 et H13 du schéma `regles_prestations` (hypothèses détaillées dans [reference_cipres_T25_T57.md](reference_cipres_T25_T57.md)) ; fiches de validation T32 (fiche pilote) et T41 (`excalidraw/schemas/prestations/tableaux cipres/`) ; termes T32 à T41 de la fiche de lecture ; [points_a_valider](points_a_valider.md) n° 1 à 3, 8 à 10, 12, 34 à 44 ; lots 5 et 6 ([lot5_beneficiaires_31_12.sql](lot5_beneficiaires_31_12.sql), [lot6_ecarts_t32_t41.sql](lot6_ecarts_t32_t41.sql)) ; comparaisons A01 et A02 (dossier `comparaison_annuaire_cnss_2024`). Ce document les reprend avec leurs identifiants, et **propose** les règles nouvelles N1 à N13 sans rien modifier.

**Le schéma associé :** `excalidraw/schemas/prestations/T32 T41 beneficiaires/regles_t32_t41_beneficiaires.excalidraw`, fait par `excalidraw/sources/prestations/famille_t32_t41_beneficiaires.py`. Il dit la même chose en plus court, avec les mêmes identifiants.

**Les mots de ce document.** Le *bénéficiaire au 31 décembre* : la personne dont la prestation est en cours ce jour-là (un *stock*, pas les entrées de l'année). *Pensionné* : il touche une pension. *Rentier* (le recueil remplace le vieux mot *crédirentier*) : il touche une rente d'accident du travail. *Allocataire* : il touche les allocations familiales pour ses enfants. *Ayant droit* : la personne qui a droit à la prestation (l'orphelin, pas le tuteur qui encaisse). *Titulaire du dossier* : la personne au nom de qui le dossier est ouvert, l'assuré ; pour une veuve, c'est souvent le mari décédé. *Pension directe* : la pension de l'assuré lui-même (vieillesse, retraite anticipée, invalidité) ; une *pension de survivant* va à sa veuve, à ses orphelins ou à ses ascendants (père, mère). *Coordination* : l'assuré a cotisé à la CNSS et dans une autre caisse (un autre pays, par exemple) ; chaque caisse paie sa part. *Radiée* : la prestation est arrêtée pour de bon ; *suspendue* : arrêtée pour un temps. Un *débours* : un paiement enregistré dans l'application ; *statut D*, payé par un titre de paiement ; *statut P*, payé sans titre. Le *mois payé* : le mois que couvre un paiement ; la *saisie* : le jour où il est enregistré (un rappel versé en février peut payer octobre). Le *cliché* : la copie de la base sur laquelle on calcule. Les *réglages* : les choix écrits dans le bloc `parametres` en tête de la requête (une « CTE » en langage SQL) ; on les change sans réécrire la requête. *Pnn*, *lot n* : le résultat numéroté d'une requête de contrôle déjà lancée sur la base ; *A01*, *A02* (lancées) et *A06* à *A08* (à lancer) : les requêtes de comparaison avec l'annuaire CNSS 2024. *Md* : milliard de FCFA ; *M* : million. *OBIEE* : l'outil où la CNSS lira les tableaux ; *DTM* : les tables de l'entrepôt qui gardent les résultats prêts à lire. Les sigles des directions sont expliqués en partie 6.

---

## Ce que disent les sources

| Source | Ce qu'elle dit pour ces tableaux |
|---|---|
| Recueil des besoins v7 (14/01/2026 et 20/01/2026) | T41 (l. 152 et 153) : « Allocataire : Titulaire du dossier (assuré) » ; par mois, trimestre et année. T32 à T38 : « Idem. Tab 41 » (l. 188, 196, 264, 271, 277, 284, 290). T32 (l. 263) : pour la coordination, seule la partie CNSS dans le montant. T33 (l. 270) : un module de l'application pour changer la pension d'invalidité en pension de vieillesse à l'âge de la retraite. T35 (l. 283) : supprimer la tranche « 25 ans et plus ». T38 (l. 194 et 195) : « rentiers » ; « le cumul sans préciser un exercice ». Indicateurs (l. 1247 à 1265) : rapport démographique et taux de remplacement, sur les pensions directes, par DR et par agence. Rien pour T39 et T40 |
| Réunions avec la CNSS | Aucune décision sur T32 à T41. La date des paiements (R4) a été posée au point du 02/11/2026 ; les questions des fiches T32 et T41 et de points_a_valider attendent leur réponse |
| Atelier de Kaya (2017) | Recommandation générale (l. 53) : « prévoir un tableau statistique sur les enfants des allocataires ». T41 (l. 120) : « c'est le dossier de l'allocataire qui est considéré et non le nombre de prestataires ». T32 (l. 292) : coordination, partie CNSS seulement. T33 (l. 300 à 302) : la pension d'invalidité n'est pas transformée en pension de retraite comme le veut la loi ; module à développer. T35 (l. 322) : supprimer « 25 ans et plus ». T37 et T38 (l. 185, 193) : une saisie correcte des données. Tableau 2 (l. 1000 à 1015) : rapport démographique et taux de remplacement, sans citer la retraite anticipée. Rien sur T39 et T40 |
| Annuaire CIPRES | Description p. 13 et 14, maquettes p. 37 à 41 : effectif et montant par tranche de 5 ans et par sexe, à la fin de l'exercice ; une grille par tableau. Glossaire p. 62 : l'âge est « la différence entre l'année de constatation et l'année de naissance » ; l'allocataire est l'« assuré ou bénéficiaire ayant droit aux prestations familiales du chef de son activité professionnelle ou de celle d'autrui ». Glossaire p. 65 : pensionnés, rente |
| Annuaire CNSS 2024 | Publie l'effectif de tous ces tableaux sauf l'âge des allocataires : tableaux 18 à 22 (pensions), 76, 78 à 80 (rentes), 36 à 38 (allocataires et enfants). Montants : tableaux 19 à 21, 28 à 34 (vieillesse, au mois et par DR), 36 et 40 à 46 (prestations familiales), 90 à 97 (rentes). Flux : tableaux 23, 25, 26, 75, 77, 81. Démarche (p. xiii et xiv) : base, rapports et collecte en direction, montants « conformés » au compte de résultat, publication en juin 2025 |
| Modèles de ratios de la CNSS (2025) | Feuille 05 (contrôle a posteriori, norme 5 % au moins) : dossiers liquidés ou en portefeuille, par branche ; le recueil fixe pourtant le dénominateur de T28 à T30 aux dossiers payés (l. 134, Prestation #2). Feuille 03 (délai de liquidation) : trois blocs, prestations familiales, risques professionnels, pensions, chacun avec une synthèse (en 45 jours au plus ou au-delà, délai moyen, écart type). Feuilles 04 et 06 : restes à traiter, indus. Aucune norme ne vise les bénéficiaires au 31/12 : aucun apport direct |
| Données de la base | Lots 1 (P16), 2 (P25, P26), 3 (P34, P46, P47), 5 (P74 à P78), 6 (P79 à P81), 7 (P82, P83, P87), 8 (P96 à P99) ; comparaisons A01 et A02 ; carte B9 de `regles_prestations` |

---

## 0. Les tableaux de la famille

| Tableau | Ce qu'il compte (Annuaire CIPRES) | Page | Ce qu'on a déjà |
|---|---|---|---|
| T32 | Les bénéficiaires d'une pension de vieillesse et les montants servis, à la fin de l'exercice, par tranche de 5 ans et par sexe. Lignes : moins de 55 ans, 55-59 … 95-99, 100 ans et plus, TOTAL ; colonnes Hommes, Femmes, Total, chacune avec Effectif et Montant. Pas de colonne de moyenne (elle se calcule : montant ÷ effectif), pas de ligne « âge inconnu » | p. 13 ; maquette p. 37 ; glossaire p. 62 et 65 | `t32_t41_requete_proposee.sql` (pas exécutée) ; fiche T32 (fiche pilote) ; lots 5 et 6 |
| T33 | La même chose pour les pensions d'invalidité. Lignes : moins de 25 ans, 25-29 … 55-59, 60 ans et +, TOTAL | p. 13 ; maquette p. 37 | même requête ; lot 5 |
| T34 | Les veuves et les veufs pensionnés (leur conjoint assuré est décédé). Lignes : moins de 15 ans, 15-19 … 95-99, 100 ans et plus, TOTAL | p. 13 ; maquette p. 38 ; glossaire p. 65 | même requête ; lots 5 et 6 |
| T35 | Les orphelins pensionnés de la branche PVID (pensions de vieillesse, d'invalidité et de décès). Lignes : moins de 5 ans, 5-9, 10-14, 15-19, 20-24, 25 ans et plus (supprimée par Kaya 2017 et le recueil v7), TOTAL | p. 14 ; maquette p. 38 ; glossaire p. 65 | même requête ; lot 5 |
| T36 | Les ascendants pensionnés (le père ou la mère d'un assuré décédé). Lignes : moins de 50 ans, 50-54 … 95-99, 100 ans et plus, TOTAL | p. 14 ; maquette p. 39 ; glossaire p. 65 | même requête ; lot 5 |
| T37 | Les victimes d'accident du travail ou de maladie professionnelle qui touchent une rente à vie (rente viagère). Lignes : moins de 20 ans, 20-24 … 95-99, 100 ans et plus, TOTAL | p. 14 ; maquette p. 39 ; glossaire p. 65 | même requête ; lot 5 |
| T38 | Les veuves et veufs d'une victime qui touchent une rente (« crédirentiers », à dire « rentiers »). Même grille que T37 | p. 14 ; maquette p. 40 | même requête ; lot 5 |
| T39 | Les orphelins d'une victime qui touchent une rente. Lignes : moins de 5 ans … 20-24, 25 ans et plus, TOTAL | p. 14 ; maquette p. 40 | même requête ; lot 5 |
| T40 | Le père ou la mère d'une victime décédée qui touche une rente. Lignes : moins de 50 ans, 50-54 … 75-79, 80 ans et +, TOTAL | p. 14 ; maquette p. 41 | même requête ; lot 5 |
| T41 | Les allocataires des prestations familiales et les montants servis, par tranche de 5 ans et par sexe. Lignes : moins de 20 ans, 20-24 … 75-79, 80 ans et plus, TOTAL | p. 14 ; maquette p. 41 ; glossaire p. 62 | même requête ; fiche T41 ; lot 6 (P79) |

Une seule requête calcule les dix tableaux. Plus bas, on l'appelle « la requête t32_t41 ». Elle lit les copies de la base dans l'entrepôt (schéma `DWH`) ; les lots ont été lancés sur la copie de l'application elle-même (schéma `USER_DWH`).

---

## 1. Ce que demande le recueil des besoins v7

Toutes les lignes du recueil qui touchent T32 à T41. Les numéros de ligne sont ceux de `besoins_v7.txt`. Les formules des indicateurs sont des équations Word, absentes du texte : elles ont été relues dans le document Word (extraction `style_grh/travail/scratchpad/recueil_out/document_dump.txt`), avec la note 2 sur les cotisants actifs.

| Réf. | Ce que dit le recueil | En clair | Où c'est pris en compte | Statut |
|---|---|---|---|---|
| Prestation #4 (T41), l. 152 | « Allocataire : Titulaire du dossier (assuré) » | Dans T41, on compte la personne au nom de qui est ouvert le dossier d'allocations familiales, l'assuré, une fois, à son âge et à son sexe : pas les enfants, pas la personne qui encaisse. Le glossaire CIPRES (p. 62) est plus large (« assuré ou bénéficiaire ayant droit … du chef de son activité professionnelle ou de celle d'autrui ») : le recueil, plus fort, le réduit au titulaire | N2 ; réglage `allocataire` = TITULAIRE. Aujourd'hui la requête compte des dossiers (−6 allocataires en 2024, mesuré sans la condition de paiement, à remesurer). Titulaire décédé : Q8 | à intégrer |
| Prestation #4 (T41), l. 153 | « Périodicité : Mensuelle, Trimestrielle, Annuelle » | T41 se compte aussi à la fin de chaque mois et de chaque trimestre | N1 ; la condition de paiement suit la date (N10) ; réglages `periodicite`, `t41_paiement` | à intégrer |
| Prestation #9 (T37), l. 188 | « Idem. Tab 41 » | T37 reprend la périodicité de T41. Il reprend aussi la définition de l'allocataire : pour une rente viagère, le titulaire du dossier est la victime elle-même, donc cela ne change rien (à vérifier) | N1 ; R5 | à intégrer |
| Prestation #10 (T38), l. 194 | « Crédirentiers : rentiers » | Dire « rentiers » dans le titre et les libellés | N9 ; T39 et T40 par cohérence (Q13) | à intégrer |
| Prestation #10 (T38), l. 195 | « Le cumul sans préciser un exercice » | Lecture BI : tous les rentiers veufs et veuves en cours à la date choisie, quelle que soit l'année où leur rente a commencé (un stock). Autre lecture : le montant cumulé depuis le début de chaque rente. Pour T45, juste après (l. 203), le recueil demande au contraire « la période (début, fin) en paramètre » : cela appuie la lecture « stock » | R6 (stock à la date d'arrêté), N1 ; remarque B28 du recueil | question (Q14) |
| Prestation #10 (T38), l. 196 | « Idem. Tab 41 » | Périodicité de T41. La définition de l'allocataire ne s'applique pas : le titulaire du dossier est la victime décédée, et l'on compte le veuf ou la veuve | N1 ; R5 (lecture BI) | à intégrer |
| Prestation #20 (T32), l. 263 | « Pour les pensions de coordination, les montants concernent uniquement la partie CNSS » | Un assuré qui a cotisé dans deux caisses reçoit une pension de chacune. Dans T32, on ne met que ce que verse la CNSS. Kaya 2017 le disait déjà (l. 292) | R3 : le montant ne prend que les paiements de la CNSS. Une pension payée par l'autre caisse (catégorie `COOR_AC` du dossier) reste dans l'effectif, sans montant (P78 : 2 901 pensions de vieillesse dans ce cas fin 2025). Reste la moyenne (Q16) | pris en compte |
| Prestation #20 (T32), l. 264 | « Idem. Tab 41 » | Périodicité de T41. Le titulaire du dossier est le retraité : la définition ne change rien (à vérifier) | N1 | à intégrer |
| Prestation #21 (T33), l. 270 | « Les informaticiens doivent développer un nouveau module pour la gestion des pensions d'invalidité qui sont convertis en pension normale dès que l'assuré atteint l'âge de départ à la retraite » | Un travail sur l'application, pas sur la BI. Tant que le module manque, un invalide reste invalide toute sa vie dans l'application | N5 : T33 compte les pensions d'invalidité telles qu'elles sont, comme l'annuaire CNSS (tableau 21). Q15 | hors BI |
| Prestation #21 (T33), l. 271 | « Idem. Tab 41 » | Périodicité de T41. Le titulaire est l'invalide lui-même | N1 | à intégrer |
| Prestation #22 (T34), l. 277 | « Idem. Tab 41 » | Périodicité de T41. La définition de l'allocataire ne s'applique pas : on compte la veuve ou le veuf, pas le titulaire du dossier (l'assuré décédé) | N1 ; R5 (lecture BI) | à intégrer |
| Prestation #23 (T35), l. 283 | « Supprimer la tranche d'âge « 25 ans et plus » » | Un orphelin n'est compté que jusqu'à 24 ans : la dernière ligne de T35 devient « 20-24 ans ». Kaya 2017 le demandait déjà (l. 322) | R6 : seuls les orphelins de moins de 25 ans, même si leur pension n'est pas radiée ; réglage `age_max_orphelin`. L'annuaire CNSS fait pareil (tableau 20) | pris en compte |
| Prestation #23 (T35), l. 284 | « Idem. Tab 41 » | Périodicité de T41. On compte l'orphelin, pas le tuteur qui reçoit le paiement | N1 ; R5 | à intégrer |
| Prestation #24 (T36), l. 290 | « Idem. Tab 41 » | Périodicité de T41. On compte l'ascendant, pas l'assuré décédé | N1 ; R5 | à intégrer |
| #9, #10, #20 à #24 (T32 à T38), l. 188, 196, 264, 271, 277, 284, 290 ; renvoi à #4 (T41), l. 152 | « Idem. Tab 41 » : « Allocataire : Titulaire du dossier (assuré) » | Pour T34, T35, T36 et T38, lue à la lettre, la phrase ferait compter l'assuré décédé (le titulaire du dossier) à la place de sa veuve, de son orphelin ou de son parent, avec l'âge et le sexe du défunt. Pour T32, T33 et T37, le titulaire est le bénéficiaire lui-même : rien ne change | R5 : on compte l'ayant droit (lecture BI, point d'attention 1). La question est posée une seule fois pour T32 à T49 : T42-T49 Q1 | question (T42-T49 Q1) |
| Liste du domaine Prestation, l. 115 à 335 | Aucune ligne pour les tableaux 39 et 40 | La CNSS n'a rien dit sur T39 (rentes d'orphelins) ni sur T40 (rentes d'ascendants) ; l'atelier de Kaya ne les avait pas examinés non plus | La BI leur applique les règles de T38 : « rentiers », sortie au mois, au trimestre, à l'année (N1, N9) ; remarque B38 du recueil | question (Q13) |
| Indicateur « rapport démographique », l. 1247 à 1254 (NB l. 1251) | Rapport = cotisants actifs ÷ retraités ; « le nombre de retraités est le nombre de bénéficiaires de pensions directes (Pension vieillesse, Pension anticipé, pension d'invalidité) » ; note 2 du document Word : les cotisants actifs sont les « dossiers actifs » ; annuel ; par DR et par agence ; aucune structure responsable | Le nombre de cotisants pour un retraité. Les retraités sont ceux de T32 (anticipée comprise) et de T33, chacun une fois. La l. 1252 (« le dernier revenu d'activité … ») est mal placée : elle va avec le taux de remplacement | N7, N3 ; les cotisants viennent du recouvrement. Coordination payée par l'autre caisse : Q17 | à intégrer |
| Indicateur « taux de remplacement », l. 1256 à 1265 | Taux = montant de la pension ÷ dernier revenu d'activité × 100 ; « le dernier revenu d'activité est le salaire moyen des cinq meilleures années » (l. 1260) ; pensions directes (l. 1262) ; annuel ; par DR et par agence | La part du salaire que la pension remplace, pour les pensions directes | Pas encore calculé. La base garde un salaire moyen avec chaque prestation (`PRESTATION_ESP.PE_SAL_MOYEN`), jamais profilé (Q19). Toutes les pensions en cours ou les nouvelles de l'année (Q18) | question (Q18, Q19) |
| Prestation #3 (T31), l. 142 à 144 | « PVID : … (Branche pension) ; AT/MP : … (RP) ; PF : Prestation familiale » | Les trois branches : T32 à T36 sont la PVID (pensions de vieillesse, d'invalidité et de décès), T37 à T40 les risques professionnels, T41 les prestations familiales | R1 du README (les branches) ; R2 | pris en compte |
| Termes de référence, l. 4 à 10 | « cinq (5) Directions régionales : Ouagadougou, Bobo Dioulasso, Ouahigouya, Fada N'gourma, Dédougou » | « Par DR » veut dire ces cinq directions. Ouahigouya est la « direction régionale du Nord » (DRN) de l'annuaire CNSS | N3 | à intégrer |
| Indicateurs, l. 1065 | « De manière globale, donner la possibilité de désagréger l'indicateur » | Chaque tableau et chaque indicateur se découpe : DR, agence, type de prestation, sexe, âge | N3, N4. Type, sexe et âge sont déjà dans la requête ; DR et agence manquent | à intégrer |
| Attentes, l. 29 ; caractéristiques techniques, l. 51 et 68 | Données « traquées jusqu'au niveau de granularité le plus élémentaires » ; analyses « y compris au niveau transactionnel » ; « sur des axes non préalablement définis » | Chaque chiffre doit se déplier jusqu'à la liste des bénéficiaires. Le passage de la l. 29 vient d'un cahier des charges bancaire (remarque B41 du recueil) : on s'appuie surtout sur les l. 51 et 68 | N12 | à intégrer |
| Caractéristiques techniques, l. 72 | « Permettre le contrôle par profil d'utilisateurs des conditions d'accès aux données (règles de confidentialité) » | La liste des bénéficiaires est une donnée personnelle : qui peut la voir dépend de son profil | N12 : la liste n'a ni nom ni numéro d'assuré ; son accès suit le profil | à intégrer |
| Attentes et caractéristiques, l. 31 et 74 | Présentation multidimensionnelle ; une dimension ajoutée après coup vaut pour tous les indicateurs | Les mêmes axes pour les dix tableaux et leurs indicateurs : tableau, code de prestation, tranche, sexe, DR, agence, date d'arrêté | N3, N4, N12 | à intégrer |
| Attentes, l. 35 | « Production des rapports et des états de restitution à des fréquences adaptées » | Chaque tableau à sa fréquence | N1 | à intégrer |
| Attentes et caractéristiques, l. 28, 34, 75 | Peu d'intervention manuelle ; restitutions automatisées ; rafraîchissement automatique | Les chiffres sortent d'une requête, sans saisie à la main | La requête t32_t41 | pris en compte |
| Caractéristiques techniques, l. 58 | Règles de transcodage (passage d'un code à un libellé) écrites et rejouables | La correspondance entre codes de prestation et tableaux, et les bornes des tranches, sont écrites | R2, R8 ; listes `perimetre` et `grille` de la requête | pris en compte |
| Attentes et caractéristiques, l. 24 et 78 | Profilage de la qualité des données | Mesurer les trous de la base | Lots 5 et 6 ; dates de naissance par défaut, pensions d'orphelins non radiées, pensions actives de titulaires décédés (partie 7, point 9) | pris en compte |
| Général, l. 15 et 92 à 94 | La source : l'application de gestion des prestations | Les tableaux se calculent sur la copie de cette application | Toutes les règles ; copie de la base au 02/02/2026 | pris en compte |
| Indicateur 2, l. 1085 à 1091 | « La proportion des prestations payées par virement bancaire » : payées par virement ÷ payées × 100 ; annuel, trimestriel, mensuel ; par type, DR, agence et mode de paiement ; DES, DCP, DR | La part des paiements faits par virement. Il se calcule sur les mêmes paiements que les montants de T32 à T41 | Hors de cette famille : mêmes paiements que R3 ; dans la base, le virement est un type d'effet de paiement (code BVB), mais 62 % des paiements de prestations familiales de 2025 n'ont pas d'effet (README, constat N2) | à intégrer |
| Ratio des ressources, l. 1129 à 1141 (note l. 1133) ; mêmes ratios l. 1210 à 1214 et 1287 à 1294 | « Le montant total des prestations ordonnancées au titre de la période est l'ensemble des prestations ayant fait l'objet d'une notification » | La seule définition d'un montant de prestations dans le recueil. Elle vise un autre indicateur : T32 à T41 publient le montant « servi » (Annuaire CIPRES p. 13 et 14) | Versée aux questions du montant (Q1) et de sa date (Q2) ; ne les tranche pas | pris en compte |

**Quels tableaux n'ont aucune ligne.** T39 (rentes d'orphelins) et T40 (rentes d'ascendants) n'ont aucune ligne dans le recueil, ni dans le tableau 1 de Kaya 2017 (remarque B38 du recueil). Pour eux, seule la maquette de l'Annuaire CIPRES fait foi ; la BI leur applique les règles de T38 (question Q13).

**Les lignes générales qui ne changent rien ici :** l. 32 (alertes : aucune norme CIPRES ne vise les bénéficiaires au 31/12), l. 53 à 55 et 60 (fichiers Excel), l. 80 et 81 (document de référence, ni nommé ni joint : rangé dans la famille T50-T57, question T50-T57 Q25), indicateur de satisfaction (l. 1075 à 1084 : rangé dans la famille T50-T57, hors base), « pourcentage des ratios CIPRES respectés » (l. 1978 à 1989 : aucun ratio ici), colonnes « Priorité » vides (l. 120 et 121). La l. 298 (coordination dans T42) et la l. 202 (« rentiers » pour T45) relèvent de la famille T42 à T49 ; N9 cite la seconde.

---

## 2. Les règles, en clair

### R2 — Les tableaux et leurs codes de prestation

**Règle.** Chaque tableau compte une famille de prestations, reconnue par son code dans l'application : T32 pension de vieillesse (RPV) et retraite anticipée (RPRA, sur une ligne à part, voir N4) ; T33 invalidité (RPI) ; T34 veuve (RPSV) et veuf (RPSF) ; T35 orphelin (RPSE) ; T36 ascendant (RPA) ; T37 rente viagère de la victime (ARI) ; T38 rente de veuve ou de veuf (ARSV, ARSF) ; T39 rente d'orphelin (ARSE) ; T40 rente d'ascendant (ARSA) ; T41 allocations familiales (FAF).

**Exemple.** Une pension de veuve (RPSV) va dans T34 ; une rente de veuve d'accident du travail (ARSV) va dans T38. Un accidenté à moins de 15 % d'incapacité reçoit une allocation d'incapacité (AAI), pas une rente : il n'est pas dans T37.

**Sources.** Annuaire CIPRES p. 13 et 14, glossaire p. 65 ; Recueil v7, l. 142 à 144 (les trois branches) ; Annuaire CNSS 2024, tableaux 18 et 79 (une ligne par code ; nos stocks y retombent, P74) ; README, correspondance des codes ; Données P34 (libellés), P87 (rentes de 15 à 100 %).

**État.** Confirmée par l'annuaire CNSS 2024. La liste des codes reste à faire viser (points_a_valider n° 18).

### R5 — La personne comptée

**Règle.** On compte la personne qui a droit à la prestation (l'ayant droit, `PRESTATION_ESP.IND_ID_PREST`), une seule fois par tableau, même si elle a deux prestations du même tableau. Pour un orphelin, c'est l'orphelin, pas le tuteur qui reçoit l'argent. Une personne peut être dans deux tableaux : on n'additionne jamais deux tableaux pour compter des personnes.

**Exemple.** Une veuve qui a aussi sa propre retraite compte dans T34 et dans T32. Un retraité dont la pension a été révisée, et qui a deux pensions en cours fin 2024, compte une fois dans T32.

**Sources.** README, R5 ; Annuaire CNSS 2024, tableau 20 (orphelins comptés à leur âge, de 0 à 24 ans) ; Proposition BI (lecture du « Idem. Tab 41 » du recueil, l. 196, 277, 284, 290 : la définition de l'allocataire ne vaut pas pour un survivant) ; Données P74 (fin 2024 : 31 267 pensions de T32 en cours pour 31 093 personnes ; compter les personnes retombe mieux sur les 30 808 publiés), P77 (519 retraités aussi dans un autre tableau fin 2025), P16 (l'orphelin n'est jamais la personne payée).

**État.** Confirmée par l'annuaire CNSS 2024 ; à valider (points_a_valider n° 8). En 2024, 174 personnes ont deux pensions de T32 en cours (sans doute des révisions saisies en 2025 avec un effet en 2024) ; aussi 25 en T35, 15 en T37 et 203 enfants en T41.

### R6 — Le bénéficiaire au 31 décembre (pensions et rentes)

**Règle.** Est compté celui dont la prestation est en cours le 31 décembre : elle a commencé avant, elle n'est ni radiée ni suspendue (sauf reprise avant cette date), et la personne est en vie. Qu'elle ait été payée en fin d'année ou non. Orphelins (T35, T39) : seulement ceux de moins de 25 ans (année du calcul moins année de naissance), même si leur pension n'est pas radiée.

**Exemple.** Une pension en cours mais plus payée depuis octobre (certificat de vie attendu) est comptée. Une pension suspendue le 31 octobre et pas reprise ne l'est pas.

**Sources.** Annuaire CIPRES p. 13 et 14 (fin d'exercice) ; Recueil v7, l. 283 et Kaya 2017, l. 322 (T35) ; Annuaire CNSS 2024, tableaux 18, 20 et 76 à 80 ; README, R6 ; Données P74 (2024 : T32 31 093 pour 30 808, +0,9 % ; T33 +5,0 % ; T36 exact ; T37 −1,5 % ; T38 +1,1 % ; T40 +3,4 %), P81 (253 pensions de vieillesse suspendues fin 2024, non comptées).

**État.** Confirmée par l'annuaire CNSS 2024 ; à valider (points_a_valider n° 3). En exigeant un paiement dans l'année, on n'aurait que 27 670 retraités en 2024 : −10,2 % face à l'annuaire (−11 % face à notre stock). L'écart de 2024 (+285) vient sans doute de pensions créées ou révisées après la publication avec un effet en 2024 : à vérifier par A06. Les veuves et les orphelins restent sous le publié depuis 2023 (Q4, partie 4). La limite de 25 ans pour T39 est une question (Q12).

### R6 (T41) — L'enfant et l'allocataire de T41

**Règle.** Un enfant compte s'il a droit le 31 décembre (allocation en cours, 21 ans au plus, en vie) et s'il a été payé dans l'année. L'allocataire est compté s'il a au moins un enfant compté. T41 s'écarte ainsi des pensions : un enfant sans certificat n'est pas radié, il n'est plus payé. Seules les allocations familiales (FAF) comptent : une future mère qui ne touche que l'allocation prénatale n'est pas une allocataire.

**Exemple.** Un dossier dont le seul enfant n'a rien touché en 2024 n'est pas compté.

**Sources.** README, R5 et R6 ; Kaya 2017, l. 120 ; Annuaire CNSS 2024, tableaux 36 (dernière tranche « 20-21 ans »), 37 et 38 ; Données P79 (2024 : 98 048 allocataires pour 97 378, +0,7 % ; 271 577 enfants pour 268 481, +1,2 % ; sans la condition de paiement, +5,4 % et +5,6 %).

**État.** Confirmée par l'annuaire CNSS 2024 pour 2024 seulement : de 2020 à 2023, nos allocataires dépassent le publié de 5,3 % à 11,3 %, sans explication (P79 écarte les allocations saisies après coup). La condition de paiement reste une question (Q5 ; points_a_valider n° 38). La limite de 21 ans est tranchée par l'annuaire (tableau 36). L'allocation prénatale seule est tranchée par l'annuaire : avec les seules allocations familiales, on retrouve ses allocataires à 0,7 % près (points_a_valider n° 41 et fiche T41 Q4 peuvent être clos).

### N2 — L'allocataire est le titulaire du dossier

**Règle.** L'allocataire est le titulaire du dossier d'allocations familiales (`DOSSIER.IND_ID`), l'assuré. Son âge et son sexe sont ceux de T41. S'il a deux dossiers, il compte une fois.

**Exemple.** Un assuré avec deux dossiers d'allocations familiales compte pour un allocataire.

**Sources.** Recueil v7, l. 152 ; Kaya 2017, l. 120 (le dossier de l'allocataire) ; Annuaire CIPRES, glossaire p. 62 (plus large : « assuré ou bénéficiaire ayant droit aux prestations familiales du chef de son activité professionnelle ou de celle d'autrui ») ; Données P74 (102 637 titulaires pour 102 643 dossiers en 2024, sans la condition de paiement).

**État.** Nouvelle ; tranchée par le recueil v7 (points_a_valider n° 39 et fiche T41 Q2 peuvent être clos) ; à intégrer (réglage `allocataire`). L'écart de −6 est mesuré sans la condition de paiement : à remesurer sur les 98 048 allocataires. Le cas du titulaire décédé reste une question (Q8).

### R8 — L'âge, les tranches et le sexe

**Règle.** L'âge est l'année du calcul moins l'année de naissance, sans regarder le jour d'anniversaire. Chaque tableau a la grille de sa maquette CIPRES. Sexe : 1 hommes, 2 femmes. Proposition : une personne de 120 ans et plus (date de naissance par défaut, année 1900) va dans la dernière tranche du tableau, « 100 ans et plus », comme dans l'annuaire CNSS ; un contrôle à part compte ces dates. Aujourd'hui, la requête la met dans une ligne « âge inconnu ».

**Exemple.** Né en 1960 : 64 ans dans T32 de 2024, tranche 60-64 ans. Une veuve « née en 1900 » : 124 ans, ligne « 100 ans et plus » de T34.

**Sources.** Annuaire CIPRES, glossaire p. 62 (âge), maquettes p. 37 à 41 (aucune ligne « âge inconnu ») ; README, R8 ; Annuaire CNSS 2024, tableaux 19 et 21 (mêmes grilles que T32 et T33), 20 (679 femmes de 100 ans et plus, impossible sans les veuves « nées en 1900 »), 76 ; Données P75, P76, P80 (503 personnes nées « en 1900 » en 2024, dont 499 veuves), P83.

**État.** Grilles et code du sexe confirmés par l'annuaire CNSS 2024. La ligne des dates par défaut est tranchée par l'annuaire (tableau 20) et la maquette CIPRES : réglage `ages_impossibles` (points_a_valider n° 9 peut être clos). La date à laquelle la CNSS calcule l'âge reste à vérifier : T32 a plus de jeunes et moins d'âgés que le tableau 19, et le lot 7 (P83) a vu le même décalage chez les nouveaux retraités (Q10).

### R3 — Le montant

**Règle.** Le montant est ce que la CNSS a payé dans l'année : les paiements faits (débours de paiement au statut D ou P), jamais le net à payer d'un titre. Par défaut, on prend les paiements des bénéficiaires comptés : c'est le seul montant qui se range dans une tranche d'âge et un sexe. Autre valeur : tout le payé de l'année ; l'argent versé aux personnes sorties dans l'année va alors sur une ligne à part « sortis dans l'année », et la moyenne se calcule sans elle. Pour une pension de coordination, seule la part que paie la CNSS compte ; une pension payée par l'autre caisse est comptée sans montant. Le montant moyen est le montant divisé par l'effectif.

**Exemple.** Un retraité payé 12 fois 100 000 FCFA : 1 200 000 FCFA dans T32. Une pension de coordination payée par l'autre caisse : comptée, 0 FCFA.

**Sources.** Annuaire CIPRES p. 13 et 14 (« montants servis ») ; Recueil v7, l. 263 ; Kaya 2017, l. 292 ; README, R3 ; fiche T32, règles 7 et 8 ; Données P75 (T32 2024 : 37,67 Md aux comptés, −1,5 % ; 38,63 Md en tout, +1,0 % ; 38,24 Md publiés ; 0,96 Md versés à 1 105 pensionnés sortis dans l'année), P78 (2 901 pensions de coordination payées ailleurs, aucun paiement CNSS, fin 2025).

**État.** Proposée. L'annuaire CNSS 2024 ne tranche pas : le chiffre publié tombe entre nos deux montants pour la vieillesse et l'invalidité ; il est sous nos deux montants pour les survivants (nous sommes à +1,6 % et +2,5 %) et au-dessus pour les allocations familiales (nous sommes à −6,7 % et −3,4 %). Question Q1, réglage `montant`.

### A1 — Les accessoires de pension

**Règle (sens de la carte A1, gardé).** La bonification pour enfant à charge (RBEC), les majorations pour enfant (RME, RME-COL) et la contribution aux charges du ménage (RCCM) s'ajoutent à une pension. Elles ne font pas un pensionné de plus. Ce qu'on fait de leur montant dans T32 est une règle à part : N13, juste en dessous.

**Exemple.** Un retraité qui touche sa pension et une bonification pour enfant compte une fois dans T32, pas deux.

**Sources.** Carte A1 de `regles_prestations` ; README, correspondance des codes (accessoires) ; Annuaire CNSS 2024, tableau 18 (seuls les 7 types de pension font l'effectif) ; Données P34 (libellés des codes).

**État.** Confirmée par l'annuaire CNSS 2024. Le remboursement de cotisations (RRC), rangé parmi les accessoires par le README, n'en est sans doute pas un (à vérifier) : l'annuaire range les remboursements parmi les allocations (tableau 34), et la base n'a aucun paiement RRC en 2024 (P98).

### N13 — Le montant de T32 sans les accessoires

**Règle.** Le montant de T32 ne prend que la pension elle-même (RPV, et RPRA sur sa ligne à part). L'argent des accessoires (bonification, majorations, charges du ménage) n'y entre pas ; il peut se montrer sur une ligne à part, comme la « majoration pour enfant » de l'annuaire.

**Exemple.** Un retraité touche 100 000 FCFA de pension et 5 000 FCFA de bonification pour enfant par mois : 1 200 000 FCFA dans le montant de T32 pour l'année, pas 1 260 000.

**Sources.** Annuaire CNSS 2024, tableau 34 (la pension de retraite, 38 235 880 033 FCFA, est exactement le montant du tableau 19 ; la majoration pour enfant, 537 343 000, est à part) et tableau 28 (même découpage, mois par mois) ; Données P98 (bonification de 2024 : 539 376 000 à la saisie, +0,4 % ; 554 254 000 au mois payé, +3,1 %).

**État.** Nouvelle ; tranchée par l'annuaire CNSS 2024 (le recueil, Kaya 2017 et l'Annuaire CIPRES n'en disent rien). **Elle va contre ce qu'écrivaient les documents existants :** la carte A1 de `regles_prestations` (« elles suivent la ligne de la pension »), le README (« montant à rattacher à la pension ? ») et points_a_valider n° 12. Ces documents sont laissés tels quels (partie 7, point 17).

### R4 — L'année d'un paiement

**Règle.** Un paiement compte pour l'année du mois qu'il paie (réglage `date_montant` = MOIS). L'autre lecture : l'année où il est saisi, rappels compris (SAISIE).

**Exemple.** Un rappel versé en février 2025 pour octobre à décembre 2024 compte en 2024 avec MOIS, en 2025 avec SAISIE.

**Sources.** README, R4 ; points_a_valider n° 1 ; Annuaire CNSS 2024, démarche p. xiii et xiv (montants conformés au compte de résultat) et tableaux 28 à 33, 40 à 46, 90 à 95 (montants mois par mois) ; Données P98 (face à l'annuaire 2024 : vieillesse +0,7 % au mois payé, +1,5 % à la saisie ; invalidité +2,2 % et −0,4 % ; survivants +2,5 % et +2,0 % ; allocations familiales −3,4 % et −2,9 % ; rentes viagères −12,2 % et −12,6 % ; rentes de survivants −25,1 % et −12,5 %).

**État.** En attente de la CNSS (question posée au point du 02/11/2026), non bloquante ; question Q2. Le choix change les montants de −3,5 % (retraite anticipée) à +29,4 % (rentes d'orphelins) selon le tableau (P98). Il change aussi l'effectif de T41, car la condition de paiement suit la même date : jamais mesuré. Les montants mois par mois de l'annuaire peuvent trancher sans attendre (partie 4).

### R13 — La date du calcul

**Règle.** L'année N se calcule sur la copie de la base de fin mars N+1, date à laquelle la CNSS arrête son annuaire. Plus tard, la base a changé : décès et radiations saisis après, pensions créées ou révisées avec un effet passé.

**Exemple.** Une veuve décédée en novembre 2024, décès saisi en juin 2025 : comptée au 31/12/2024 sur la copie de fin mars 2025, comme dans l'annuaire.

**Sources.** README, R13 ; Annuaire CNSS 2024, démarche (publication en juin 2025, après un atelier), tableaux 20, 22 et 26 ; Données P99 (444 décès de retraités de 2024 dont la pension n'a plus été modifiée après mars 2025, pour 447 publiés : c'est un indice, car la base n'a pas de date de déclaration du décès, P96 ; 404 avec la date de modification de la fiche de la personne ; 619 dans la base d'aujourd'hui).

**État.** Proposée ; appuyée par l'annuaire CNSS 2024 ; à valider (points_a_valider n° 2, question Q3). Aujourd'hui, le seul cliché disponible porte la copie du 02/02/2026 : pour 2024, dix mois trop tard.

### N1 — La date d'arrêté : fin de mois, de trimestre ou d'année

**Règle.** Le stock se compte à la fin de chaque période demandée : fin de mois, fin de trimestre ou 31 décembre. Par défaut, le montant est celui payé pendant la période elle-même (le mois, ou le trimestre) ; le montant de l'année s'obtient en additionnant les mois. Autre valeur : le cumul depuis le 1er janvier. Le stock, lui, ne s'additionne pas.

**Exemple.** T32 au 31/03/2025 : les pensions en cours ce jour-là ; montant de mars 2025 (ou de janvier à mars avec le cumul).

**Sources.** Recueil v7, l. 153 et « Idem. Tab 41 » (l. 188, 196, 264, 271, 277, 284, 290) ; l. 35 ; remarque B32 du recueil ; Annuaire CNSS 2024, tableaux 28, 41 et 90 (montants de chaque mois) ; même réglage `periodicite` que l'effectif GRH.

**État.** Nouvelle, à intégrer : la requête ne calcule que le 31/12 d'un exercice. Le montant d'un tableau mensuel est une question (Q7, réglage `montant_periode`). T39 et T40 ne sont pas cités : même périodicité par cohérence (Q13).

### N3 — La direction régionale et l'agence

**Règle.** Chaque bénéficiaire est rattaché à la direction régionale de son dossier (`DOSSIER.DR_NO`) : DRO Ouagadougou, DRB Bobo-Dioulasso, DRN Ouahigouya, DRF Fada N'Gourma, DRD Dédougou. Son agence est le service de son dossier (`SP_NO`), comme pour T25 à T27. On découpe ainsi les tableaux et les indicateurs.

**Exemple.** Fin 2024, pensions de vieillesse de la DRO : 20 352 chez nous, 20 067 retraités publiés (+1,4 %).

**Sources.** Recueil v7, l. 4 à 10, l. 1065, l. 1254 et 1265 (rapport démographique et taux de remplacement par DR et agence) ; Annuaire CNSS 2024, tableaux 22, 37, 38, 76, 78, 79 ; Données A02 (ascendants exacts dans les 5 DR, vieillesse à 0,1-4,0 % près), P46 et P47 (20 services, 350 lieux de paiement).

**État.** Nouvelle, à intégrer ; la DR du dossier est plausible face au tableau 22 de l'annuaire CNSS 2024 (vieillesse à 0,1-4,0 % près, ascendants exacts, mais veuves −8,1 % à la DRB et −11,5 % à la DRD) : il précise, sans confirmer, comme dans les familles T25-T27 et T42-T49 ; agence à confirmer par la DSI (Q20, même question que T25-T27 Q20). Le recueil demande la DR et l'agence pour les indicateurs (l. 1065, 1254, 1265) ; pour les dix tableaux eux-mêmes, aucune ligne ne le demande : c'est une proposition BI, appuyée par les l. 31 et 74 (une dimension ajoutée vaut pour tous) et par l'annuaire CNSS, qui publie ces effectifs par DR.

### N4 — Le type de pension reste visible ; la retraite anticipée à part

**Règle.** Le résultat garde le code de chaque prestation. La retraite anticipée (RPRA) va sur une ligne à part sous T32, hors des tranches d'âge : les tranches de T32 ne comptent que la vieillesse (RPV), comme le tableau 19 de la CNSS. Pour la maquette CIPRES d'origine, on additionne les deux.

**Exemple.** Fin 2024 : 31 180 pensions de vieillesse et 87 pensions de retraite anticipée en cours (comparaison A01, des pensions et non des personnes) ; l'annuaire publie 30 730 retraités de vieillesse et 78 de retraite anticipée.

**Sources.** Annuaire CNSS 2024, tableaux 18, 22 et 23 (anticipée publiée à part), 19 (vieillesse seule, sans l'anticipée) ; Recueil v7, l. 248 et 255 (ligne « retraite anticipée » dans T25 et T28) et l. 1251 (l'anticipée est une pension directe) ; Kaya 2017, l. 271 (type à part dans T25) ; Données P76 (16 retraités de moins de 55 ans chez nous, 2 au tableau 19 : sans doute nos retraites anticipées).

**État.** Nouvelle, à intégrer. Tranchée par l'annuaire CNSS 2024 : le recueil, Kaya 2017 et l'Annuaire CIPRES ne disent rien de T32 ; toutes les sources de la CNSS traitent l'anticipée à part (points_a_valider n° 34 et fiche T32 Q2 peuvent être clos). Réglage `retraite_anticipee`. Le nombre de personnes en retraite anticipée reste à mesurer.

### N5 — La pension d'invalidité non convertie

**Règle.** Une pension d'invalidité reste dans T33 même après l'âge de la retraite, tant que l'application ne la transforme pas en pension de vieillesse. La BI ne la déplace pas elle-même.

**Exemple.** Un invalide de 68 ans compte dans T33, ligne « 60 ans et + ».

**Sources.** Kaya 2017, l. 300 à 302 ; Recueil v7, l. 270 ; Annuaire CNSS 2024, tableau 21 (70 des 119 invalides ont 60 ans et plus) ; Données P76 (72 sur 125).

**État.** Nouvelle (pratique de la CNSS), à valider : la loi et le recueil voudraient voir ces personnes en vieillesse. Question Q15, réglage `invalides_age_retraite`.

### N6 — Le tableau des enfants d'allocataires

**Règle.** À côté de T41, un tableau des enfants comptés par tranche d'âge (moins de 5 ans, 5-9, 10-14, 15-19, 20-21 ans), par sexe, avec le montant.

**Exemple.** 2024 : 271 577 enfants chez nous, 268 481 publiés.

**Sources.** Kaya 2017, l. 53 (« prévoir un tableau statistique sur les enfants des allocataires ») ; Annuaire CNSS 2024, tableau 36 ; fiche T41 Q5 ; Données P79 (total), P76 (âges, sans la condition de paiement).

**État.** Nouvelle (Kaya 2017 et pratique de la CNSS) ; tranchée par Kaya 2017 (points_a_valider n° 42 peut être clos). La forme par âge ne retombe pas encore (Q11).

### N7 — Les retraités du rapport démographique

**Règle.** Pour le rapport démographique, les retraités sont les bénéficiaires de pensions directes : vieillesse, retraite anticipée et invalidité, c'est-à-dire T32 (avec sa ligne anticipée) et T33 à la même date, chaque personne une fois, par DR et par agence. Par défaut, les pensions de coordination payées par l'autre caisse y sont : ce sont des bénéficiaires.

**Exemple.** Au 31/12/2024 : 31 093 personnes dans T32 et 125 dans T33.

**Sources.** Recueil v7, l. 1247 à 1254 (NB l. 1251) ; remarque B37 du recueil ; Données P74, P77, P78.

**État.** Nouvelle, à intégrer. Question Q17, réglage `coordination_retraites` (2 901 pensions payées par l'autre caisse fin 2025, près de 9 % des retraités).

### N8 — Le contrôle des entrées et des sorties

**Règle.** Avec chaque tableau, on vérifie : stock de fin d'année = stock de l'année d'avant + nouveaux − décès − autres sorties. Les nouveaux viennent de T42 à T49 ; pour T33, T36 et T41, qui n'ont pas de tableau des nouveaux, les entrées se calculent à part. Un écart signale une erreur de saisie ou de calcul.

**Exemple.** Annuaire CNSS 2024, orphelins : 8 687 + 769 nouveaux − 2 décès = 9 454, mais 9 478 publiés. Aucune sortie, alors que de 900 à 1 900 orphelins sortent chaque année dans la base.

**Sources.** Annuaire CNSS 2024, tableaux 18, 23, 26, 75, 77, 80, 81 ; Annuaire CIPRES p. 15 et 16 (T48 rentes viagères, T49 rentes de veuves et veufs) ; même contrôle que T60 en GRH ; Proposition BI ; Données P74, P82 (773 nouveaux orphelins en 2024).

**État.** Nouvelle (proposition BI).

### N9 — Rentiers plutôt que crédirentiers

**Règle.** Les libellés de T38 (et de T45 à T49) disent « rentiers » au lieu de « crédirentiers ».

**Exemple.** T38 : répartition des rentiers veufs et veuves et montants par tranche d'âge et par sexe.

**Sources.** Recueil v7, l. 194 (T38) et l. 202 (T45).

**État.** Nouvelle (libellé), à intégrer ; pour T39 et T40, par cohérence (Q13).

### N10 — T41 en cours d'année : la fenêtre de paiement

**Règle.** Pour T41 à une fin de mois ou de trimestre, un enfant compte s'il a été payé dans les 12 mois qui précèdent la date d'arrêté. Au 31 décembre, c'est l'année civile, comme aujourd'hui.

**Exemple.** T41 au 30/06/2025 : un enfant payé en novembre 2024 compte encore ; un enfant plus payé depuis mai 2024 ne compte plus.

**Sources.** Recueil v7, l. 153 (périodicité) ; Données P79 (la condition « payé au dernier trimestre » seule donne −1 % d'allocataires en 2024, « payé dans l'année » +0,7 %) ; Proposition BI.

**État.** Nouvelle (proposition BI), à valider (Q6). Réglage `t41_paiement`.

### N11 — Les dossiers transférés à une autre caisse

**Règle.** Une pension dont le dossier a été transféré à une autre caisse (catégorie `CNSS_AC` de `DOSSIER.DOS_CATEGORIE`) reste comptée, comme aujourd'hui. Pour les dossiers de T25 à T27, la règle T25-T27 D1 les retire : un réglage permet de faire de même ici.

**Exemple.** Fin 2025 : 34 pensions de T32 dans ce cas (2 payées) et 79 de T34 (P78).

**Sources.** Données P78 ; T25-T27 D1 ; Proposition BI.

**État.** Nouvelle (proposition BI). Effet petit, pas mesuré pour 2024. Question Q23, réglage `dossier_transfere`.

### N12 — Retrouver chaque bénéficiaire

**Règle.** La BI garde la liste des bénéficiaires : une ligne par personne, tableau et date d'arrêté, avec son code de prestation, son âge, son sexe, sa DR, son agence et son montant, sans nom ni numéro d'assuré. Chaque case de T32 à T41 se retrouve dans cette liste. Qui peut la voir dépend de son profil.

**Exemple.** Les 125 invalides de T33 au 31/12/2024 se listent un à un, sans nom.

**Sources.** Recueil v7, l. 51, 68 (et l. 29), l. 72 (contrôle par profil), l. 31 et 74.

**État.** Nouvelle ; aucune requête ne la produit. La table du DTM qui la portera reste à choisir avec Jules (partie 7, point 8).

---

## 3. Les cas

| Cas | Ce que fait la règle | Pourquoi |
|---|---|---|
| 1. Un retraité (code RPV) payé tous les mois de 2024, né en 1958 | Compté dans T32 au 31/12/2024, tranche 65-69 ans (2024 − 1958 = 66) ; montant : les 12 mois payés en 2024 | Compté (R6, R8, R3) |
| 2. Un retraité dont la pension court, ni suspendue ni radiée, mais plus payée depuis octobre 2024 (certificat de vie attendu) | Compté au 31/12/2024, comme dans l'annuaire CNSS ; montant : les mois payés, de janvier à septembre. En 2024, 3 423 retraités comptés n'ont rien reçu de la CNSS dans l'année (31 093 − 27 670) ; une grande part sont des pensions payées par l'autre caisse (2 901 fin 2025 ; pas mesuré pour 2024) | Compté (R6 : payé ou non) |
| 3. Une pension de coordination que verse l'autre caisse (dossier `COOR_AC`) | Comptée dans l'effectif de T32, montant 0 FCFA. Avec elles, la pension moyenne est 8,8 % plus basse (fin 2025) | Selon un réglage (`moyenne_coordination` ; R3 ; Q16) |
| 4. Un retraité décédé le 12/08/2024, payé de janvier à juillet | Pas compté au 31/12/2024. Ses 7 mois payés n'entrent dans T32 que si `montant` = TOUT, sur la ligne « sortis dans l'année ». En 2024, 1 105 pensionnés sortis dans l'année ont reçu 0,96 Md | Pas compté (R6) ; montant selon un réglage (R3, Q1) |
| 5. Une pension suspendue le 31/10/2024, pas reprise au 31/12 | Pas comptée. Fin 2024, 253 pensions de vieillesse sont dans ce cas ; les compter mettrait T32 à +1,7 % de l'annuaire au lieu de +0,9 % | Pas compté (R6 ; P81) |
| 6. Une retraite anticipée (code RPRA), 54 ans en 2024 | Sur la ligne à part « retraite anticipée » sous T32, hors des tranches. L'annuaire la publie à part (78 en 2024) et la sort de son tableau des âges | Selon un réglage (`retraite_anticipee` = LIGNE_A_PART ; N4, tranché par l'annuaire) |
| 7. Une veuve dont la pension (RPSV) est ajoutée au dossier de son mari retraité ; elle a aussi sa propre retraite | Comptée dans T34 à son âge et à son sexe à elle, et dans T32 pour sa retraite. On n'additionne jamais T32 et T34 pour compter des personnes. 13 104 des 25 541 pensions de veuve sont dans le dossier du défunt (carte B9 de `regles_prestations`) | Compté (R5) |
| 8. Une veuve « née en 1900 » (date mise par défaut à la saisie) : 124 ans en 2024 ; 499 veuves dans ce cas | Comptée dans T34, ligne « 100 ans et plus », comme dans l'annuaire CNSS (679 veuves de 100 ans et plus dans son tableau 20) ; un contrôle à part compte ces dates. La requête actuelle la met en « âge inconnu » | Selon un réglage (`ages_impossibles` ; R8, tranché par l'annuaire) |
| 9. Un orphelin né en 1999, pension (RPSE) jamais radiée, plus payée ; son tuteur recevait l'argent | Pas compté dans T35 en 2024 : la limite est « moins de 25 ans », même sans radiation (5 774 pensions dans ce cas fin 2024). Un orphelin de 24 ans est compté, à son âge à lui | Pas compté (R6, R5 ; recueil l. 283) |
| 10. Un invalide de 68 ans (code RPI), pension jamais transformée en pension de vieillesse | Compté dans T33, ligne « 60 ans et + », comme dans l'annuaire CNSS (70 de ses 119 invalides ont 60 ans et plus). Le jour où l'application aura le module, il passera dans T32 | À surveiller (N5 ; Q15) |
| 11. Un orphelin de 23 ans, rentier après la mort de son père dans un accident du travail (code ARSE) | Compté dans T39 avec la limite des pensions (moins de 25 ans). L'annuaire compte 485 rentes d'orphelins en 2024, entre nos 454 (moins de 25 ans) et 531 (sans limite) | Question (R6 ; Q12) |
| 12. Un assuré de 38 ans, titulaire du dossier d'allocations familiales ; enfants de 4, 9 et 13 ans, payés en 2024 | Un allocataire dans T41, tranche 35-39 ans, au sexe du titulaire ; montant : les allocations des 3 enfants. Les 3 enfants comptent dans le tableau des enfants | Compté (R6 pour T41, N2, N6) |
| 13. Un enfant dont l'allocation court au 31/12/2024 sans aucun paiement en 2024 (certificat de scolarité attendu) ; seul enfant du dossier | L'enfant n'est pas compté, l'allocataire non plus. C'est ce qui retombe sur l'annuaire CNSS en 2024 : +0,7 % d'allocataires ; en comptant ces enfants, +5,4 % | Pas compté (R6 pour T41 ; P79 ; Q5) |
| 14. Un enfant né en 2002 dont l'allocation n'a pas été radiée | Pas compté : 21 ans au plus, comme la dernière tranche « 20-21 ans » du tableau 36. Le dossier ne compte que si un autre enfant est compté | Pas compté (R6 pour T41) |
| 15. Un titulaire de dossier d'allocations familiales décédé ; ses enfants touchent encore, par un tuteur | Aujourd'hui compté dans T41, avec l'âge qu'il aurait. Le recueil dit « titulaire du dossier » ; le glossaire CIPRES compterait plutôt la personne qui touche les allocations « du chef » de l'assuré décédé. Nombre de cas pas encore mesuré | Question (N2 ; Q8) |
| 16. Une veuve décédée en novembre 2024, décès saisi en juin 2025 | Comptée dans l'annuaire CNSS 2024, arrêté avant la saisie ; pas comptée si l'on calcule 2024 sur la base d'aujourd'hui. Sur la copie de fin mars 2025, elle serait comptée comme par la CNSS | À surveiller (R13 ; Q3, Q4) |
| 17. Un rappel versé en février 2025 pour octobre à décembre 2024 | Compté en 2024 avec `date_montant` = MOIS (aujourd'hui), en 2025 avec SAISIE. Le choix change les montants de −3,5 % à +29 % selon le tableau, et l'effectif de T41 | Selon un réglage (`date_montant` ; R4 ; Q2) |
| 18. Une pension de vieillesse révisée en 2025 avec un effet en 2024 : deux pensions en cours au 31/12/2024 pour la même personne | Une seule personne dans T32. 174 personnes dans ce cas en 2024 | Compté une fois (R5) |
| 19. Une pension d'un dossier transféré à une autre caisse (`CNSS_AC`) | Comptée aujourd'hui (34 dans T32 et 79 dans T34 fin 2025). T25 à T27 retirent ces dossiers | Selon un réglage (`dossier_transfere` ; N11 ; Q23) |

---

## 4. Face à l'annuaire CNSS 2024

Les modèles de ratios de la CNSS n'apportent rien de direct ici (voir « Ce que disent les sources »). L'annuaire, lui, publie presque tous ces effectifs. Seule l'année 2024 a été publiée sur une base proche de la nôtre ; mais on ne garde pas les seules années qui arrangent : quand 2020 à 2023 s'écartent, c'est dit.

| Tableau de l'annuaire | Ce qu'on compare | Nous | Publié | Écart | Verdict | Ce que ça change |
|---|---|---|---|---|---|---|
| Tableau 18 (p. 16) | Vieillesse et anticipée, 2020 à 2024 (T32) | 25 273 ; 26 587 ; 28 077 ; 29 265 ; 31 093 personnes (P74) | 25 230 ; 26 481 ; 27 941 ; 29 117 ; 30 808 | +0,2 % ; +0,4 % ; +0,5 % ; +0,5 % ; +0,9 % | confirme | R6 est la définition de la CNSS (payée ou non). Exiger un paiement dans l'année donnerait 27 670 en 2024 (−10,2 %). L'écart de 2024 vient sans doute de pensions créées ou révisées après la publication : à vérifier par A06 |
| Tableau 18 (p. 16) | Invalidité, 2020 à 2024 (T33) | 111 ; 109 ; 119 ; 121 ; 125 | 114 ; 111 ; 115 ; 116 ; 119 | −3 à +6 (−2,6 % à +5,0 %) | confirme | Quelques unités sur de petits nombres |
| Tableaux 18 et 22 (p. 16 et 18) | Ascendants 2024 (T36) | 59 ; par DR 48 / 8 / 1 / 1 / 1 | 59 ; 48 / 8 / 1 / 1 / 1 | 0 | confirme | Exact, dans chacune des 5 DR |
| Tableaux 18 et 80 (p. 16 et 62) | Ascendants 2020 à 2023 (T36, T40) | T36 : 58 ; 60 ; 65 ; 59. T40 : 225 ; 218 ; 230 ; 214 | T36 : 48 ; 53 ; 59 ; 54. T40 : 185 ; 187 ; 193 ; 196 | T36 : +20,8 % ; +13,2 % ; +10,2 % ; +9,3 %. T40 : +21,6 % ; +16,6 % ; +19,2 % ; +9,2 % (5 à 40 personnes) | écart à expliquer | Seule 2024 tombe juste (T36 exact, T40 +3,4 %). Pensions ou rentes créées après coup avec un effet passé, ou autre définition à l'époque : à vérifier, pas bloquant |
| Tableau 18 (p. 16) | Veuves et veufs, 2020 à 2024 (T34) | 19 869 ; 20 567 ; 21 154 ; 20 258 ; 20 715. En 2024 : 20 359 femmes, 356 hommes (P75) | 19 534 ; 20 213 ; 21 008 ; 21 130 ; 21 767. En 2024 : 21 438 veuves, 329 veufs | +1,7 % ; +1,8 % ; +0,7 % ; −4,1 % ; −4,8 %. En 2024 : veuves −1 079 (−5,0 %), veufs +27 (+8,2 %) | écart à expliquer | Tout l'écart est sur les veuves. Les suspensions (250 fin 2024) ne l'expliquent pas. Piste : décès ou radiations saisis après l'arrêt de l'annuaire (R13 ; A06, A07). Q4 |
| Tableau 20 (p. 17) | Les orphelins comptés à leur âge, moins de 25 ans (T35) | La requête compte les orphelins de moins de 25 ans, à leur âge | 9 478 orphelins, tous de moins de 25 ans : les 5 tranches de moins de 25 ans font exactement les orphelins du tableau 18 | aucun sur la règle | confirme | La limite de 25 ans et le comptage de l'orphelin sont la pratique de la CNSS et la demande du recueil (l. 283). Reste la radiation des 5 774 pensions d'orphelins de 25 ans et plus (partie 7) |
| Tableaux 18, 20, 23 et 26 (p. 16 à 21) | Les orphelins pensionnés, 2020 à 2024 (T35) | 9 686 ; 9 535 ; 9 276 ; 8 108 ; 7 970. En 2024 par âge : 112 ; 596 ; 1 339 ; 2 861 ; 3 062 | 8 982 ; 8 789 ; 8 667 ; 8 687 ; 9 478. En 2024 : 93 ; 636 ; 1 551 ; 3 386 ; 3 812 | +7,8 % ; +8,5 % ; +7,0 % ; −6,7 % ; −15,9 %. En 2024 : −1 275 à 15-24 ans, −233 à 0-14 ans | écart à expliquer | Le chiffre 2024 de la CNSS ne tient pas avec ses propres entrées : 8 687 + 769 − 2 = 9 454 pour 9 478, sans aucune sortie à 25 ans, alors que la base en voit sortir de 900 à 1 900 par an. On ne juge pas la règle sur lui seul. A07 et A08 à exécuter |
| Tableaux 18, 23 et 26 (p. 16, 19, 21) | Entrées et sorties de 2024 (N8) | Base 2024 : 29 265 + 2 463 nouveaux − 31 093 = 635 sorties de retraités, presque toutes des décès (619 connus aujourd'hui, P97) | Vieillesse 29 042 + 2 486 − 447 = 31 081 (30 730 publiés) ; veuves 21 461 (21 438) ; veufs 329 (329) ; invalidité 119 (119) ; anticipée 77 (78) ; orphelins 9 454 (9 478) ; ascendants 54 + 7 − 89 = −28 (59) | Chez la CNSS : 351 retraités sortis sans être comptés parmi les décès ; orphelins 24 de trop ; 89 décès d'ascendants pour 54 pensionnés | précise | Les chiffres publiés ne tiennent pas toujours d'un tableau à l'autre : une règle ne se corrige pas sur une seule ligne publiée. La BI montre ce contrôle à côté de chaque tableau (N8). Décès d'ascendants : points_a_valider n° 57 |
| Tableau 19 (p. 16) | Grille et sexe des retraités (T32) | Grille de la maquette CIPRES p. 37 ; 24 814 hommes, 6 279 femmes (T32, anticipée comprise) | Même grille ; 24 627 hommes, 6 103 femmes (vieillesse seule) | Hommes +187 (+0,8 %) ; femmes +176 (+2,9 %) | confirme | La grille et le code du sexe sont ceux de la CNSS. Les femmes en trop sont surtout à 55-64 ans (+187 : 895 contre 772, et 2 341 contre 2 277) : à expliquer, lié à points_a_valider n° 44. Comparaison à refaire sur la vieillesse seule |
| Tableaux 19 et 25 (p. 16 et 20) | Forme par âge (T32, R8) | 55-59 ans 3 778 ; 55-64 ans 12 632 ; 65-89 ans 18 080 ; moins de 55 ans 16 ; 100 ans et plus 74. Nouveaux retraités (P83) : 377 de 60 ans, 353 de 61 ans | 3 399 ; 12 037 ; 18 341 ; 2 ; 56. Tableau 25 : 519 de 60 ans, 256 de 61 ans | 55-59 ans +379 (+11,2 %) ; 55-64 ans +595 ; 65-89 ans −261 | écart à expliquer | Plus de jeunes, moins d'âgés. Deux pistes : la CNSS calculerait l'âge à une autre date (le lot 7 voit le même décalage chez les nouveaux retraités) ; ou une base qui a vécu depuis (R13). Les 14 de moins de 55 ans en trop sont sans doute des retraites anticipées (N4). Q10 |
| Tableaux 19 et 34 (p. 16 et 28) | Montant de T32 | Vieillesse et anticipée : 37 665 983 112 aux comptés, 38 625 184 454 tout le payé. Vieillesse seule, tout le payé : 38 496 066 498 au mois payé, 38 821 571 458 à la saisie | 38 235 880 033 | −1,5 % ; +1,0 % ; +0,7 % ; +1,5 % | précise | Le publié tombe entre nos deux montants. Le plus proche n'est pas le même partout : pour la vieillesse seule, tout le payé au mois payé ; pour les survivants, le payé aux comptés ; pour l'invalidité, tout le payé à la saisie (ligne des tableaux 20, 21 et 36) : Q1 et Q2 restent ouvertes |
| Tableaux 28 et 34 (p. 22 et 28) | Majoration pour enfant 2024 (A1, N13) | Bonification (RBEC) : 539 376 000 à la saisie, 554 254 000 au mois payé (P98) | 537 343 000, à part de la pension ; le tableau 19 ne prend que la pension | +0,4 % (saisie) ; +3,1 % (mois payé) | confirme | A1 : l'accessoire ne fait pas un pensionné de plus. N13 : son montant reste hors de T32 (tranché par ces tableaux) |
| Tableau 20 (p. 17) | Survivants de 100 ans et plus (R8) | 635 femmes, dont 500 de 120 ans et plus (« nées en 1900 ») ; 135 sans elles | 679 femmes, aucun homme ; 60 764 FCFA par personne, plus que les 90-94 ans (39 646) et les 95-99 ans (27 792) | −44 en gardant ces dates ; −544 sans elles | tranche | La CNSS range ces dates par défaut dans « 100 ans et plus », et la maquette CIPRES n'a pas de ligne « âge inconnu ». Leur montant moyen laisse penser que ce ne sont pas de vraies centenaires. R8 : réglage `ages_impossibles` = DERNIERE_TRANCHE, avec un contrôle qui compte ces dates |
| Tableau 20 (p. 17) | Veuves, veufs et ascendants par âge (T34, T36) | 20 777 ; 25-54 ans 2 314 ; 55-99 ans 17 828 ; 100 ans et plus 635 (P76) | 21 826 ; 2 315 ; 18 832 ; 679 | −1 049 (−4,8 %) : −1 à 25-54 ans, −1 004 à 55-99 ans, −44 à 100 ans et plus | précise | Les veuves qui nous manquent ont presque toutes 55 ans ou plus : pas des remariages (aucun écart avant 55 ans), pas des suspensions. Décès ou radiations saisis après l'arrêt de l'annuaire : à confirmer par une requête (A06, A07). Q4 |
| Tableaux 20, 21 et 36 (p. 17, 18, 30) | Montants 2024 des survivants (T34 à T36), des invalides (T33), des allocations familiales (T41) | Survivants : 7 161 046 495 aux comptés, 7 222 987 423 en tout, 7 184 934 762 à la saisie. Invalidité : 112 058 723 ; 116 130 355 ; 113 131 108. Allocations familiales : 7 688 913 000 ; 7 954 458 000 ; 8 000 440 000 | 7 047 164 871 ; 113 603 730 ; 8 236 741 500 | Survivants +1,6 % ; +2,5 % ; +2,0 %. Invalidité −1,4 % ; +2,2 % ; −0,4 %. Allocations familiales −6,7 % ; −3,4 % ; −2,9 % | précise | Le publié tombe entre nos deux montants pour l'invalidité ; il est sous nos montants pour les survivants et au-dessus pour les allocations familiales. La CNSS aligne ses montants sur la comptabilité. Q1, Q2, Q9 |
| Tableau 21 (p. 18) | Invalides par âge et sexe (T33) | 125 (91 hommes, 34 femmes) ; 40-44 ans 6 ; 45-49 ans 8 ; 50-54 ans 17 ; 55-59 ans 22 ; 60 ans et plus 72 | 119 (86 hommes, 33 femmes) ; 3 ; 7 ; 16 ; 23 ; 70 | +6 (+5,0 %) ; 60 ans et plus : 57,6 % chez nous, 58,8 % à la CNSS | confirme | La grille de T33 est celle de la CNSS. Plus de la moitié des invalides ont passé 60 ans : la CNSS les laisse en invalidité, la BI aussi (N5) |
| Tableau 22 (p. 18) | Pensionnés par DR (N3) | Pensions (A02) : vieillesse 20 352 / 7 638 / 1 366 / 1 007 / 818 ; veuves et veufs 10 744 / 7 031 / 1 473 / 917 / 563 ; ascendants 48 / 8 / 1 / 1 / 1 | Vieillesse 20 067 / 7 538 / 1 340 / 968 / 817 ; veuves et veufs 11 123 / 7 647 / 1 471 / 890 / 636 ; ascendants identiques (ordre DRO, DRB, DRN, DRF, DRD) | Vieillesse +0,1 % à +4,0 % ; ascendants exacts ; veuves et veufs : DRO −379 (−3,4 %), DRB −616 (−8,1 %), DRD −73 (−11,5 %), DRN +2, DRF +27 | précise | La DR de la CNSS semble être celle du dossier (N3). Les veuves manquent surtout à Bobo-Dioulasso et à Ouagadougou. A02 compte des pensions, pas des personnes. Orphelins par DR pas encore comparés (A02 compte tous les âges) Verdict « précise », comme dans les familles T25-T27 et T42-T49 : des chiffres plausibles, pas une preuve, tant que les veuves s'écartent selon la DR |
| Tableau 36 (p. 30) | Enfants d'allocataires par âge et sexe (N6, T41) | 271 577 enfants payés (P79). Par âge, sans la condition de paiement : 71 155 ; 79 887 ; 68 923 ; 50 538 ; 13 016 ; 53,5 % de garçons | 268 481 ; 59 869 ; 74 632 ; 65 913 ; 48 889 ; 19 178 ; 53,7 % de garçons ; 8 236 741 500 FCFA | Total +3 096 (+1,2 %) ; moins de 5 ans +11 286 (+18,9 %) ; 20-21 ans −6 162 (−32,1 %) | écart à expliquer | La limite de 21 ans, le sexe et le tableau des enfants sont confirmés (N6). La forme par âge non : la condition de paiement ne peut que baisser nos 20-21 ans, et leurs 18 177 FCFA en moyenne dans l'année laissent penser que la CNSS compte aussi les enfants sortis dans l'année à 21 ans. Q10, Q11 |
| Tableaux 37 et 38 (p. 30 et 31) | Allocataires et enfants 2024 (T41) | 98 048 allocataires ; 271 577 enfants (P79) | 97 378 ; 268 481 ; 39 856 conjoints | +0,7 % ; +1,2 %. Sans la condition de paiement : +5,4 % et +5,6 % | confirme | Seule la condition « enfant payé dans l'année » retombe en 2024 (R6 pour T41), avec les seules allocations familiales. Les conjoints ne sont demandés ni par la CIPRES ni par le recueil. Comparaison par DR à faire |
| Tableau 38 (p. 31) | Allocataires 2020 à 2023 (T41) | 93 972 ; 96 656 ; 98 570 ; 98 128 | 84 401 ; 88 587 ; 91 085 ; 93 233 | +11,3 % ; +9,1 % ; +8,2 % ; +5,3 % | écart à expliquer | Non expliqué. P79 a testé la base vue au 30/06 de l'année suivante : l'écart ne bouge presque pas. Autre piste, non vérifiée : des paiements saisis après coup pour des mois anciens. 2024 seule ne suffit pas à confirmer la règle |
| Tableau 76 (p. 60) | Rentiers viagers 2024 par âge et sexe (T37) | 1 236 (1 046 hommes, 190 femmes) ; par tranche, de 0 à 13 d'écart (70-74 ans : 145 pour 158) | 1 255 (1 061 hommes, 194 femmes) ; DRO 713, DRB 447, DRN 37, DRF 38, DRD 20 | −19 (−1,5 %) ; hommes −15 (−1,4 %), femmes −4 (−2,1 %) | confirme | T37 se calcule comme la CNSS. Sa grille s'arrête à « 90 ans et + », celle de la CIPRES à « 100 ans et plus » : on regroupe pour comparer. La DR du dossier donnera la colonne par DR (N3) |
| Tableaux 78, 79 et 80 (p. 61 et 62) | Rentes en cours 2024 (T37, T38, T40) ; et T37, T38 de 2020 à 2023 | 2024 : T37 1 236 ; T38 288 ; T40 214. Avant : T37 1 358 ; 1 325 ; 1 305 ; 1 238. T38 319 ; 310 ; 312 ; 293 | 2024 : 1 255 ; 285 (283 veuves, 2 veufs) ; 207. Ensemble 2 232, avec les 485 rentes d'orphelins. Avant : 1 303 ; 1 278 ; 1 270 ; 1 246. 309 ; 298 ; 297 ; 285 | 2024 : −1,5 % ; +1,1 % ; +3,4 %. Avant : T37 de −0,6 % à +4,2 % ; T38 de +2,8 % à +5,1 % | confirme | R6 vaut pour les rentes. Les flux 2024 tiennent (rentes viagères : 14 sorties autres que des décès). « Ensemble au 31-12 » (tableau 78) appuie la lecture « stock » du « cumul » de T38 (Q14). T40 avant 2024 : voir la ligne des ascendants |
| Tableaux 80, 77 et 81 (p. 61 et 62) | Rentes d'orphelins 2020 à 2024 (T39) | Moins de 25 ans : 475 ; 482 ; 497 ; 450 ; 454. Sans limite d'âge : 605 ; 599 ; 602 ; 527 ; 531 | 492 ; 421 ; 399 ; 379 ; 485 ; 44 nouvelles et aucun décès en 2024 | Avec la limite : −3,5 % ; +14,5 % ; +24,6 % ; +18,7 % ; −6,4 %. Sans limite : +23 % à +51 %, +9,5 % en 2024 | écart à expliquer | Le chiffre publié saute de 106 en 2024 pour 44 entrées (379 + 44 = 423 pour 485). De 2021 à 2023, il est sous les nôtres, comme avec une limite plus basse. Limite à demander (Q12) ; 25 ans en attendant |
| Tableaux 90, 96 et 97 (p. 71 et 74) | Montants 2024 des rentes (T37 à T40) | Rentes viagères : 399 428 069 au mois payé, 397 834 986 à la saisie, 384 111 754 aux comptés. Rentes de survivants : 275 923 240 ; 322 631 157 ; 274 761 735 | 455 021 317 ; 368 602 710 (les rachats de rente, 25 366 909, sont à part) | Viagères −12,2 % ; −12,6 % ; −15,6 %. Survivants −25,1 % ; −12,5 % ; −25,5 % | écart à expliquer | Une partie des paiements des rentes d'accident manque dans la base des prestations (Q21). Les effectifs de T37 à T40 sont justes ; leurs montants sont présentés avec une réserve |
| Tableau 28 et tableaux 29 à 33 (p. 22 à 27) | Prestations vieillesse servies mois par mois, ensemble et par DR (R4, N1, N3) | pas encore comparé | Survivants : 396 à 424 millions en janvier, avril, juillet et octobre ; 602 à 773 millions les autres mois ; 7 047 164 871 sur l'année | — | précise | Profil très marqué : comparer mois par mois nos paiements au mois payé et à la saisie peut trancher R4 sans attendre la CNSS, comme le tableau 121 l'a fait pour la GRH. Donne aussi les montants mensuels (N1) et par DR (N3) |
| Tableau 90 et tableaux 91 à 95 (p. 71 à 73) | Rentes d'accident servies mois par mois, ensemble et par DR (R4) | pas encore comparé | Rentes viagères : 22 à 25 millions en janvier, avril, juillet et octobre ; 39 à 51 millions les autres mois, pour environ 1 250 rentiers | — | précise | Même test de R4 pour T37 à T40 |
| Tableau 40 et tableaux 41 à 46 (p. 32 à 36) | Allocations familiales par DR et par mois (T41) | pas encore comparé | DRO 4 970 538 500 ; DRB 1 818 839 500 ; DRN 650 602 000 ; DRF 498 528 000 ; DRD 298 233 500 | — | précise | Montant de T41 par DR (N3) et par mois (N1, R4) |
| Tableaux 34 et 96 (p. 28 et 74) | Montants par DR (N3) | pas encore comparé | Pension de retraite : DRO 26 978 969 899 ; DRB 9 088 718 494 ; DRN 937 252 400 ; DRF 664 250 549 ; DRD 566 688 691. Rentes par DR au tableau 96 | — | précise | Contrôle des montants par DR |
| Tableau 25 (p. 20) | Nouveaux retraités de 2024 par âge exact et par sexe | P83 : 377 de 60 ans, 353 de 61 ans ; 641 nouvelles retraitées | 519 ; 256 ; 527 femmes | Décalage d'âge ; femmes +114 | précise | Le même décalage que dans T32 (tableau 19) : la date de calcul de l'âge est à vérifier avant de dire R8 confirmée en entier (Q10 ; points_a_valider n° 44) |
| Tableaux 106 et 97 (p. 80 et 74) | Montants servis de 2020 à 2024 | pas encore comparé | Branche des pensions : de 35 232 227 700 (2020) à 46 629 625 556 (2024) ; rentes d'accident sur 5 ans au tableau 97 | — | précise | Tester les règles de montant sur cinq ans, comme les effectifs ; dire si le manque d'environ 12 % des paiements d'accident est constant |
| Démarche et fiche signalétique (p. xiii, xiv et xx) | Comment la CNSS fait son annuaire | — | Le service Études et Statistique de la DPC exploite la base et les rapports des services, complète par une collecte par an dans chaque DR, conforme les montants au compte de résultat, fait valider en atelier ; publication en juin 2025. 62 231 bénéficiaires des pensions = tableau 18 ; 2 232 rentes = tableau 79 | — | précise | On ne retombera jamais à l'unité. Un rapprochement avec la comptabilité (vue `V_BI_BALANCE_COMPTABLE`, attendue) dira la date des paiements mieux que la base des prestations. Publication en juin : arrêt vers fin mars (R13). Le total de 62 231 additionne des pensions, pas des personnes (R5) |
| Tableaux 18 à 80 | Ce qui n'est pas publié | — | Ni l'âge ni le sexe des allocataires (T41) ; T34 et T36 mélangés par âge (tableau 20) ; aucun montant par tranche pour les rentes (T37) ; ni âge, ni sexe, ni montant par type pour T38 à T40 ; ni l'âge des retraites anticipées, ni la part CNSS de la coordination | — | pas publié | Ces colonnes ne se valident qu'avec la CNSS |

**En clair, ce que l'annuaire confirme :**
- la façon de compter un bénéficiaire, payé ou non (R6) : T32, T33, T36, T37, T38 et T40 en 2024, T32 sur cinq ans ;
- l'orphelin compté à son âge, avant 25 ans (R5, R6) ;
- la condition « enfant payé dans l'année » pour T41, en 2024 seulement ;
- les grilles de T32, T33 et T37, et le code du sexe (R8) ;
- l'accessoire qui ne fait pas un pensionné de plus (A1) ;
- l'invalidité gardée après 60 ans (N5).

**Ce qu'il tranche :** la retraite anticipée à part (N4, tableaux 18, 19, 22) ; le montant de T32 sans les accessoires (N13, tableaux 19, 28, 34) ; les dates de naissance par défaut dans « 100 ans et plus » (R8, tableau 20) ; la limite de 21 ans des enfants (dernière tranche « 20-21 ans » du tableau 36 ; la forme par âge, elle, reste un écart à expliquer, carte N6). Ces choix tranchés tiennent à la présentation de l'annuaire (ce qu'il publie à part, ses grilles), pas à un chiffre : ils n'ont pas de ligne « tranche » dans le tableau ci-dessus, sauf R8.

**Ce qu'il précise sans trancher :** la DR du dossier (N3, tableau 22) ; le montant (comptés ou tout le payé, tableaux 19, 20, 21, 34, 36) ; les entrées et sorties (tableaux 23, 26) ; les séries mensuelles et par DR (tableaux 28 à 34, 40 à 46, 90 à 96), qui peuvent trancher R4.

**Ce qu'il faut expliquer :** les veuves (−5,0 %) et les orphelins (−15,9 %) de 2024 ; les rentes d'orphelins ; les ascendants et T41 de 2020 à 2023 ; la forme par âge de T32 et des enfants ; les montants des rentes d'accident (−12 à −25 %).

**Ce qu'il ne publie pas :** l'âge et le sexe des allocataires, le détail par âge de T34, T36, T38 à T40, les montants par tranche des rentes.

---

## 5. Les réglages

Ils s'écrivent dans la CTE `parametres`, le bloc de réglages en tête de la requête t32_t41. Changer une valeur ne demande pas de réécrire la requête. Une décision attendue de la CNSS ne bloque rien : la valeur retenue s'applique en attendant. « À créer » : le choix est aujourd'hui écrit en dur dans la requête, ou n'existe pas. Un réglage suit le recueil v7 par défaut ; quand le recueil se tait, il suit la pratique de l'annuaire CNSS.

| Réglage | Requête | Valeur retenue | Autre valeur | Ce que ça change | Qui décide |
|---|---|---|---|---|---|
| `annee`, `debut`, `fin` | t32_t41 (aujourd'hui, l'exercice est donné au lancement, `:2`) | Proposé : calculés dans la CTE `parametres` à partir du cliché et de `periodicite`, le cliché restant la seule valeur donnée au lancement | l'exercice donné au lancement (comme aujourd'hui) | Fixe les dates d'arrêté et la période des montants | BI (forme de la requête, comme l'effectif GRH) |
| `date_montant` | t32_t41 | MOIS : le mois payé | SAISIE : le jour de saisie du paiement, rappels compris | Montants de −3,5 % (retraite anticipée) à +29,4 % (rentes d'orphelins) selon le tableau ; vieillesse +0,7 % ou +1,5 % face à l'annuaire. Change aussi l'effectif de T41 (condition de paiement) : à mesurer avant toute décision | DCP, avec la DPC (Q2 ; question du 02/11/2026) |
| `montant` (à créer) | t32_t41 (en dur : payé aux comptés) | COMPTES : payé dans l'année aux bénéficiaires comptés | TOUT : tout le payé de l'année pour les codes du tableau, avec une ligne à part « sortis dans l'année » et une moyenne sans elle | T32 2024 : 37,67 Md ou 38,63 Md (38,24 Md publiés) ; la ligne « sortis » ferait 0,96 Md | DCP ; DES et DR pour T41 ; DP et DR pour les rentes (Q1) |
| `montant_periode` (à créer) | t32_t41 (N1) | PERIODE : le montant du mois ou du trimestre ; l'année = la somme des mois | CUMUL : depuis le 1er janvier | La pension moyenne d'un mois ne se compare pas à celle d'une année | DCP, DES, DP (Q7) |
| `periodicite` (à créer, comme l'effectif GRH) | t32_t41 | MENSUELLE : chaque fin de mois (le 31/12 en fait partie) | ANNUELLE (comme aujourd'hui) ; TRIMESTRIELLE | Plus de lignes ; dans l'outil de restitution (OBIEE), l'effectif doit être en « dernière valeur » pour ne pas additionner les mois | tranché par le recueil (l. 153) ; Jules pour OBIEE |
| `retraite_anticipee` (à créer) | t32_t41 (en dur : dans les tranches de T32) | LIGNE_A_PART : une ligne « retraite anticipée » sous T32, hors des tranches | DANS_T32 (comme aujourd'hui) ; HORS_T32 | 87 pensions de 2024 changent de ligne ; le nombre de personnes est à mesurer | tranché par l'annuaire CNSS 2024 (tableaux 18, 19, 22) |
| `ages_impossibles` (à créer) | t32_t41 (en dur : « âge inconnu ») | DERNIERE_TRANCHE : 120 ans et plus dans « 100 ans et plus », avec un contrôle à part | LIGNE_A_PART : « âge inconnu » (comme aujourd'hui) | 504 personnes de 120 ans et plus en 2024 (500 en T34, 3 en T38, 1 en T32) changent de ligne ; le total ne bouge pas | tranché par l'annuaire CNSS 2024 (tableau 20) et la maquette CIPRES |
| `age_max_orphelin` (à créer) | t32_t41 (en dur : moins de 25 ans, T35 et T39) | 25 (moins de 25 ans) | 21 pour T39, si la CNSS le dit | T39 2024 : 454 avec la limite de 25 ans, 531 sans limite (485 publiés) | tranché pour T35 (recueil l. 283, tableau 20) ; DP et DR pour T39 (Q12) |
| `age_max_enfant` (à créer) | t32_t41 (en dur : 21 ans au plus, T41) | 21 | une autre limite | Les enfants de 22 ans et plus ne comptent pas | tranché par l'annuaire CNSS 2024 (tableau 36) |
| `t41_paiement` (à créer) | t32_t41 (en dur : paiement obligatoire dans l'année) | 12_MOIS : payé dans les 12 mois avant la date d'arrêté ; au 31/12, c'est l'année civile (N10) | SANS : comme les pensions | 2024 : 98 048 allocataires (+0,7 %) ; sans condition, 102 643 (+5,4 %) | DES et DR (Q5, Q6) |
| `allocataire` (à créer) | t32_t41 (en dur : un allocataire par dossier) | TITULAIRE : chaque titulaire une fois, même avec deux dossiers | DOSSIER (comme aujourd'hui) | −6 allocataires en 2024 sans la condition de paiement ; à remesurer sur les 98 048 | tranché par le recueil (l. 152) |
| `moyenne_coordination` (à créer) | t32_t41, colonne du montant moyen | TOUTES : montant ÷ effectif, pensions payées par l'autre caisse comprises | PAYEES_CNSS : moyenne des seules pensions que paie la CNSS | Fin 2025, la pension moyenne de T32 serait 9,7 % plus haute sans elles (8,8 % plus basse avec) ; à mesurer pour 2024. Même choix pour T34 (334 pensions, 1 payée) et T35 (315, 2 payées) | DCP (Q16) |
| `invalides_age_retraite` (à créer) | t32_t41 (N5) | DANS_T33 : comme l'application et l'annuaire | A_PART : une ligne à part pour les invalides qui ont passé l'âge de la retraite | 72 des 125 invalides de 2024 ont 60 ans et plus | DCP (Q15) |
| `coordination_retraites` (à créer) | indicateur « rapport démographique » (N7) | AVEC : toutes les pensions directes | SANS : sans les pensions payées par l'autre caisse | 2 901 pensions de vieillesse payées ailleurs fin 2025, près de 9 % des retraités | DCP (Q17) |
| `remplacement_population` (à créer) | indicateur « taux de remplacement » | STOCK : toutes les pensions directes en cours à la date d'arrêté | NOUVELLES : les pensions directes nouvelles de l'année (T42) | Change la population de l'indicateur | DCP (Q18) |
| `dossier_transfere` (à créer) | t32_t41 (N11) | GARDER (comme aujourd'hui) | EXCLURE : comme T25 à T27 (T25-T27 D1) | Fin 2025 : 34 pensions de T32 (2 payées), 79 de T34 | DCP, avec la DSI (Q23) |
| `agence` (à créer) | t32_t41 (N3) | SERVICE : le service du dossier (`SP_NO`) | LIEU_PAIEMENT | Le découpage par agence des tableaux et des indicateurs | DSI (Q20, même question que T25-T27 Q20) |
| `perimetre` et `grille` (listes) | t32_t41, CTE `perimetre` et `grille` | Les codes de la règle R2 et les grilles de l'Annuaire CIPRES (bas et haut de chaque tranche, par tableau) | — | Changer un code ou une borne sans réécrire la requête ; pour T35, la ligne « 25 ans et plus » ne sort jamais | DCP (codes) ; Annuaire CIPRES et recueil v7 (grilles) |
| Le cliché (valeur donnée au lancement, `:1`, pas un réglage de `parametres`) | t32_t41 | Le cliché présent dans l'entrepôt (copie de la base du 02/02/2026) | Le cliché de fin mars N+1 (R13) | Sur la base d'aujourd'hui, environ 1 050 veuves et 1 500 orphelins de moins que l'annuaire en 2024 (décès et radiations saisis tard) | DPC (date d'arrêt de l'annuaire, Q3), puis BI (calendrier du job) |

---

## 6. Les questions ouvertes

Chaque question a une valeur par défaut dans la requête : la réponse change un réglage, pas le travail. Les destinataires suivent le recueil v7 : la DCP (Direction centrale des prestations) pour les pensions ; la DP et les DR (directions régionales) pour les rentes d'accident (DP : sigle du recueil, à faire préciser, DCP ou DPASS) ; la DES (sigle du recueil, ou la direction qui l'a remplacée) et les DR pour les prestations familiales. Les autres sigles viennent de l'annuaire CNSS 2024 : DSI (Direction des systèmes d'information), DPC (Direction de la prospective et de la coopération : son service Études et Statistique fait l'annuaire), DAI (Direction de l'audit interne), DPCG (Direction de la planification et du contrôle de gestion), DPASS (Direction de la prévention et de l'action sanitaire et sociale). Les DR : DRO Ouagadougou, DRB Bobo-Dioulasso, DRN Ouahigouya, DRF Fada N'Gourma, DRD Dédougou.

Ne sont plus posées, car tranchées (voir les règles) : la retraite anticipée dans T32 (N4, par l'annuaire) ; la ligne des dates de naissance par défaut (R8, par l'annuaire) ; la limite d'âge des orphelins de T35 (R6, par le recueil et l'annuaire) ; qui est l'allocataire (N2, par le recueil) ; la limite de 21 ans des enfants et l'allocation prénatale seule (R6 pour T41, par l'annuaire) ; le tableau des enfants (N6, par Kaya 2017) ; l'argent des accessoires dans le montant de T32 (N13, par l'annuaire : points_a_valider n° 12).

### Q1 — Le montant d'un tableau est-il ce qui a été payé dans l'année aux personnes comptées au 31 décembre, ou tout ce qui a été payé dans l'année, y compris aux personnes sorties en cours d'année ?
**À qui :** DCP (pensions) ; DES et DR (T41) ; DP et DR (rentes).
**En attendant :** payé aux personnes comptées (`montant` = COMPTES).
**Pourquoi on la pose :** T32 2024 : 37,67 Md aux comptés, 38,63 Md en tout, 38,24 Md publiés. L'annuaire ne tranche pas : il tombe entre nos deux montants pour la vieillesse et l'invalidité, sous nos montants pour les survivants, au-dessus pour les allocations familiales. Avec TOUT, l'argent des sortis irait sur une ligne à part.
**Déjà posée :** points_a_valider n° 35 ; fiche T32 Q3 ; fiche T41 Q3 ; README.

### Q2 — Dans votre annuaire, un paiement est-il rangé dans le mois et l'année qu'il paie, dans le mois où il est saisi (rappels compris), ou dans le mois où il est versé ?
**À qui :** DPC (Service études et statistique), avec la DCP.
**En attendant :** l'année du mois payé (`date_montant` = MOIS).
**Pourquoi on la pose :** la date de saisie retombe mieux pour 4 groupes sur 6 (invalidité −0,4 % contre +2,2 % ; rentes de survivants −12,5 % contre −25,1 %), le mois payé pour la vieillesse (+0,7 % contre +1,5 %) et pour les rentes viagères (−12,2 % contre −12,6 %). Le choix change les petits tableaux jusqu'à +29 % et l'effectif de T41. Les montants mois par mois de l'annuaire (tableaux 28, 41, 90) peuvent trancher sans attendre.
**Déjà posée :** au point du 02/11/2026, sans réponse ; points_a_valider n° 1 ; README, R4. Même question que T28-T31 Q24, reposée ici sans changement (une seule réponse vaut pour tout le domaine).

### Q3 — Votre annuaire de l'année N est-il fait avec la base arrêtée vers la fin mars de l'année N+1 ?
**À qui :** DPC (Service études et statistique).
**En attendant :** proposition R13 ; aujourd'hui, le cliché du 02/02/2026.
**Pourquoi on la pose :** les décès de retraités de 2024 que la base connaissait sans doute fin mars 2025 retombent sur le tableau 26 (444, indice, pour 447) ; la base d'aujourd'hui en compte 619. L'annuaire est publié en juin 2025, après un atelier.
**Déjà posée :** points_a_valider n° 2 ; README, R13.

### Q4 — Les décès des veuves pensionnées, ou l'arrêt de leur pension, sont-ils souvent enregistrés plusieurs mois après ?
**À qui :** DCP, avec les DR de Bobo-Dioulasso et de Ouagadougou.
**En attendant :** R6 inchangée ; R13 proposée.
**Pourquoi on la pose :** fin 2024, il nous manque 1 079 veuves (20 359 pour 21 438), alors que les veufs sont au-dessus (356 pour 329). Elles ont presque toutes 55 ans ou plus (tableau 20) et sont suivies à la DRB (−616) et à la DRO (−379) (tableau 22). Les suspensions (250) n'expliquent pas l'écart.
**Déjà posée :** points_a_valider n° 37 ; README ; requêtes A06 et A07.

### Q5 — Un enfant dont l'allocation court au 31 décembre, mais qui n'a rien touché dans l'année, compte-t-il dans T41 ?
**À qui :** DES et DR.
**En attendant :** non : seuls les enfants payés comptent (`t41_paiement` = 12_MOIS, l'année civile au 31/12).
**Pourquoi on la pose :** c'est la seule lecture qui retombe sur l'annuaire en 2024 (98 048 allocataires pour 97 378 ; en comptant ces enfants, 102 643). Mais de 2020 à 2023, nos allocataires dépassent le publié de 5 % à 11 % avec la même règle.
**Déjà posée :** points_a_valider n° 38 ; fiche T41 Q1.

### Q6 — Pour T41 à la fin d'un mois ou d'un trimestre, un enfant compte-t-il s'il a été payé dans les 12 mois qui précèdent ?
**À qui :** DES et DR.
**En attendant :** oui, 12 mois avant la date d'arrêté (N10).
**Pourquoi on la pose :** le recueil veut T41 au mois et au trimestre (l. 153). Une partie des allocations est payée en retard, par rappel, quand un certificat manque : exiger un paiement dans le mois ferait chuter l'effectif.
**Déjà posée :** nouvelle.

### Q7 — Pour un tableau du mois ou du trimestre, le montant est-il celui de la période seule, ou le cumul depuis le 1er janvier ?
**À qui :** DCP, DES, DP.
**En attendant :** le montant de la période ; l'année est la somme des mois (`montant_periode` = PERIODE).
**Pourquoi on la pose :** le recueil demande les dix tableaux au mois (sauf T39 et T40, non cités) sans dire quel montant. Vos tableaux mensuels (28, 41, 90) publient le montant de chaque mois.
**Déjà posée :** nouvelle.

### Q8 — Si le titulaire d'un dossier d'allocations familiales est décédé et que ses enfants touchent encore, qui est l'allocataire dans T41 : le titulaire décédé, ou la personne qui touche les allocations pour les enfants ?
**À qui :** DES et DR.
**En attendant :** le titulaire, compté à l'âge qu'il aurait (année moins année de naissance).
**Pourquoi on la pose :** le recueil (l. 152) dit « titulaire du dossier (assuré) » ; le glossaire CIPRES compterait plutôt la personne qui a droit aux allocations « du chef » de l'assuré. Le nombre de dossiers dans ce cas n'est pas encore mesuré.
**Déjà posée :** nouvelle.

### Q9 — Le montant des allocations familiales de votre annuaire (8,24 Md en 2024) comprend-il des allocations validées mais pas encore versées ?
**À qui :** DPC, avec la DES.
**En attendant :** les allocations payées (débours au statut D ou P) des enfants comptés.
**Pourquoi on la pose :** chez nous, 7,69 Md pour les enfants comptés (−6,7 %), 7,95 Md pour tout le payé (−3,4 %), 8,00 Md à la saisie (−2,9 %), alors que l'effectif retombe à 0,7 % près. Le montant publié est un multiple de 500 FCFA, comme les allocations, alors que nos rappels par ajustement ne le sont pas : il ne contient sans doute que des allocations.
**Déjà posée :** points_a_valider n° 40 ; fiche T41 Q3.

### Q10 — Dans vos tableaux par âge (19, 25, 36), l'âge est-il l'année de l'exercice moins l'année de naissance ?
**À qui :** DPC (Service études et statistique).
**En attendant :** oui, comme le glossaire CIPRES (R8).
**Pourquoi on la pose :** T32 a plus de jeunes retraités (55-59 ans : 3 778 pour 3 399) et moins d'âgés (65-89 ans : −261) que votre tableau 19 ; les nouveaux retraités montrent le même décalage (60 ans : 377 pour 519). Si vous preniez l'âge atteint l'année de la publication, c'est exactement ce qu'on verrait.
**Déjà posée :** nouvelle (liée à points_a_valider n° 44).

### Q11 — Votre tableau des enfants d'allocataires (tableau 36) compte-t-il aussi les enfants sortis dans l'année parce qu'ils ont atteint 21 ans ?
**À qui :** DPC, avec la DES.
**En attendant :** enfants en cours au 31/12 et payés dans l'année.
**Pourquoi on la pose :** notre total retombe (+1,2 %), pas la forme : 19 % de moins de 5 ans en plus et 32 % de 20-21 ans en moins. Vos 20-21 ans touchent en moyenne 18 177 FCFA dans l'année, contre 28 000 à 34 000 FCFA pour les autres tranches : comme s'ils n'avaient été payés qu'une partie de l'année.
**Déjà posée :** nouvelle (liée à points_a_valider n° 42 et à la fiche T41 Q5).

### Q12 — Jusqu'à quel âge une rente d'orphelin d'accident du travail est-elle versée ?
**À qui :** DP et DR.
**En attendant :** comme les pensions, moins de 25 ans (`age_max_orphelin` = 25).
**Pourquoi on la pose :** l'annuaire compte 485 rentes d'orphelins en 2024, entre nos 454 (moins de 25 ans) et 531 (sans limite). De 2021 à 2023, ses chiffres (421, 399, 379) sont bien sous les nôtres ; en 2024, ils montent de 106 pour 44 nouvelles rentes. Ni le recueil ni Kaya 2017 ne parlent de T39.
**Déjà posée :** nouvelle.

### Q13 — T39 et T40 n'ont aucune ligne dans le recueil : les produit-on comme T38, avec le mot « rentiers » et une sortie au mois, au trimestre et à l'année ?
**À qui :** DP et DR.
**En attendant :** oui (N1, N9).
**Pourquoi on la pose :** T39, T40, T26 et T56 n'ont aucun commentaire dans le recueil, et Kaya 2017 ne les examinait pas. Pour T39, la maquette CIPRES garde la ligne « 25 ans et plus », qui restera vide tant que la limite de 25 ans s'applique (Q12).
**Déjà posée :** remarque B38 du recueil ; même cas que T26 (T25-T27 Q12).

### Q14 — Pour T38, « le cumul sans préciser un exercice » veut-il dire tous les rentiers en cours à la date choisie, quelle que soit l'année où leur rente a commencé ?
**À qui :** DP et DR.
**En attendant :** oui, un stock à la date d'arrêté (R6).
**Pourquoi on la pose :** la phrase se lit aussi comme un montant cumulé depuis le début de chaque rente. Le recueil demande au contraire pour T45 « la période (début, fin) en paramètre » (l. 203), et votre tableau 78 publie l'« ensemble au 31-12 » : deux appuis à la lecture « stock ».
**Déjà posée :** remarque B28 du recueil.

### Q15 — En attendant le module de conversion, gardez-vous dans T33 les invalides qui ont passé l'âge de la retraite, ou faut-il les montrer à part ?
**À qui :** DCP.
**En attendant :** gardés dans T33 (`invalides_age_retraite` = DANS_T33), comme votre tableau 21.
**Pourquoi on la pose :** la loi et le recueil (l. 270) voudraient les voir en vieillesse ; 72 des 125 invalides de 2024 ont 60 ans et plus.
**Déjà posée :** nouvelle (le module lui-même : recueil l. 270, Kaya 2017).

### Q16 — Faut-il garder dans la pension moyenne les pensions de coordination que paie l'autre caisse (la CNSS ne leur verse rien) ?
**À qui :** DCP.
**En attendant :** oui : montant ÷ effectif total (`moyenne_coordination` = TOUTES), pour T32, T34 et T35.
**Pourquoi on la pose :** 2 901 pensions de vieillesse dans ce cas fin 2025 : la moyenne serait 9,7 % plus haute sans elles. La maquette CIPRES n'a pas de colonne de moyenne : qui divise le montant par l'effectif les garde, comme quand on divise les chiffres de votre tableau 19.
**Déjà posée :** points_a_valider n° 36 ; fiche T32 Q5.

### Q17 — Pour le rapport démographique, une pension de coordination payée par l'autre caisse compte-t-elle comme un retraité de la CNSS ?
**À qui :** DCP.
**En attendant :** oui (`coordination_retraites` = AVEC).
**Pourquoi on la pose :** la CNSS ne leur verse rien, mais elles sont dans votre effectif (tableau 18) ; elles font près de 9 % des retraités.
**Déjà posée :** nouvelle.

### Q18 — Le taux de remplacement porte-t-il sur toutes les pensions directes en cours, ou seulement sur les pensions nouvelles de l'année ?
**À qui :** DCP.
**En attendant :** toutes les pensions directes en cours à la date d'arrêté (`remplacement_population` = STOCK).
**Pourquoi on la pose :** le recueil (l. 1256 à 1265) ne le dit pas. Sur les nouvelles pensions, l'indicateur suivrait mieux les règles de calcul de l'année.
**Déjà posée :** nouvelle (relevé du recueil).

### Q19 — Le salaire moyen enregistré avec chaque pension dans l'application (`PRESTATION_ESP.PE_SAL_MOYEN`) est-il bien le salaire moyen des cinq meilleures années ?
**À qui :** DSI, avec la DCP.
**En attendant :** pas d'indicateur.
**Pourquoi on la pose :** le recueil définit ainsi le dernier revenu d'activité (l. 1260). Le champ n'a jamais été profilé.
**Déjà posée :** nouvelle (remarque B37 du recueil).

### Q20 — L'agence d'un bénéficiaire est-elle bien le service de son dossier dans l'application (`SP_NO`) ?
**À qui :** DSI.
**En attendant :** service du dossier (`agence` = SERVICE) ; la DR du dossier est déjà confirmée.
**Pourquoi on la pose :** le rapport démographique et le taux de remplacement sont demandés par agence. La base a 20 services et 350 lieux de paiement (P46, P47).
**Déjà posée :** T25-T27 Q20 (une seule réponse vaut pour les deux familles) ; remarque B34 du recueil ; voisine de points_a_valider n° 67.

### Q21 — Où sont enregistrés les paiements de rentes d'accident du travail qui manquent dans la base des prestations ?
**À qui :** DSI, avec la direction financière.
**En attendant :** les paiements de la base (table `DEBOURS`) ; montants des rentes présentés avec une réserve.
**Pourquoi on la pose :** 2024 : rentes viagères 399 millions dans la base pour 455 publiés (−12 %) ; rentes de survivants 276 millions au mois payé et 323 à la saisie pour 369 publiés (−25 % et −12,5 %).
**Déjà posée :** points_a_valider n° 51 ; README (questions DSI).

### Q22 — Un contrôle d'existence est-il prévu pour les pensionnés très âgés et pour les pensions encore actives dont le titulaire est décédé ?
**À qui :** DCP.
**En attendant :** comptés tant que la pension est en cours et que la personne n'est pas déclarée décédée.
**Pourquoi on la pose :** fin 2025, 69 pensionnés payés ont 100 ans ou plus et 215 pensions sont actives alors que leur titulaire est décédé (P25, P26). En 2024 : 74 retraités de 100 ans et plus chez nous, 56 dans l'annuaire.
**Déjà posée :** points_a_valider n° 43 ; fiche T32 Q4.

### Q23 — Une pension dont le dossier a été transféré à une autre caisse compte-t-elle encore parmi vos bénéficiaires au 31 décembre ?
**À qui :** DCP, avec la DSI.
**En attendant :** comptée (`dossier_transfere` = GARDER).
**Pourquoi on la pose :** fin 2025, 34 pensions de T32 (2 payées) et 79 de T34 sont dans des dossiers transférés (catégorie `CNSS_AC`). Pour les dossiers de T25 à T27, on les retire (T25-T27 D1).
**Déjà posée :** nouvelle (à joindre à la question DSI sur les catégories de dossier du README).

---

## 7. Les points d'attention

1. **« Idem. Tab 41 » sur T32 à T38.** On en garde la périodicité. La définition de l'allocataire (titulaire du dossier) est sans effet pour T32, T33 et T37 (le titulaire est le bénéficiaire lui-même, à vérifier) et ne s'applique pas à T34, T35, T36 et T38 : on compterait l'assuré décédé au lieu de la veuve ou de l'orphelin. T39 et T40 n'ont pas d'« Idem ». La remarque B05 du recueil (`remarques_besoins_v7`) étend la définition à T37 et T38 sans dire qu'elle ne vaut pas pour un survivant, et oublie T32 à T36 : si on la reprend, la corriger dans le sens de ce point.
2. **On ne retombera jamais à l'unité sur l'annuaire.** Notre base est une copie du 02/02/2026 ; la CNSS a compté sur sa base du printemps 2025, complétée par des rapports et des collectes en direction, et ses montants sont alignés sur la comptabilité (démarche, p. xiii et xiv).
3. **Les années 2020 à 2023.** T32 y tombe à 0,5 % près, mais T41 (+5 % à +11 %), T36 et T40 (+9 % à +22 %) non, sans explication prouvée. On ne garde pas les années qui arrangent : une règle « confirmée » seulement par 2024 est dite comme telle.
4. **Les chiffres de la CNSS ne sont pas toujours cohérents entre eux.** En 2024, aucun orphelin ne sort (pensions et rentes) et 89 ascendants décèdent pour 54 pensionnés. On ne corrige pas une règle sur une seule ligne publiée.
5. **Personnes ou pensions.** Les comparaisons A01 et A02 comptent des pensions (31 267 pour T32 en 2024), la requête des personnes (31 093) : 174 personnes ont deux pensions en cours. On n'additionne jamais deux tableaux pour compter des personnes (519 retraités sont aussi dans un autre tableau fin 2025). Le total de 62 231 de l'annuaire additionne des pensions.
6. **Un stock ne s'additionne pas d'un mois à l'autre :** avant de passer au mois, l'effectif doit être réglé en « dernière valeur » dans OBIEE, comme pour l'effectif GRH. Les montants, eux, s'additionnent.
7. **R13 et le cliché unique.** Si les faits des prestations ne gardent qu'un cliché, comme ceux de la GRH (vidés à chaque chargement), il faudra produire T32 à T41 fin mars et garder le résultat dans le DTM ; recalculer plus tard donnera moins de veuves et d'orphelins. À vérifier.
8. **La requête n'a jamais été exécutée, et sa table du DTM n'existe pas.** Tous nos chiffres viennent des lots 5 à 8. La requête demande encore l'exercice au lancement (`:2`), alors que, comme pour la GRH, le cliché `:1` doit être la seule valeur donnée au lancement et les choix dans la CTE `parametres`. `DTM_PRESTATION` ne peut pas porter ces tableaux : elle compte des personnes payées par mois (pas un stock), prend la personne du titre de paiement (le tuteur pour un orphelin) et une seule grille d'âge (hypothèses H8, H9, H13 de `regles_prestations` et de `reference_cipres_T25_T57.md`). Le modèle ne se refond pas sans visa : il faut une évolution compatible, à choisir avec Jules.
9. **Qualité des données à signaler à la CNSS, pas à corriger dans la BI :** 503 dates de naissance par défaut (1900) en 2024 ; 17 enfants nés après l'exercice ; 215 pensions actives de titulaires décédés (2025) ; 5 774 pensions d'orphelins de 25 ans et plus jamais radiées, presque jamais payées (fin 2024) : faut-il les radier ? À signaler à la DCP (points_a_valider n° 10, partie radiation) ; elles ne changent pas T35, qui coupe à 25 ans. Le README note 8 447 pensions d'orphelin actives sans paiement au 4e trimestre 2025. Kaya 2017 demandait déjà une saisie correcte des données (l. 185, 193).
10. **Les montants des rentes d'accident (T37 à T40)** sont de 12 à 25 % sous le publié, quelle que soit la date : à présenter avec une réserve tant que la DSI n'a pas répondu (Q21).
11. **Le montant de T41** est de 3 à 7 % sous le publié alors que l'effectif est juste : les questions du montant (Q1, Q9) et de la date (Q2) pèsent ici plus qu'ailleurs.
12. **`date_montant` change aussi l'effectif de T41.** Dans la requête, les paiements filtrés par cette date servent de condition de paiement pour T41. Changer le réglage change le nombre d'allocataires et d'enfants : jamais mesuré (P79 n'utilise que le mois payé).
13. **Les grilles de la CNSS ne sont pas toutes celles de la CIPRES :** T32 (tableau 19) et T33 (tableau 21) sont identiques ; le tableau 20 mélange survivants et orphelins sur une grille fine ; le tableau 76 s'arrête à « 90 ans et + » ; la CNSS ne publie pas l'âge des allocataires. On compare en regroupant les tranches.
14. **L'âge n'est pas calculé comme en GRH :** année moins année de naissance pour les prestations (glossaire CIPRES), années pleines pour le personnel. C'est voulu. La CNSS calcule peut-être l'âge à une autre date (Q10).
15. **T35 et T39 et la ligne « 25 ans et plus » :** pour T35, elle ne sort jamais, car la requête ne fabrique pas de ligne vide ; si OBIEE affiche une grille fixe, il faut la retirer de la grille de T35. Pour T39, la maquette CIPRES la garde : elle restera vide tant que la limite de 25 ans s'applique.
16. **Données personnelles :** on ne lit jamais les noms. La vue des décédés de la DSI (`V_PRESTATAIRES_DECEDES_BI`) en contient et n'est pas utilisée ; le classeur des ratios contient des noms et des numéros d'assurés : seuls ses titres, colonnes et normes ont été lus. La liste des bénéficiaires (N12) n'a ni nom ni numéro, et son accès suit le profil (recueil l. 72).
17. **Les documents existants, laissés tels quels :** N13 sort l'argent des accessoires du montant de T32, alors que le README, points_a_valider n° 12 et la carte A1 de `regles_prestations` disaient qu'il suit la pension (A1 garde ici son sens : pas un pensionné de plus) ; la requête et la fiche T32 mettent la retraite anticipée dans les tranches de T32 (N4 la met à part) ; le README (R5) compte un allocataire par dossier (N2 : par titulaire) ; le README (R8) et la requête gardent la ligne « âge inconnu » (R8 proposée : « 100 ans et plus »). Points_a_valider n° 9, 12, 34, 39, 41 et 42 et fiche T41 Q2 et Q4 peuvent être clos ; le n° 10 se réduit à la radiation.
18. **Deux familles de remarques B.** Les remarques B01 à B41 du recueil (`remarques_besoins_v7`) n'ont rien à voir avec les cartes B1 à B25 de `regles_prestations` : ce document écrit « remarque Bnn du recueil » ou « carte Bn de `regles_prestations` ».

---

## 8. Comment c'est vérifié

- **Sources relues pour ce document :** `besoins_v7.txt`, l. 1 à 82, 112 à 153, 185 à 205, 245 à 298, 1083 à 1092, 1127 à 1142, 1244 à 1268 ; `kaya.txt`, l. 45 à 60, 110 à 125, 150 à 199, 281 à 330, 995 à 1015 ; `annuaire_cipres.txt`, glossaire (l. 2838 à 2845) et descriptions des tableaux 32 à 41 ; `annuaire_cnss_2024.txt`, tableau 28 (l. 1784 à 1830), tableau 40 (l. 2333 à 2350), tableau 90 (l. 4078 à 4097), démarche (l. 630 à 650) ; en-tête et CTE `parametres` de `t32_t41_requete_proposee.sql` ; README (règles R2 à R13) ; titres des points_a_valider cités ; questions des fiches T32 et T41.
- **Recalculs :** tous les écarts de la partie 4 (pourcentages refaits sur les chiffres) ; −10,2 % (27 670 face à 30 808) ; la moyenne avec et sans la coordination (2 901 pour 32 840 : 8,8 % plus basse avec, 9,7 % plus haute sans) ; la bonification (+0,4 % à la saisie, +3,1 % au mois payé) ; veuves et veufs séparés (−1 079 et +27) ; les historiques de T36, T40 et T41 ; les sorties d'orphelins de la base ; le profil mensuel des survivants (tableau 28 : 396 à 424 millions en janvier, avril, juillet, octobre) et des rentes viagères (tableau 90 : 22 à 25 millions ces mois-là) relu dans le texte de l'annuaire. Les chiffres des lots viennent du dossier de la famille, recalculés par sa contre-vérification (sommes, totaux et écarts justes).
- **Lots utilisés :** 1 (P16), 2 (P25, P26), 3 (P34, P46, P47), 5 (P74 à P78), 6 (P79 à P81), 7 (P82, P83, P87), 8 (P96 à P99) ; comparaisons A01 et A02. Aucune requête nouvelle n'a été lancée ; aucune table DTM n'a été lue.
- **Corrections reprises de la contre-vérification :**
  - *erreur* : le sens des montants des survivants et des allocations familiales, inversé dans le dossier (R3, partie 4, Q1) ; T41 et les ascendants « confirmés » sur la seule année 2024 : lignes séparées, 2020 à 2023 en « écart à expliquer », et l'explication « base qui a beaucoup changé » remplacée par « non expliqué ; P79 écarte les allocations saisies après coup » (R6 pour T41, partie 4, point 3) ; l'effet de `date_montant` (de −3,5 % à +29 %, et sur l'effectif de T41 : R4, réglages, point 12) ; N8 avec T42 à T49 et des sorties d'orphelins de 900 à 1 900 par an.
  - *manque* : trois questions ajoutées (Q15 invalides après l'âge de la retraite ; Q13 « rentiers » et périodicité pour T39 et T40, réunies en une question) ; le reste à faire complété (âge et sexe des allocataires, séries mensuelles pour R4, vieillesse seule par âge, retraites anticipées en personnes, titulaires de T41 avec la condition de paiement, effet de `date_montant` sur T41, montants par DR) ; la ligne « sortis dans l'année » pour `montant` = TOUT (R3) ; la coordination dans le rapport démographique (N7, Q17, réglage `coordination_retraites`) ; les dossiers transférés `CNSS_AC` (N11, Q23).
  - *imprécision* : R8 « grilles confirmées ; date de l'âge à vérifier » (P83, tableau 25, Q10) ; personnes et pensions (R5 prouvée par 2024 : 31 093 personnes, 31 267 pensions ; A01 et A02 dits « pensions ») ; glossaire CIPRES cité en entier pour l'allocataire (N2, Q8) ; −6 allocataires « sans la condition de paiement, à remesurer » ; retraite anticipée à part par défaut (N4) ; « Idem. Tab 41 » sans effet pour T32, T33, T37 et inapplicable pour T34, T35, T36, T38 (point 1 ; R5 marquée « Proposition BI ») ; R13 « 444, indice ; 404 avec la fiche de la personne » ; veuves et veufs séparés, « décès ou radiations » (Q4) ; A1 : changement signalé (désormais porté par N13), RRC sorti des accessoires (à vérifier), deux dates pour la bonification ; feuille 05 sans apport direct et feuille 03 en trois blocs ; −10,2 % face à l'annuaire et cause de 2024 « à vérifier par A06 » ; moyenne de la coordination 9,7 % et 8,8 %, aussi pour T34 et T35 ; le montant d'un tableau mensuel (N1, Q7, `montant_periode`) ; tableau 19 comparé avec le périmètre dit (vieillesse seule à refaire), femmes de 55 à 64 ans liées au n° 44 ; « cumul » de T38 avec les deux lectures et le contraste avec T45 (Q14) ; « DRN, Ouahigouya » ; « 2 232, avec les 485 rentes d'orphelins » ; « rien reçu de la CNSS » ; « au 4e trimestre 2025 » ; « laisse penser que ».
  - *demandes du recueil oubliées*, toutes reprises en partie 1 : l. 72 (contrôle par profil, N12), l. 1085 à 1091 (virement), l. 1129 à 1141 avec la note l. 1133 (prestations ordonnancées = notifiées, versée à Q1 et Q2).
  - *tableaux de l'annuaire oubliés*, repris en partie 4 : tableaux 28 à 33, 90 à 95, 40 à 46, 34 et 96 par DR, 25, 106 et 97.
- **Points où ce document va plus loin que le dossier et sa contre-vérification :**
  - **trois questions du dossier sont dites tranchées**, selon la hiérarchie des sources : la retraite anticipée (le recueil, Kaya 2017 et l'Annuaire CIPRES se taisent pour T32 ; l'annuaire CNSS la met à part partout) ; la ligne des dates par défaut (tableau 20 et maquette CIPRES sans « âge inconnu ») ; l'allocation prénatale seule (les seules allocations familiales retrouvent les allocataires publiés). La contre-vérification proposait pour la première « A_PART ou une explication » : on retient une ligne à part sous T32, pour garder le total CIPRES d'origine ;
  - **deux points du dossier ne sont plus des questions à la CNSS** : la radiation des orphelins de 25 ans et plus ne change aucun chiffre de T35, elle devient un signalement (point 9) ; le choix de la table du DTM revient à l'équipe BI (point 8, partie 9) ;
  - **`periodicite` vaut MENSUELLE par défaut** (le dossier gardait ANNUELLE) : un réglage suit le recueil, qui demande les trois fréquences ;
  - **l'agence vaut le service du dossier par défaut**, comme pour T25 à T27 (le dossier n'en mettait pas) ; Q20 renvoie à T25-T27 Q20 ;
  - **les destinataires** suivent la règle du pilote (DCP, DES et DR, DP et DR) au lieu de « direction des prestations » ;
  - **le taux de remplacement** : la question « stock ou nouvelles pensions » (Q18), relevée dans le recueil, est ajoutée avec son réglage.
- **Un chiffre qui semble différer du pilote, mais non :** pour la DRD (tableau 22), ce document compte les veuves et les veufs ensemble (563 pour 636, −73, soit −11,5 %) ; le document T25 à T27 compte les veuves seules (552 pour 625, −11,7 %, comparaison A02). Les deux sont justes ; rien n'a été changé dans le pilote.
- **Relecture (exactitude et clarté), 08/11/2026.** Toutes les lignes du recueil sur T32 à T41 ont été relevées à nouveau (grep dans `besoins_v7.txt`, demandes F3 et transverses du relevé, REPRISE parties 5 et 6, demandes oubliées de la contre-vérification) : les 29 lignes sont dans la partie 1 et dans le schéma, avec la bonne ligne. Les 26 corrections de la contre-vérification sont appliquées. Les chiffres ont été retrouvés dans `annuaire_cnss_2024.txt` (tableaux 18 à 26, 28, 34, 36 à 38, 40, 76 à 81, 90, 106) et dans les fichiers des lots (P74 à P83, P97 à P99, A01, A02), et les écarts recalculés : ils sont justes. Corrections faites :
  - **A1 garde son sens** (un accessoire ne fait pas un pensionné de plus, carte A1 de `regles_prestations`). La partie « montant hors de T32 », qui allait contre ce sens, devient une règle nouvelle, **N13**, tranchée par l'annuaire (tableaux 19, 28, 34) ; points_a_valider n° 12 s'ajoute à ceux qui peuvent être clos. Le doute sur RRC reste dans l'état de A1 ;
  - **références** : H8, H9, H13 et A1 ne sont pas dans le README mais dans `regles_prestations` (et `reference_cipres_T25_T57.md` pour les hypothèses) ; R7 (nouveaux pensionnés) n'est pas de cette famille ; les trois branches sont R1 du README ; colonnes « Priorité » l. 120 et 121 (et non 118 et 119) ; dénominateur de T28 à T30, l. 134 (et non 135) ;
  - **un sens à rectifier** : pour l'invalidité, le montant le plus proche de l'annuaire est tout le payé à la saisie (−0,4 %), pas le payé aux comptés (−1,4 %) (partie 4, montant de T32). Q2 cite aussi les rentes viagères, où le mois payé fait mieux (2 groupes sur 6 en tout) ;
  - **un chiffre daté** : les 2 901 pensions de coordination payées ailleurs sont de fin 2025 ; dans le cas 2 (année 2024), elles sont dites comme telles ;
  - **la remarque sur le pilote** (DRD −11,5 % ici, −11,7 % dans T25-T27) : les deux sont justes, l'un compte veuves et veufs, l'autre les veuves seules ;
  - **clarté** : l'« En bref » cite N13 et le module des invalides (N5), dit que la limite de 25 ans des orphelins vient du recueil, et explique A06 à A08 et le DTM ; « Les mots de ce document » expliquent A01 à A08, Md, M, OBIEE et DTM ; N3 dit que la DR des dix tableaux est une proposition BI (le recueil ne la demande que pour les indicateurs) ; la sortie « 351 » de la partie 4 est dite « chez la CNSS » ; le point 1 de la partie 7 dit ce qu'il faut corriger dans la remarque B05.
  Aucun statut, verdict ni chiffre n'a été changé sans la raison écrite ci-dessus. Le schéma a été mis à jour de même et régénéré (0 avertissement).
- **Un choix de forme :** quand l'annuaire confirme 2024 mais pas 2020 à 2023, la comparaison est coupée en deux lignes (« confirme » pour 2024, « écart à expliquer » pour l'historique), plutôt qu'un seul verdict. La ligne d'ensemble « ce qui n'est pas publié » porte le verdict « pas publié ».
- **Critique d'exhaustivité et de cohérence entre les cinq familles, 08/11/2026.** Le recueil v7 (l. 115 à 335 et indicateurs l. 1018 à 1378) a été relu à nouveau face aux cinq documents de famille, à la liste du recueil (`besoins_v7_prestations.md`) et au document sur l'annuaire (`annuaire_cnss_2024_prestations.md`). Changements ici : deux lignes ajoutées à la partie 1, qui en compte maintenant 31 : T39 et T40 sans ligne dans le recueil (question Q13), comme T26 et T56 dans leurs familles ; « Idem. Tab 41 » lu « allocataire = titulaire du dossier » pour T32 à T38 (question T42-T49 Q1, posée une seule fois pour T32 à T49 ; la périodicité reste « à intégrer » sur les lignes de chaque tableau). Tableau 22 : « précise » au lieu de « confirme », comme les familles T25-T27 et T42-T49 pour la même comparaison ; N3, « En clair » et le schéma suivent. Q2 prend la formulation de T28-T31 Q24 (le mois payé, la saisie ou le versement) et dit qu'elle en est la reprise. La ligne l. 152 cite Q8. « Ce qu'il tranche » dit pourquoi la limite de 21 ans est tranchée alors que la carte N6 est en écart. L'« En bref » n'emploie plus « CTE » ni « DTM ». La satisfaction et le document de référence sont rangés dans la famille T50-T57.

---

## 9. Ce qui reste à faire

| Tâche | Qui | État |
|---|---|---|
| Exécuter `t32_t41_requete_proposee.sql` sur le cliché présent, exercice 2024. Attendu : T32 31 093 personnes et 37,67 Md, T34 20 715, T35 7 970, T37 1 236, T41 98 048 allocataires | toi (exécution), puis Claude | à faire |
| Exécuter A06, A07 et A08 (comparaison avec l'annuaire) : base vue à la publication, dates de saisie des radiations, orphelins par âge | toi | à faire |
| Lot de contrôle à rédiger : décès des veuves (T34) survenus avant le 31/12/2024 mais saisis après mars 2025, par tranche et par DR (R13, Q4) | Claude, puis toi | à faire |
| Même lot : montants mois par mois de 2024, au mois payé et à la saisie, face aux tableaux 28 à 33, 41 à 46, 90 à 95 (pour trancher R4) ; montants par DR face aux tableaux 34, 40 et 96 ; montants de 2020 à 2023 face aux tableaux 106 et 97 | Claude, puis toi | à faire |
| Même lot : âge et sexe des allocataires de T41 (jamais mesurés, non publiés) ; titulaires distincts avec la condition de paiement (N2) ; effet de `date_montant` sur l'effectif de T41 | Claude, puis toi | à faire |
| Même lot : enfants payés en 2024 par tranche du tableau 36, avec le montant, avec et sans « en cours au 31/12 » ; allocataires par DR face au tableau 37 ; allocataires de 2020 à 2023 payés par des paiements saisis avant le 30/06 de l'année suivante | Claude, puis toi | à faire |
| Même lot : montant des allocations familiales 2024 par statut de paiement et par date, face aux 8 236 741 500 FCFA (Q9) | Claude, puis toi | à faire |
| Même lot : T32 en vieillesse seule (RPV) par âge, avec l'âge 2024 et l'âge 2025, face au tableau 19 (Q10) ; retraites anticipées en personnes (N4) | Claude, puis toi | à faire |
| Même lot : rentes d'orphelins (T39) par âge de 2020 à 2024, limite à 21 et à 25 ans, face au tableau 80 ; orphelins (pensions et rentes) par DR face aux tableaux 22 et 79 | Claude, puis toi | à faire |
| Même lot : dossiers d'allocations familiales comptés dont le titulaire est décédé (Q8) ; pensions de coordination et dossiers transférés en 2024 (Q16, Q23) | Claude, puis toi | à faire |
| Ajouter à la CTE `parametres` les réglages « à créer » (partie 5), retirer la valeur `:2` ; ajouter DR, agence (après la DSI) et code de prestation ; la ligne « retraite anticipée » ; le tableau des enfants (N6) ; le contrôle des entrées et sorties (N8) ; la liste des bénéficiaires (N12) | Claude | proposé, à faire après visa |
| Choisir avec Jules la table du DTM des bénéficiaires au 31/12 (`DTM_PRESTATION` ne convient pas) et la « dernière valeur » dans OBIEE ; vérifier si les faits des prestations gardent un seul cliché (R13) | Claude, avec Jules | à faire |
| Poser Q1 à Q23 (DCP, DES et DR, DP et DR, DSI, DPC) ; signaler les données à corriger (point 9) | CNSS, via toi | à poser |
| Si l'utilisateur le décide : reporter ces règles dans le README, la requête, les fiches T32 et T41, la fiche de lecture, `regles_prestations` et points_a_valider (dont n° 9, 10, 12, 34, 39, 41, 42) | Claude, sur demande | non commencé (rien n'a été modifié) |
