# À faire — domaine Prestations

État au 30/10/2026. Liste des tâches que Claude reprendra une fois tes requêtes en cours terminées. Pour reprendre : donne les résultats (ou reporte-les dans les en-têtes « Résultat »), puis demande « reprends le TODO des prestations ».

## Objectifs

1. **Produire T25 à T57 depuis la source**, avec des règles validées par la direction des prestations et la DSI (règles R1 à R12 du [README](README.md)).
2. **Corriger les requêtes DTM du domaine** sans refondre le modèle : mêmes tables, évolutions rétrocompatibles, mises en production seulement après visa métier.
3. **Lever les inconnues de la base** : sens des statuts, tables pas encore profilées, cas particuliers des dossiers.
4. **Tenir les supports à jour** : artefact « Tables des prestations CNSS », schémas Excalidraw, docs pour la CNSS.

## Ordre d'attaque des tableaux (30/10/2026)

T25 est fait : requête proposée, fiche de validation, règles D1 à D8. Il sert de modèle. Les 32 autres tableaux se rangent en six familles ; dans une famille, une seule requête et les mêmes règles servent à tous les tableaux.

| Ordre | Famille | Tableaux | Moteur commun | Déjà prêt | Reste à faire |
| --- | --- | --- | --- | --- | --- |
| 1 | Dossiers et durée de liquidation | T25, T26, T27 | Requête T25 : on compte des demandes (un dossier × une ligne), règles D1 à D8 | T25, T26, T27 : requêtes proposées et fiches de validation (30/10/2026) | Faire répondre la direction des prestations aux fiches ; exécuter les trois requêtes ; aligner la DTM_DOSSIER proposée |
| 2 | Bénéficiaires au 31/12 | T32 à T41 | Une prestation active au 31/12 et payée au 4e trimestre (R1 à R6, R8) | Lot 2 : première version de T32 (29 721 pensionnés) | Requête commune ; fiche pilote T32, puis T33 à T41 ; orphelins sans paiement (8 447) ; allocataires de T41 (tâche 6) |
| 3 | Nouveaux bénéficiaires | T42 à T49 | Même requête que la famille 2, avec l'entrée : notifié dans l'exercice (R7) ; services passés (R9) ; taux d'IPP (T48) | Lot 2 (P27, P28) | Fiches ; taux d'IPP de T48 à mesurer |
| 4 | Évolution sur 5 ans | T50 à T57 | Effectifs et montants par année (R3, R4) | Règles R3 et R4 | T54 et T55 : compter les liquidées, pas les payées (Kaya 2017) ; T57 : assurés décédés déclarés (Kaya 2017) |
| 5 | Indus | T31 | Ajustements (règle I1) | P11 | Lot indus (tâche 5) ; question REM-TP |
| 6 | Contrôles a posteriori | T28 à T30 | Contrôles saisis (règle C1) | P06, P13 | Bloqué hors BI : plus de saisie depuis juillet 2025, réponse DSI attendue ; Kaya 2017 : rapports manuels de la DAI |

Pourquoi cet ordre :
- La famille 1 finit ce que T25 a commencé, avec le même moteur : un seul lot de contrôles sert T26 et T27.
- La famille 2 est le cœur de l'Annuaire (10 tableaux, base des études actuarielles), et le lot 2 a déjà prouvé l'essentiel de ses règles.
- Les familles 3 et 4 reprennent le moteur de la famille 2.
- La famille 5 demande son propre lot ; la famille 6 attend une réponse de la DSI.

## Vue d'ensemble

| N° | Tâche | Objectif | Attend | Qui | État |
| --- | --- | --- | --- | --- | --- |
| 1 | Reporter les résultats de tes requêtes | 3 | tes requêtes | toi, puis Claude | en cours |
| 1 bis | Exécuter le lot 3, partie A (listes de codes, P31–P47), puis compléter le schéma | 3, 4 | — | toi, puis Claude | fait le 29/10/2026 (schéma) ; artefact : tâche 11 |
| 2 | Codes IJ et RV de DTM_ACCIDENT_TRAVAIL | 2 | — | Claude | à faire |
| 3 | Lot 3 : points à vérifier de T25 et DTM_DOSSIER | 1, 3 | 1 | Claude, puis toi (exécution) | T25 fait le 30/10/2026 (lot 3 B exécuté ; requête T25 et fiche revues) ; reste DTM_DOSSIER (frais médicaux AT/MP) |
| 4 | Lot 3 : tables pas encore mesurées | 3 | 1 | Claude, puis toi | à faire |
| 5 | Lot 3 : indus (T31) | 1, 3 | 1 | Claude, puis toi | à faire |
| 6 | Lot 3 : allocataires (T41) | 1, 3 | 1 | Claude, puis toi | à faire |
| 7 | Exécuter les requêtes T25 et DTM_DOSSIER proposées | 1, 2 | 3 | toi | à faire |
| 8 | Requête DTM_PRESTATION proposée | 2 | validation de R3 à R8 | Claude | à faire |
| 9 | Requête DTM_PRESTATION_INDUE proposée | 2 | 5 | Claude | à faire |
| 10 | Contrôles et indicateurs | 2 | réponse DSI | Claude | à faire |
| 11 | Mettre à jour l'artefact et les schémas | 4 | 2, 4 | Claude | à faire |
| 12 | Commiter le travail | — | ton accord | Claude | fait le 30/10/2026 (commit 2ab48b4) ; à refaire à chaque étape |
| 13 | Lot 4 : T26 et T27 | 1, 3 | — | Claude, puis toi (exécution) | fait le 30/10/2026 : lot exécuté (P59–P73), requêtes et fiches T26 et T27 |

Ordre proposé : 7 (exécuter T25, T26, T27), puis 6 et 8 (famille 2 : bénéficiaires au 31/12), 2, 4, 5 et 9, 10 ; 11 et 12 à chaque étape franchie. Voir « Ordre d'attaque des tableaux ».

## Détail des tâches

### 1. Reporter les résultats de tes requêtes

- **Objectif** : garder la trace de ce qui a été exécuté et de ce qu'on en tire.
- **Comment** : reporter chaque résultat dans l'en-tête « Résultat » et « Constat » de sa requête, mettre l'état du lot dans le tableau « Lots » du README, ajouter les constats nouveaux à « Constats à ce jour ».
- **Fait quand** : le README dit ce que tes requêtes ont montré, et les tâches suivantes sont revues à la lumière des résultats.

### 1 bis. Listes de codes complètes

- **Objectif** : que le schéma `tables_prestations` (partie 3) et l'artefact listent tous les codes avec leur libellé exact.
- **Comment** : exécuter [lot3_listes_de_codes.sql](lot3_listes_de_codes.sql) (P31 à P47, rapide : listes de codes et comptages sur des tables légères). Reporter les résultats dans les en-têtes, puis remplacer dans le schéma les libellés suivis de « ? » et les renvois « Pn » par les vraies listes (prestations en nature, effets, modes de paiement, directions, services, lieux de paiement).
- **Utile aussi pour** : la tâche 2 (codes de DTM_ACCIDENT_TRAVAIL, P34) et la question DSI sur les modes de paiement (P38).
- **État au 29/10/2026** : fait pour le schéma. Résultats dans les en-têtes du lot 3 et, pour les listes longues, dans [resultats/](resultats/). Le schéma `tables_prestations` liste tous les codes (partie 3) et la géographie (nouvelle partie 4). Reste l'artefact (tâche 11).

### 2. Codes IJ et RV de DTM_ACCIDENT_TRAVAIL

- **Objectif** : savoir si `MONTANT_IJ` et `MONTANT_RENTE` de `DTM_ACCIDENT_TRAVAIL` sont toujours à zéro.
- **Pourquoi** : `dtm/sql/accident_travail.sql` filtre `TPE_CODE IN ('IJ', 'RV')`, alors que la base n'a que 28 codes en espèces, dont `AIJ` (indemnités journalières) et `ARI` (rente viagère d'incapacité), et aucun `IJ` ni `RV` (P04, confirmé par la liste complète P34).
- **Comment** :
  1. Requête de contrôle (lot 3) : débours de la branche A par `TPE_CODE`, en 2025, avec le nombre et le montant ; vérifier que `IJ` et `RV` n'apparaissent pas.
  2. Proposer la correction : `AIJ` pour les indemnités journalières ; pour les rentes, `ARI` seul ou avec les rentes de survivants `ARSV`, `ARSF`, `ARSE`, `ARSA` (à trancher avec la direction des prestations).
  3. Appliquer aussi les règles R3 et R4 aux montants : débours `PM` au statut D ou P, `DEB_MONTANT`, mois de `DEB_DATE_EFFET`.
- **Livrable** : `accident_travail_requete_proposee.sql` dans ce dossier, sans toucher au pipeline tant que ce n'est pas validé.

### 3. Lot 3 : points à vérifier de T25 et DTM_DOSSIER

- **Objectif** : lever les points laissés ouverts par [t25_requete_proposee.sql](t25_requete_proposee.sql) et [dtm_dossier_requete_proposee.sql](dtm_dossier_requete_proposee.sql) avant leur mise en production.
- **Questions à vérifier dans les données** :
  - Une réception donne-t-elle un seul dossier ? Une pension de réversion ouvre-t-elle un nouveau dossier ?
  - Combien de dossiers ont plusieurs notifications (révision, réversion) ? Écart entre la première et la dernière, et avec `DOS_DATE_NOTIFICATION`.
  - Les réceptions anciennes jamais closes : combien, par année de réception ? Faut-il les sortir du stock en instance ?
  - Les dossiers de frais médicaux seuls (AT/MP) : la date d'approbation du premier remboursement peut-elle servir de date de liquidation ?
  - `RD_STATUT` (E, I, L, R, A) : répartition et lien avec l'existence d'un dossier ou d'un rejet.
- **Livrable** : partie B de `lot3_listes_de_codes.sql` (à renommer si besoin), requêtes numérotées à partir de P48, en lecture seule et en comptages. Pas de `FETCH FIRST` (base antérieure à 12c) : limiter avec `ROWNUM`.
- **État au 30/10/2026** : la partie T25 est dans [lot3b_controles_t25.sql](lot3b_controles_t25.sql) (P48 à P57, une requête ou plus par question de la fiche T25), exécutée (P52 relancée après un alias de plus de 30 caractères ; P58 ajoutée après P52). Constats dans le README (« Lot 3, partie B »). Appliqué le 30/10/2026 à [t25_requete_proposee.sql](t25_requete_proposee.sql) et à la fiche Excalidraw T25 : compter par demande (cas B de [t25_q4_deux_cas.sql](t25_q4_deux_cas.sql)), réceptions ANCIEN rattachées à la réversion, COOR_AC gardés, stock en instance corrigé, un seul dossier par réception. La DTM_DOSSIER proposée suit encore l'ancienne règle (par dossier) : à aligner après validation. Les noms de colonne restent sous 30 caractères (ORA-00972). Le point des frais médicaux AT/MP (DTM_DOSSIER) est mesuré par P63 du lot 4 (tâche 13).

### 4. Lot 3 : tables pas encore mesurées

- **Objectif** : compléter l'inventaire de P01 pour les tables décrites seulement d'après leurs colonnes.
- **Tables** : `RECEPTION_DOS_PIECE`, `PE_AJ`, `BORDEREAU_PREST`, `IND_COMPTE_BANCAIRE` (volume et dernière saisie seulement : aucune donnée bancaire en sortie).
- **À vérifier aussi** : part des effets avec un bordereau (`EFFET.BO_ID`) ; recoupement entre `PE_AJ` et `DEBOURS.AJ_ID`.
- **Fait quand** : les volumes remplacent « volume non mesuré » dans l'artefact et le schéma (tâche 11).

### 5. Lot 3 : indus (T31)

- **Objectif** : préparer les règles de T31 (trop perçus et moins perçus par branche et par mois) et le taux d'indus, dont la norme CIPRES est de 2 % au plus.
- **Questions à vérifier dans les données** :
  - Montants par mois et par branche, avec les types retenus (TP, TP-CONV, MP, RC) et sans REM-TP, AV ni MPG.
  - Sens de `AJ_STATUT` (A, C, I, R) d'après les dates et les débours liés.
  - Dénominateur du taux : montant des prestations payées du même mois (règle R3).
  - Poids de la branche M, exclue à tort par le DTM.
- **Livrable** : requêtes du lot 3 ; règles à ajouter au README.

### 6. Lot 3 : allocataires (T41)

- **Objectif** : savoir compter les allocataires au 31/12 et leurs enfants.
- **Points à vérifier** : l'allocataire est la personne payée (`IND_ID_BENEF` de `PRESTATION_ESP`, car 62 % des paiements PF de 2025 n'ont pas d'effet) ; paiement trimestriel de FAF, donc présence au 4e trimestre ; un allocataire compté une fois même avec plusieurs enfants ; 38 919 FAF actives sans paiement au 4e trimestre 2025 (P25).
- **Livrable** : requêtes du lot 3 ; règle R6 précisée pour les allocataires.

### 7. Exécuter les requêtes T25 et DTM_DOSSIER proposées

- **Objectif** : vérifier que les règles donnent des chiffres plausibles avant de les soumettre. Depuis le 30/10/2026, aussi [t26_requete_proposee.sql](t26_requete_proposee.sql) et [t27_requete_proposee.sql](t27_requete_proposee.sql) : comparer avec le lot 4 (2025 : 343 demandes AT/MP liquidées hors soins, 8 246 nouveaux allocataires, 4 273 grossesses).
- **Comment** : exécuter sur le cliché 092026 ; comparer avec P09 et P10 (2025, vieillesse : 4 671 réceptions, 4 783 notifications, médiane 62 jours) ; noter le temps d'exécution de `DTM_DOSSIER`.
- **Attend** : l'accord de Jules (Oracle Analytics) pour `DTM_DOSSIER`, dont le sens de `NB_DOSSIERS`, `NB_INSTANCE` et `ID_TEMPS` change.

### 8. Requête DTM_PRESTATION proposée

- **Objectif** : rendre `DTM_PRESTATION` juste pour T32 à T57, avec les mêmes colonnes.
- **Corrections** : montant = `DEB_MONTANT` des débours `PM` au statut D ou P, jamais `NET_A_PAYER` de l'effet (R3, montants gonflés 3,8 fois aujourd'hui) ; ajustements sortis des paiements ; mois de `DEB_DATE_EFFET` (R4) ; personne = ayant droit `IND_ID_PREST`, sauf allocataire FAF (R5) ; personne, direction et service lus sur la prestation et le dossier, pas sur l'effet ; M = maternité, rattachée aux PF ; âge selon R8 ; mois cotisés pris une fois par personne, pas additionnés à chaque paiement (R9) ; nouveau = notifié dans l'année, pas premier paiement (R7) ; décès selon la définition de T57 retenue à l'atelier de Kaya.
- **Limite du grain** : `DTM_PRESTATION` est mensuel ; il convient pour les montants, pas pour les effectifs (des comptes distincts par mois ne s'additionnent pas sur l'année). Piste à rédiger : une table annuelle des bénéficiaires pour T32 à T55, à soumettre avant toute création (pas de refonte du modèle sans visa).
- **Livrable** : `dtm_prestation_requete_proposee.sql`, sur le modèle de la requête DTM_DOSSIER proposée.

### 9. Requête DTM_PRESTATION_INDUE proposée

- **Objectif** : rendre T31 juste.
- **Corrections** : REM-TP sorti des indus ; branche M incluse ; règles de la tâche 5.

### 10. Contrôles et indicateurs

- **Objectif** : T28 à T30 et `DTM_INDICATEURS_PRESTATIONS` cohérents avec les tables corrigées.
- **Points** : ne plus compter `DEB_VERIFIE` comme un contrôle a posteriori ; savoir de la DSI où sont saisis les contrôles depuis juillet 2025 ; recalculer les indicateurs une fois les tâches 7 à 9 faites.

### 11. Mettre à jour l'artefact et les schémas

- **Artefact** : « Tables des prestations CNSS », https://claude.ai/artifact/UEtDg8Wa5xiEhGu999krDR.
- **Schémas** : `excalidraw/schemas/prestations/tables_prestations.excalidraw` et `domaine_prestations.excalidraw`. Si le fichier a été retouché dans la visionneuse, modifier le fichier lui-même plutôt que de régénérer avec `--force` (l'état écrasé resterait dans l'historique des versions, mais les retouches seraient à refaire).
- **Quand** : après les tâches 2 et 4 (volumes, codes), et à chaque réponse sur les statuts.
- **Déjà prêt pour l'artefact** : les listes complètes du lot 3 (libellés exacts des 28 codes, prestations en nature, effets, modes, étapes, ajustements, pièces, directions, services, lieux de paiement), déjà reportées dans le schéma le 29/10/2026.

### 12. Commiter le travail

- **Objectif** : ne pas perdre le travail, qui n'est pas encore suivi par git.
- **Contenu** : `analyses/prestations/` (dont `resultats/`) et `excalidraw/` (dont les changements de `lib/excalidraw_builder.py` : largeurs de texte recalées sur la police Excalifont, `section` qui renvoie son cadre ; et l'historique des versions : `lib/historique.py`, `server.py`, `viewer/`), sur la branche `fix/presta`. Le dossier `excalidraw/historique/` reste hors de git.
- **Attend** : ton accord. `.gitignore` est aussi modifié : à regarder avant de l'inclure.

### 13. Lot 4 : T26 et T27

- **Objectif** : savoir ce qu'est une demande en AT/MP et en PF, et d'où part la durée de liquidation, avant d'adapter la requête T25 et d'écrire les fiches T26 et T27.
- **Fichier** : [lot4_controles_t26_t27.sql](lot4_controles_t26_t27.sql), P59 à P68, sur USER_DWH, comptages seulement. P59, P63 et P68 lisent DEBOURS (soins des dossiers AT/MP) : quelques minutes chacune.
- **Questions** :
  - T26 Q1, qu'est-ce qu'une demande AT/MP (P59, P60) ;
  - T26 Q2, ce qui est notifié (P61) ;
  - T26 Q3, d'où part la durée : la déclaration ou le certificat de consolidation, comme le dit Kaya 2017 (P62) ;
  - T26 Q4, les frais médicaux (P63) ;
  - T27 Q1, un enfant ajouté est-il une demande (P64, P65) ;
  - T27 Q2, les allocations prénatales, « ni réception ni notification » selon Kaya 2017 (P66) ;
  - T27 Q3, la maternité (P65, P67) ;
  - T26 Q5 et T27 Q4, refus et stock en instance (P68).
- **État au 30/10/2026** : partie A exécutée ; résultats dans les en-têtes, constats et tableau « ce que T26 et T27 reprennent de T25 » dans le README (« Lot 4 »), cartes B18 à B23 et D9 à D14 dans le schéma regles_prestations. Partie B (P69 à P73) exécutée le même jour : les IJ attendent environ 7 mois après la fin de l'arrêt sans que la base dise pourquoi ; les réceptions ANCIEN AT/MP sont des demandes de rente (70 %) ou de rente de survivant (9 %) ; l'allocation d'incapacité est notifiée depuis 2019 ; le pic de soins de 2024 vient de dates d'approbation remplies en bloc, d'où la datation des soins par la saisie du premier remboursement.
- **Fait le 30/10/2026** : [t26_requete_proposee.sql](t26_requete_proposee.sql), [t27_requete_proposee.sql](t27_requete_proposee.sql), et les fiches Excalidraw `T26_dossiers_at_mp` et `T27_dossiers_pf` (dossier « tableaux cipres »), avec quatre questions chacune. Reste : exécuter les requêtes (tâche 7) et recueillir les réponses de la direction des prestations.
- **Démarche suivie** : reporter les résultats dans les en-têtes, en tirer les règles de T26 et T27 (celles de T25 qui valent telles quelles, celles qui changent), adapter la requête T25 aux branches A, F et M, puis faire les deux fiches Excalidraw avec le gabarit de T25.

## Questions en attente de réponse

Elles sont listées dans la partie « Questions ouvertes » du [README](README.md) (DSI et direction des prestations). Une réponse peut changer les tâches 3 à 10 : la reporter dans le README, puis revoir les règles concernées.
