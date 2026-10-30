# Analyses de données — domaine Prestations

Ce dossier garde l'historique des requêtes de profilage du domaine des prestations sociales. Elles servent à vérifier les tableaux CIPRES du domaine 3 (T25 à T57) et les requêtes DTM qui les produisent. Chaque requête indique son objectif, les tableaux visés et le résultat obtenu.

Référence : [reference_cipres_T25_T57.md](reference_cipres_T25_T57.md) résume ce que demande l'Annuaire, les tranches d'âge de chaque tableau et les hypothèses H1 à H14 tirées de la lecture des requêtes.

Suite du travail : [TODO.md](TODO.md) liste les tâches à reprendre, avec leurs objectifs et leurs dépendances.

## Conventions

- Les requêtes portent d'abord sur la base source, schéma `USER_DWH`, celui que lit l'ETL des prestations malgré son nom : tables de prestations et leurs nomenclatures. On ne dépend donc pas des faits du DWH, qui peuvent être vides ou incomplets. Une partie B, exécutée avec la connexion du DWH, compare les faits `DWH.FAIT_*` à la source et lit les tables `DTM.*`.
- Le contenu des tables DTM n'est pas analysé : il est considéré comme faux. Les règles sont refaites à partir de la source ; les requêtes DTM ne sont relues que pour savoir quoi corriger.
- Un fichier par lot, numéroté dans l'ordre d'exécution. Les requêtes sont numérotées P01, P02… (P pour prestations, pour ne pas les confondre avec les Q du domaine GRH), sans jamais réutiliser un numéro.
- L'en-tête de chaque requête donne l'objectif, les tableaux visés, le résultat et le constat.
- Les requêtes sont en lecture seule et ne renvoient que des comptages, sans donnée nominative.
- La base est antérieure à Oracle 12c : pas de `FETCH FIRST`, on limite avec `ROWNUM`. Les alias font 30 caractères au plus.
- Exécution en mode script (F5) dans SQL Developer.
- Une fois un lot exécuté, ses résultats sont reportés dans les en-têtes « Résultat ». Une liste trop longue pour un en-tête (plus d'une vingtaine de lignes) est gardée telle quelle dans [resultats/](resultats/), un fichier TSV par requête, nommé `lotN_Pnn_<sujet>.tsv`.

Vue d'ensemble pour s'approprier le travail : le schéma Excalidraw [regles_prestations](../../excalidraw/schemas/prestations/regles_prestations.excalidraw) reprend toutes les règles et tous les constats avec les requêtes qui les prouvent (à régénérer par `excalidraw/sources/prestations/regles_prestations.py` quand une règle change).

## Lots

| Lot | Fichier | Date | Objectif | État |
| --- | --- | --- | --- | --- |
| 1 | [lot1_inventaire_prestations.sql](lot1_inventaire_prestations.sql) | 26/10/2026 | Inventaire : volumes, codes (branches, types de prestation, statuts, ajustements, étapes), qualité des individus ; test des hypothèses H1, H3 à H9, H11 à H14. Partie A sur USER_DWH (P01–P16), partie B sur le DWH (P17–P19) | Exécuté le 26/10/2026 ; P19 abandonnée (contenu DTM non utilisé) |
| 2 | [lot2_statuts_stocks_nouveaux.sql](lot2_statuts_stocks_nouveaux.sql) | 26/10/2026 | Sens des statuts des débours et paiements sans effet (P20–P23) ; pensionnés au 31/12 et esquisse de T32 (P24–P26) ; nouveaux pensionnés (P27–P28) ; mois cotisés (P29–P30). Sur USER_DWH | Exécuté le 26/10/2026 |
| 3 | [lot3_listes_de_codes.sql](lot3_listes_de_codes.sql) | 29/10/2026 | Partie A : toutes les listes de codes du domaine, avec leurs libellés (P31–P47). La partie B (points à vérifier, indus, allocataires) suivra à partir de P48. Sur USER_DWH | Partie A exécutée le 29/10/2026 ; listes longues (P35, P44, P46, P47) dans [resultats/](resultats/) |
| 3 B | [lot3b_controles_t25.sql](lot3b_controles_t25.sql) | 30/10/2026 | Partie B pour T25 : une requête ou plus par question de la fiche T25 (Q1 coordination P48, Q2 date du dépôt P49, Q3 stock en instance P50 et P58, Q4 pension de la veuve P51–P54 et P58, Q5 refus P55–P57). Sur USER_DWH, sans DEBOURS | Exécuté le 30/10/2026 (P52 relancée après un ORA-00972) ; P57 dans [resultats/](resultats/) |

## Requêtes proposées (à valider)

| Tableau | Fichier | Date | État |
| --- | --- | --- | --- |
| T25 | [t25_requete_proposee.sql](t25_requete_proposee.sql) | 27/10/2026, revue le 30/10/2026 | Traduit les règles 1 à 10 de la fiche Excalidraw « T25 dossiers de pension ». Revue après le lot 3 B : compte des demandes (dossier × ligne), rattache les réceptions ANCIEN à la réversion, garde COOR_AC, n'exclut que CNSS_AC, sort du stock les dossiers fermés jamais notifiés et les réceptions d'avant 2016 sans dossier (paramètre `debut_stock`). Pas encore exécutée |
| T25, question Q4 | [t25_q4_deux_cas.sql](t25_q4_deux_cas.sql) | 30/10/2026 | La même requête simplifiée sous les deux réponses possibles à Q4 : cas A, un dossier par ligne (nouveau dossier pour la veuve) ; cas B, une demande par dossier et par ligne (pension de la veuve dans le dossier du défunt). Le lot 3 B donne raison au cas B, repris dans la requête T25. Pas encore exécutée |
| T25–T27 (DTM_DOSSIER) | [dtm_dossier_requete_proposee.sql](dtm_dossier_requete_proposee.sql) et [dtm_dossier_ddl_propose.sql](dtm_dossier_ddl_propose.sql) | 29/10/2026 | Remplacerait `dossier.sql` V5 : mêmes colonnes plus SOMME_DELAIS_JOURS ; chaque événement (dépôt, liquidation, rejet) compté à son mois ; NB_INSTANCE = stock en fin de mois. Mêmes règles que T25, étendues à AT/MP et PF. Pas encore exécutée ; accord de Jules requis (sens de colonnes changé) ; 7 points à vérifier en fin de fichier |

## Tableaux CIPRES et requêtes

| Tableau | Contenu | Requête DTM | Requêtes de profilage |
| --- | --- | --- | --- |
| T25–T27 | Dossiers PVID, AT/MP, PF par durée de liquidation | `dossier.sql` | P02, P03, P04, P06, P09, P10 |
| T28–T30 | Dossiers contrôlés a posteriori | `controle_prestation.sql`, `indicateurs_prestations.sql` | P06, P13 |
| T31 | Prestations indues par branche et par mois | `prestation_indue.sql` | P07, P11 |
| T32–T41 | Pensionnés, crédirentiers et allocataires au 31/12, par âge et sexe | `prestation.sql` | P03, P04, P07, P08, P12, P14, P15, P16 |
| T42–T49 | Nouveaux pensionnés et crédirentiers de l'exercice | `prestation.sql` | P04, P12, P14, P16 |
| T50–T55 | Évolution sur 5 ans des bénéficiaires et des montants | `prestation.sql` | P04, P07, P08 |
| T56 | Prestations servies par branche | `prestation.sql` | P02, P05, P07 |
| T57 | Décédés par branche | `prestation.sql` | P15 |
| Tous | Faits du DWH et tables DTM publiées | — | P01, P17, P18, P19 |
| Tous | Listes de codes et axe géographique (directions, services, lieux de paiement) | — | P31 à P47 |

## Constats à ce jour

Le détail, hypothèse par hypothèse, est dans [reference_cipres_T25_T57.md](reference_cipres_T25_T57.md).

- La base source est une copie arrêtée au 02/02/2026, et le DWH en est la copie complète (P01, P17).
- **Dossiers (T25–T27).** DTM_DOSSIER ne garde que les dossiers qui ont un paiement : en 2025, 33 % des dossiers AT/MP ouverts, 61 % en PF, 83 % en vieillesse (P09). Il lit le statut F comme « liquidé » et A comme « en instance ». Or A veut dire « dossier actif » et F « dossier fermé » : 61 632 dossiers de vieillesse sont au statut A (P02). Pour 2025, il ne trouve que 310 dossiers liquidés, alors que 15 479 sont notifiés (P10).
- **Durée de liquidation.** Elle se calcule bien (réception → notification). Médiane 2025 : vieillesse 62 j, AT/MP 156 j, PF 13,5 j, maternité 17 j. Plus de 45 jours pour 61 % des dossiers de vieillesse et 98 % des dossiers AT/MP (P10).
- **Branche M.** C'est la maternité, rattachée à la branche PF (P02, P05). Le libellé « Maladie » de `prestation.sql` est faux, et M doit rejoindre PF dans les tableaux CIPRES.
- **Montants.** Le net à payer vu par DTM_PRESTATION est gonflé 3,8 fois (222,9 Md pour 58,5 Md en 2025) (P08). Il faut prendre le montant des débours.
- **Bénéficiaires.** DTM_PRESTATION prend la personne payée dans l'effet de paiement. Or 62 % des paiements PF de 2025 n'ont pas d'effet (P07). Pour les orphelins, la personne payée est le tuteur, jamais l'orphelin (P16). Tous les paiements sont reliés à leur prestation (P20) : la personne peut donc se lire dans PRESTATION_ESP.
- **Tranches d'âge.** La source a 21 tranches de 5 ans : les 11 grilles CIPRES s'obtiennent par regroupement (P14).
- **Contrôles a posteriori (T28–T30).** 115 contrôles en 2025 pour 155 206 dossiers payés, soit 0,07 % (norme : au moins 5 %). Plus aucune saisie depuis juillet 2025. DEB_VERIFIE, compté comme contrôle par le DTM, est un drapeau de saisie (P06, P13).
- **Indus (T31).** Le classement trop perçu / moins perçu suit les libellés, sauf REM-TP (remboursement d'un trop perçu). La branche M est exclue à tort (P11).
- **Pensionnés au 31/12 (T32–T41).** Les étapes sont des étapes de calcul, pas des événements de vie (P12). Le stock se lit avec le statut et les dates de la prestation, et avec les paiements du 4e trimestre (lot 2).

### Lot 2

- **Ce qu'est un paiement.** Le statut du débours suit celui de son effet (P21). D = payé par un effet décaissé (53,72 Md en 2025) ; P = payé sans effet, approuvé et comptabilisé (7,63 Md) ; V = validé mais jamais décaissé ; N, A, S = non approuvés (P20). Les vrais paiements sont donc D et P, à faire confirmer. Le circuit P existe depuis 2019, et les PF y sont passées en masse en 2025 (P22, P23).
- **Le mois d'un paiement.** DEB_DATE_EFFET est le mois payé, pas la date du paiement : les débours sont préparés environ 3 semaines avant, et 9 % sont des rappels saisis 2 mois ou plus après (P20).
- **Statut des prestations.** Il est fiable : A = active, F = fermée (avec une date de radiation), S = suspendue (avec une date de suspension) (P24).
- **Pensionnés au 31/12/2025.** Pension de vieillesse : 32 974 actives par les dates, 29 643 payées au 4e trimestre. Veuves : 20 931 et 19 474. Mais orphelins : 13 383 actives et seulement 4 974 payées, soit 8 447 pensions d'orphelin actives sans paiement (P25). 215 pensionnés de vieillesse actifs sont décédés.
- **Première version de T32.** 29 721 pensionnés de vieillesse payés au 4e trimestre 2025 (23 942 hommes, 5 779 femmes), 41,56 Md versés en 2025, soit 1,40 M FCFA par an et par pensionné. 69 pensionnés payés ont 100 ans ou plus (P26).
- **Nouveaux pensionnés.** Notification, saisie, calcul initial et premier paiement donnent presque le même nombre (2 682 à 2 953 pensions de vieillesse en 2025). La date d'effet n'en donne que 2 308, car un tiers des pensions notifiées en 2025 remontent à 2024 ou avant (rappel médian de 4 mois, 8 à 11 mois pour les survivants). La pension est payée environ 4 jours après la notification (P27, P28).
- **Mois cotisés.** Ce sont des mois (médiane 295 pour une pension de vieillesse, 92 pour une allocation). Ils sont ceux de l'assuré, recopiés sur chaque prestation du dossier ; 528 personnes ont plusieurs pensions de vieillesse (révisions). Les valeurs au-dessus de 600 (jusqu'à 999) sont à écarter, et le détail obligatoire / volontaire ne s'additionne pas au total (P29, P30).

### Lot 3, partie A (listes de codes)

- **Libellés sûrs.** Les 28 codes de prestation en espèces ont tous un libellé lu dans la base (P34). RPRA est la pension de retraite anticipée. RBEC (bonification pour enfant à charge, 68 836 prestations), RME et RME-COL (majorations pour enfant CARFO et collectivités), RCCM (contributions aux charges du ménage) et RRC (remboursement de cotisations) sont des accessoires versés en plus d'une pension : ils ne doivent pas faire un pensionné de plus.
- **Catégories de dossier.** 120 518 dossiers sur 367 947 n'ont pas de catégorie ; les requêtes proposées les lisent comme ORDINAIRE. HORS_CONV n'est jamais utilisé (P33).
- **Prestations en nature.** 51 types en 24 catégories, dont 7 frais de maternité (FM) jamais payés en 2025 : les soins payés sont ceux des AT/MP (P35, P05).
- **Paiements.** 4 types de débours : AJ, CP, PM, RB (P36). 11 types d'effet ; virement (BVB) et espèces (BE) font 98,1 % des 8,7 millions d'effets ; les relevés de coordination entre caisses (RPC, RRC) ne sont jamais utilisés (P37). 7 modes de paiement, dont S, structures sanitaires et sociales (P38).
- **Étapes et ajustements.** 13 étapes : FS n'est jamais utilisé, et 512 prestations n'ont pas de calcul initial (P40). 13 types d'ajustement : les codes COVID sont AV-COVID19 et DON-COVID19 ; OE n'est jamais utilisé (P41).
- **Géographie.** 6 directions (le siège, 0, est inactif), 20 services, 350 lieux de paiement (P45 à P47). DR_OU_SP vaut O pour le lieu où siège chaque service. Le lieu 201 « caisse tenkodogo » est rattaché à la DRB sans service, alors que Tenkodogo dépend de la DRF.

### Lot 3, partie B (questions de la fiche T25)

- **Q1, coordination.** La CNSS instruit les dossiers COOR_AC : 76 % sont reçus à son guichet, la moitié notifiés par elle ; 238 dépôts et 292 liquidations en 2025, 6 à 8 % de T25 (P48). Proposition révisée : les garder dans T25, n'exclure que les dossiers transférés (CNSS_AC, rien en 2025).
- **Q2, date du dépôt.** Le dossier est complet à la réception : 96 % des pièces sont saisies le jour même (P49). La date de réception convient. Chaque réception reçoit un rendez-vous, 45 jours après en médiane.
- **Q3, stock en instance.** La règle 6 donne 23 218 demandes en instance au 31/12/2025, cinq fois les dépôts d'une année, dont 79 % reçues avant 2021 (P50). Ce ne sont pas des demandes en attente : réceptions anciennes sans lien vers leur dossier (les 30 774 dossiers sans catégorie n'ont que 35 réceptions, P48), dossiers fermés sans date de notification (P57), réceptions qui visent un dossier existant (3 952 depuis 2016). Le vrai stock se compte en centaines par an.
- **Q4, pension de la veuve.** Les deux pratiques existent (P51). La moitié des pensions de veuve (13 104 sur 25 541) sont dans le dossier du défunt, alors que les allocations de survivants ont leur propre dossier. En 2025, 675 des 800 pensions de veuve et 420 des 797 pensions d'orphelin notifiées ne sont pas la première notification de leur dossier : une seconde notification arrive en médiane 9 ans après la première, et 902 dossiers ont été renotifiés en 2025 (P54). La requête T25 proposée les perd : il faut compter par demande (cas B de [t25_q4_deux_cas.sql](t25_q4_deux_cas.sql)). Peu de réversions ont une réception (121 réceptions de conjoint en 2025, P53) : prendre la date de demande de la pension quand la réception manque.
- **Nouvelle demande ou demande sur un dossier existant.** Le type de réception le dit (P52). NOUVEAU (51 417) ouvre son propre dossier. ANCIEN (4 018, dont 616 en 2025) est déposé sur un dossier existant, déjà notifié dans 98 % des cas, et n'ouvre jamais de dossier : ce sont surtout les réversions, et les réceptions « qui visent un dossier » du stock de P50. Onze réceptions ont ouvert deux dossiers. P58 le confirme : 94 % des réceptions ANCIEN sont suivies d'une pension de survivant, presque jamais d'une révision. En 2025, 538 sur 616 l'étaient déjà, et 78 restaient sans suite. Le lien L domine aussi pour les réversions : il veut sans doute dire « le demandeur lui-même », pas « l'assuré ».
- **Liquidés de 2025.** Avec la première notification (règle 3), 3 901 dossiers de pension sont liquidés en 2025, et non environ 4 800 : P09 prenait la dernière notification. 3 901 + 902 renotifiés ≈ 4 783. DOS_DATE_NOTIFICATION est vide pour tous les dossiers de pension (P54).
- **Q5, refus.** Les refus ne sont notés que dans le module des rejets, avec un motif codé : prescription, pas de bénéficiaire, pièce non conforme, autre (P56). Mais il sert peu : 146 rejets de pension depuis 2017. RD_STATUT ne note pas les refus (R : 47 réceptions anciennes, P55). Un dossier fermé sans notification n'est pas un refus : ce sont surtout des pensions d'avant 2018, radiées, dont la notification n'a pas été saisie, environ 8 000 (P57).

## Règles proposées après les lots 1 et 2

À faire valider par la direction des prestations avant d'écrire les nouvelles requêtes.

| N° | Sujet | Règle proposée | Tableaux |
| --- | --- | --- | --- |
| R1 | Branches | V → PVID ; A → AT/MP ; F et M → PF | Tous |
| R2 | Ligne CIPRES | D'après le code de prestation (correspondance ci-dessous) | Tous |
| R3 | Paiement | Débours de paiement au statut D ou P ; montant = DEB_MONTANT, jamais le net à payer de l'effet | T31, T32–T56 |
| R4 | Mois d'un paiement | Mois de DEB_DATE_EFFET (mois payé) | T31, T50–T56 |
| R5 | Personne comptée | L'ayant droit de la prestation (IND_ID_PREST) ; pour les allocations familiales, l'allocataire (la personne payée) | T32–T49 |
| R6 | Pensionné au 31/12 | Prestation active au 31/12 (effet avant, ni radiée ni suspendue) et payée au 4e trimestre ; une personne comptée une fois par tableau | T32–T41 |
| R7 | Nouveau pensionné | Prestation notifiée dans l'exercice ; une personne comptée une fois | T42–T49 |
| R8 | Âge | Année de l'exercice moins année de naissance ; tranches de 5 ans regroupées selon la grille de chaque tableau | T32–T49 |
| R9 | Services passés | Mois cotisés de la dernière prestation de la personne ; valeurs au-dessus de 600 écartées | T42, T50 |
| R10 | Dossier déposé | Réception du dossier dans l'exercice | T25–T27 |
| R11 | Dossier liquidé | Dossier notifié dans l'exercice ; durée = notification − réception | T25–T27 |
| R12 | Dossier en instance | Réception sans notification au 31/12 | T25–T27 |

## Correspondance proposée entre codes de prestation et lignes CIPRES

À faire viser par la direction des prestations (P04).

| Tableaux | Ligne CIPRES | Codes |
| --- | --- | --- |
| T25, T28, T32 | Pension de vieillesse | RPV, RPRA (pension de retraite anticipée, P34 ; rattachement à confirmer) |
| T25, T28, T33 | Pension d'invalidité | RPI |
| T25, T28, T34 | Pension de veuf / veuve | RPSV, RPSF |
| T25, T28, T35 | Pension d'orphelin | RPSE |
| T28, T36 | Pension d'ascendant | RPA |
| T25, T28, T50 | Allocation de vieillesse | RAV |
| T25, T28, T51 | Allocation de survivants | RAS, RASC, RASO, RASA |
| T25, T28 | Autres prestations de la branche | RBEC bonification pour enfant à charge, RME et RME-COL majorations pour enfant (CARFO, collectivités), RCCM contributions aux charges du ménage, RRC remboursement de cotisations (accessoires : montant à rattacher à la pension ?) |
| T26, T29, T37, T48 | Rente viagère | ARI |
| T26, T29, T38 | Rente de veuf / veuve | ARSV, ARSF |
| T26, T29, T39 | Rente d'orphelin | ARSE |
| T26, T29, T40 | Rente d'ascendant | ARSA |
| T26, T29, T52 | Indemnités journalières AT/MP | AIJ |
| T26, T29, T53 | Allocation d'incapacité | AAI |
| T26, T29 | Autres prestations de la branche | Prestations en nature (frais médicaux AT/MP) |
| T27, T30, T41 | Allocations familiales | FAF |
| T27, T30, T54 | Indemnités journalières de maternité | MIJ |
| T27, T30, T55 | Allocations prénatales | FAP, MAP |

## Questions ouvertes

**À la DSI**

- Que veulent dire les statuts, qui n'ont pas de liste de libellés : DOS_STATUT (A, F), PE_STATUT (A, F, S), DEB_STATUT (A, D, I, N, P, S, V), EFP_STATUT (A, D, I, N), RD_STATUT (E, I, L, R, A), NOT_STATUT (A, I), AJ_STATUT (A, C, I, R) ?
- Pourquoi 62 % des paiements PF de 2025 n'ont-ils pas d'effet de paiement ? Où se trouve la date de décaissement quand elle manque sur le débours ?
- DOSSIERS_CONTROLES n'est plus alimentée depuis juillet 2025 et contient des années 202, 203 et 2055. Les contrôles a posteriori sont-ils enregistrés ailleurs ?
- Les rejets sont-ils tous saisis dans DOSSIER_REJETE (486 en tout) ? Que deviennent les réceptions sans dossier ?
- Que veulent dire les drapeaux *_VERIFIE (DEB_VERIFIE, DOS_VERIFIE…) ?
- Les statuts de débours se lisent-ils bien ainsi : D payé par effet, P payé sans effet, V validé non décaissé ? Et I, A, N, S ? (Les modes de paiement sont maintenant connus, P38.)
- Par quel circuit passent les paiements au statut P, sans effet (virement, téléphone mobile…) ? Pourquoi les PF y sont-elles passées en masse en 2025 ?
- Que veut dire la valeur 999 dans les mois cotisés ? Les colonnes « obligatoire » et « volontaire » sont-elles encore remplies ?
- TSINISTRE n'est plus alimentée depuis décembre 2024 : est-elle abandonnée ?
- La base USER_DWH est une copie au 02/02/2026 : quand sera-t-elle rafraîchie ?
- Lieux de paiement : que veut dire DR_OU_SP quand il est vide (71 lieux) ? Le lieu 201 « caisse tenkodogo », rattaché à la DRB sans service, est-il une erreur ? Que désigne CNSS-DCP ?
- 120 518 dossiers n'ont pas de catégorie (DOS_CATEGORIE vide) : sont-ils tous des dossiers ordinaires ?

**À la direction des prestations**

- Un dossier déposé est-il une réception ? Un dossier liquidé, un dossier notifié ? Un dossier en instance, une réception sans notification ?
- Un dossier compte-t-il l'année de son dépôt ou l'année de sa liquidation ?
- Les dossiers de coordination payés par une autre caisse et les dossiers transférés entrent-ils dans les tableaux de la CNSS ?
- La correspondance des codes ci-dessus est-elle juste (retraite anticipée, accessoires de pension) ?
- Qui est un pensionné au 31 décembre : une pension active non suspendue, ou une pension payée en décembre ? Qui est un nouveau pensionné : date d'effet de la pension ou premier paiement ?
- Pour les orphelins, compte-t-on l'orphelin (et non le tuteur payé) ?
- T57 « décédés par branche » : s'agit-il des bénéficiaires décédés (fin de droit) ou des assurés décédés qui ouvrent un droit aux survivants ?
- Indus : un remboursement de trop perçu (REM-TP) est-il un indu ? Les avances (AV) et les mois de pension gratuits (MPG) en sont-ils exclus ?
- Une prestation est-elle payée quand l'effet est décaissé, ou dès qu'elle est validée ? Les montants de l'année se comptent-ils au mois payé ou à la date du paiement ?
- 8 447 pensions d'orphelin sont actives mais sans aucun paiement au 4e trimestre 2025 : faut-il les radier (limite d'âge) ?
- 69 pensionnés de vieillesse payés ont 100 ans ou plus, et 215 pensionnés actifs sont décédés : un contrôle d'existence est-il prévu ?
- Une allocation de vieillesse peut-elle correspondre à 489 mois cotisés (40 ans) ?
