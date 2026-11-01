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
| 4 | [lot4_controles_t26_t27.sql](lot4_controles_t26_t27.sql) | 30/10/2026 | Contrôles pour T26 et T27, sur le modèle du lot 3 B. T26 : ce que porte un dossier AT/MP (P59), réceptions et pièces (P60), prestations notifiées (P61), date de départ de la durée, dont le certificat de consolidation (P62), frais médicaux (P63). T27 : enfant ajouté et notifications suivantes (P64), réceptions et pièces PF et maternité (P65), allocations prénatales (P66), dossiers de maternité (P67). Les deux : refus et dossiers sans suite (P68). Partie B : départ de la durée des IJ (P69), réceptions ANCIEN AT/MP (P70, P71), date de l'allocation d'incapacité (P72), pic des soins de 2024 (P73). Sur USER_DWH ; DEBOURS lu par P59, P63, P68, P71 à P73 | Exécuté le 30/10/2026 : partie A (P59–P68), puis partie B (P69–P73) ajoutée après ses résultats |
| 5 | [lot5_beneficiaires_31_12.sql](lot5_beneficiaires_31_12.sql) | 31/10/2026 | Contrôles pour T32 à T41 : le stock au 31/12 de chaque tableau de 2020 à 2025, et qui compter comme allocataire en T41 (P74) ; le montant, payé aux bénéficiaires comptés ou tout le payé de l'année (P75) ; l'âge, le sexe et les grilles (P76) ; les personnes à plusieurs pensions (P77) ; la coordination (P78). Les chiffres publiés par la CNSS (annuaire 2024 : pensions, rentes, allocataires, montants) sont dans l'en-tête pour comparer. Sur USER_DWH ; DEBOURS lu par P74, P75 et P78 | Exécuté le 31/10/2026 ; résultats dans les en-têtes et dans resultats/lot5_*.tsv |
| 6 | [lot6_ecarts_t32_t41.sql](lot6_ecarts_t32_t41.sql) | 31/10/2026 | Les écarts qui restent après le lot 5 : qui compter comme allocataire et comme enfant en T41 (payés dans l'année, au dernier trimestre, base vue à la publication) (P79) ; les dates de naissance par défaut (P80) ; les pensions et rentes suspendues au 31/12 (P81). Sur USER_DWH ; DEBOURS lu par P79 et P81 | Exécuté le 01/11/2026 ; résultats dans les en-têtes et dans resultats/lot6_*.tsv |
| 7 | [lot7_nouveaux_beneficiaires.sql](lot7_nouveaux_beneficiaires.sql) | 01/11/2026 | Contrôles pour T42 à T49 (nouveaux pensionnés et crédirentiers) : les entrées de chaque tableau de 2020 à 2025, saisie, première entrée, notification, sorties avant le 31/12, coordination (P82) ; l'âge et le sexe des nouveaux retraités face au tableau 25 de l'annuaire de la CNSS (P83) ; l'âge et le sexe de tous les nouveaux (P84) ; le montant, au mois payé ou rappel compris (P85) ; les services passés de T42 (P86) ; le degré d'IPP de T48 (P87). Les chiffres publiés (tableaux 23, 25, 75, 77) sont dans l'en-tête. Sur USER_DWH ; DEBOURS lu par P85 | Parties A et B exécutées le 01/11/2026 ; partie B : part CNSS des services passés (P88 à P90), rentes viagères par accident (P91) |
| 8 | [lot8_evolution_5_ans.sql](lot8_evolution_5_ans.sql) | 01/11/2026 | Contrôles pour T50 à T57 (évolution sur 5 ans, récapitulatifs par branche) : qui compter chaque année, liquidés, payés ou saisis, et quel montant (P92) ; le détail par code, allocation de survivants et ses parts, prénatales (P93) ; les services passés des allocations de vieillesse (P94) ; les montants par branche (P95) ; les colonnes de décès (P96) ; les décédés par branche (P97). Les chiffres publiés (tableaux 24, 26, 34, 35, 40, 81, 97, 99) sont dans l'en-tête. Sur USER_DWH ; DEBOURS lu par P92, P93, P95 | Parties A et B exécutées le 01/11/2026 ; texte de la vue des décédés dans resultats/lot8_P101_vue_prestataires_decedes.sql |
| 9 | [lot9_indus.sql](lot9_indus.sql) | 01/11/2026 | Contrôles pour T31 (prestations indues) : les ajustements par type, branche et année (P102) ; le sens des statuts A, C, I, R (P103) ; ce que paient les débours de type AJ (P104) ; T31 de 2024 rempli, avec le taux d'indus (P105) ; d'où viennent les trop-perçus, dont ceux après un décès (P106). L'annuaire de la CNSS ne publie pas d'indus. Sur USER_DWH ; DEBOURS lu par P103 à P105 | Exécuté le 01/11/2026 ; résultats dans les en-têtes et dans resultats/lot9_*.tsv |

## Requêtes proposées (à valider)

| Tableau | Fichier | Date | État |
| --- | --- | --- | --- |
| T25 | [t25_requete_proposee.sql](t25_requete_proposee.sql) | 27/10/2026, revue le 30/10/2026 | Traduit les règles 1 à 10 de la fiche Excalidraw « T25 dossiers de pension ». Revue après le lot 3 B : compte des demandes (dossier × ligne), rattache les réceptions ANCIEN à la réversion, garde COOR_AC, n'exclut que CNSS_AC, sort du stock les dossiers fermés jamais notifiés et les réceptions d'avant 2016 sans dossier (paramètre `debut_stock`). Pas encore exécutée |
| T25, question Q4 | [t25_q4_deux_cas.sql](t25_q4_deux_cas.sql) | 30/10/2026 | La même requête simplifiée sous les deux réponses possibles à Q4 : cas A, un dossier par ligne (nouveau dossier pour la veuve) ; cas B, une demande par dossier et par ligne (pension de la veuve dans le dossier du défunt). Le lot 3 B donne raison au cas B, repris dans la requête T25. Pas encore exécutée |
| T26 | [t26_requete_proposee.sql](t26_requete_proposee.sql) | 30/10/2026 | Traduit les règles 1 à 10 de la fiche Excalidraw « T26 dossiers AT/MP », sur le modèle de T25 et d'après le lot 4 : une demande par dossier et par ligne ; IJ et soins déposés par la déclaration, rente, allocation et rente de survivant par leur demande au guichet (sinon le certificat pour la rente) ; soins liquidés à la première saisie d'un remboursement ; stock limité à 24 mois sans suite (paramètre `sans_suite_mois`). Pas encore exécutée |
| T27 | [t27_requete_proposee.sql](t27_requete_proposee.sql) | 30/10/2026 | Traduit les règles 1 à 10 de la fiche Excalidraw « T27 dossiers PF » : dossiers F et M ; une grossesse compte sur la ligne des IJ et sur celle des allocations prénatales ; un enfant ajouté est une demande si `enfant_ajoute` = 1, déposée à la date de demande de son allocation ; stock limité à 24 mois sans suite. Pas encore exécutée |
| T32 à T41 | [t32_t41_requete_proposee.sql](t32_t41_requete_proposee.sql) | 31/10/2026 | Une requête pour les dix tableaux des bénéficiaires au 31/12, dans la forme de l'Annuaire (tranche d'âge × hommes, femmes, total ; effectif et montant ; pension moyenne). Règles R2 à R6 et R8 ; T41 compté par dossier d'allocataire (Kaya 2017) ; âge de 120 ans et plus en « âge inconnu » ; fiche pilote T32 et fiche T41 (01/11). Ses règles retombent sur l'annuaire de la CNSS 2024 (lots 5 et 6) ; T41 ne compte que les enfants payés dans l'année (P79). Pas encore exécutée |
| T42 à T49 | [t42_t49_requete_proposee.sql](t42_t49_requete_proposee.sql) | 01/11/2026 | Une requête pour les huit tableaux des nouveaux bénéficiaires, sur le modèle de T32 à T41 (tranche d'âge × hommes, femmes, total ; effectif et montant), avec le total des services passés (T42) et le degré moyen d'IPP (T48). Nouveau = première prestation du tableau saisie dans l'exercice (R7) ; T45 lu comme le total des rentes AT/MP, T47 = ARSA. Part CNSS des services passés en coordination (total moins PE_COT_AUTRE) ; fiche pilote T42 (01/11). Pas encore exécutée |
| T50 à T57 | [t50_t57_requete_proposee.sql](t50_t57_requete_proposee.sql) | 01/11/2026 | Trois requêtes : T50 à T55 (une prestation sur 5 ans, effectif et montant par sexe ; services passés moyens pour T50), T56 (prestations servies par branche), T57 (assurés décédés par branche, d'après Kaya 2017). Bénéficiaire de l'année = prestation liquidée (première notification) ; montant = payé pour les mois de l'année. Date des montants réglable (date_montant) ; fiche pilote T50_T57_evolution_5_ans (01/11). Pas encore exécutée |
| T31 | [t31_requete_proposee.sql](t31_requete_proposee.sql) | 01/11/2026 | La maquette de l'Annuaire : une ligne par mois et une ligne TOTAL ; par branche (PVID, AT/MP, PF maternité comprise) puis au total, payées, trop-perçus (TP, TP-CONV), moins-perçus (MP, RC) ; taux d'indus (norme 2 %). Règles I1 et Kaya 2017 ; statut R (annulé) écarté ; réglage date_montant pour les payées ; taux 2024 : 0,39 % (lot 9) ; fiche T31_prestations_indues (01/11). Pas encore exécutée |
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

### Lot 4 (T26 et T27)

**T26, accidents du travail et maladies professionnelles**

- **Une demande par ligne, comme en T25.** Un dossier AT/MP porte souvent plusieurs prestations liquidées à des dates différentes : les IJ d'abord, la rente ou l'allocation d'incapacité des années après (191 dossiers « IJ + allocation » sur 231 et 77 « IJ + rente » sur 90 ont plusieurs dates de notification, P59). Toutes les prestations en espèces ont une date de demande, et les IJ ont une date de notification dans 97 % des cas (P61).
- **Les dépôts.** La déclaration (réception NOUVEAU) ouvre le dossier. La rente, l'allocation et les rentes de survivants arrivent par une réception ANCIEN sur ce dossier, avec un certificat de consolidation (233) ou de décès (53) (P60), comme la réversion en T25. 70 % des réceptions ANCIEN sont suivies dans l'année d'une rente ou d'une allocation, 9 % d'une rente de survivant ; celles qui ne sont suivies de rien portent surtout des pièces de maintien des droits (certificat de vie, de scolarité) (P70, P71). Depuis cette réception, la durée médiane est de 13 à 35 jours en 2025 ; depuis la déclaration, de 444 à 986 jours (P62). Le certificat pour la rente, d'où part la durée selon l'atelier de Kaya 2017, est rempli pour 65 % des rentes et 85 % des allocations (64 à 107 jours).
- **Les IJ attendent environ 7 mois.** Une IJ est demandée avec la déclaration : elle n'a pas de dépôt à elle. L'arrêt de travail dure 27 jours en médiane, mais l'IJ est notifiée 183 jours après la déclaration et 211 jours après la fin de l'arrêt. La date de reprise du travail n'est jamais saisie, le certificat de guérison ou de reprise presque jamais (14 IJ sur 226) : rien dans la base n'explique cette attente (P62, P69). On garde la déclaration comme dépôt, et la question va à la CNSS.
- **L'allocation d'incapacité est notifiée.** Les allocations sans date de notification de P61 sont d'avant 2019 ; depuis, toutes en ont une, signée le même jour (P72). C'est l'allocation des petits taux d'incapacité, 43 à 76 par an.
- **Les soins.** Un flux de remboursements (8 à 10 par dossier), qui ne passe pas par le guichet (aucune facture ni ordonnance reçue, P70) ; plus de la moitié des dossiers soignés n'ont rien d'autre. 650 à 960 dossiers par an ont leur premier remboursement (P63). Le pic de 2024 (1 477) n'est pas une activité de 2024 : 973 vieux dossiers, dont les remboursements sont saisis depuis 2005, ont reçu en bloc leur date d'approbation en 2024 (P73). On date donc un dossier de soins par la saisie de son premier remboursement, à moins de 10 jours de l'approbation dans le flux normal.
- **Les déclarations sans suite.** 4 158 dossiers sur 9 106 ouverts de 2019 à 2025 ne donnent rien : ni IJ, ni soins, ni rente (35 à 53 % chaque année de 2019 à 2024). Ils ne sont presque jamais fermés ni rejetés : avec la règle du stock de T25, ils resteraient tous en instance (P59, P68).

**T27, prestations familiales et maternité**

- **L'enfant ajouté.** Chaque année, environ 9 000 dossiers d'allocataires reçoivent un enfant de plus, notifié en médiane 5 ans après la première notification, sans passer par le guichet (6 réceptions ANCIEN en 2025). En 2025 : 8 246 dossiers à leur première notification, 8 124 renotifiés (P64, P65). Compter par dossier × ligne ne voit pas ces ajouts.
- **La maternité.** Un dossier par grossesse, déposé environ 3 semaines avant l'accouchement ; il porte l'IJ et l'allocation prénatale dans 99,4 % des cas, notifiées ensemble 16 jours après la réception en 2025 (P65, P67).
- **L'allocation prénatale est notifiée.** L'atelier de Kaya 2017 disait qu'elle n'avait ni réception ni notification. C'est vrai de l'ancien code FAP (33 en 2025), plus de la MAP de la branche maternité, notifiée 15 jours après la demande (P66).
- **Refus et stock.** Le module des rejets sert aussi peu qu'en T25 ; en PF et en maternité, presque tous les dossiers ouverts donnent une prestation : les règles D6 et D7 de T25 s'appliquent (P68).

**Ce que T26 et T27 reprennent de T25, et ce qui change** (cartes D9 à D14 du schéma [regles_prestations](../../excalidraw/schemas/prestations/regles_prestations.excalidraw))

| Règle de T25 | T26 | T27 |
| --- | --- | --- |
| D2 : une demande = un dossier × une ligne | Oui : IJ, puis rente ou allocation | Oui ; une grossesse fait deux demandes (IJ, prénatale) : à confirmer (D14) |
| D3 : une réception = un dépôt | Déclaration → première demande ; consolidation ou décès (ANCIEN) → rente, allocation ou survivants (D9) | Nouvel allocataire, grossesse ; l'enfant ajouté n'a pas de réception (D13) |
| D4 : liquidé à la première notification | IJ, rentes, survivants, allocation (notifiée depuis 2019) ; soins : saisie du premier remboursement (D11) | Toutes les lignes |
| D5 : durée depuis le dépôt | Rentes et survivants : depuis la réception ANCIEN, ou le certificat (Kaya) ; IJ : depuis la déclaration, environ 6 mois (D10) | Telle quelle (11 à 16 jours) |
| D6 : stock en instance | Les déclarations sans suite : question (D12) | Telle quelle |
| D7 : rejets | Telle quelle | Telle quelle |

### Lot 5 (T32 à T41)

- **Les règles retombent sur l'annuaire de la CNSS.** Bénéficiaires au 31/12/2024 (R6) : T32 31 093 pour 30 808 publiés (+0,9 % ; moins de 0,5 % de 2020 à 2023), T36 exact (59), T33 +5 % (125), rentes T37 à T40 de −1,5 % à +3,4 %. Sexe 1 = hommes, 2 = femmes : 24 814 et 6 279 pensionnés de vieillesse pour 24 627 et 6 103 publiés. La répartition par âge suit celle de l'annuaire à quelques centaines près (P74 à P76).
- **Payée ou non.** Seuls 27 670 pensionnés de vieillesse comptés ont été payés en 2024 ; l'annuaire en publie 30 808 : la CNSS compte aussi les pensions sans paiement dans l'année, dont les 2 901 pensions de coordination payées par une autre caisse (COOR_AC, aucun paiement CNSS). Elles entrent dans l'effectif sans montant ; la pension moyenne en est réduite de près de 9 % (P74, P78).
- **Le montant.** L'annuaire tombe entre nos deux montants : T32 37,67 Md versés aux pensionnés comptés (−1,5 %) et 38,63 Md en tout (+1,0 %) ; l'invalidité aussi (−1,4 % et +2,2 %) ; pour les survivants, nos deux montants sont un peu au-dessus (+1,6 % et +2,5 %). On garde le montant des comptés, le seul qui se range dans les tranches d'âge ; le reste (0,96 Md versés en 2024 à 1 105 pensionnés sortis dans l'année) se montre à part (P75).
- **Les orphelins.** 5 774 pensions d'orphelins de 25 ans et plus sont encore en cours en 2024, mais presque jamais payées : la limite d'âge est appliquée par le paiement, pas par la radiation, et doit rester dans la requête (P74, P75).
- **Veuves et orphelins sous le publié depuis 2023** : −4,8 % et −15,9 % en 2024, alors qu'ils étaient au-dessus de 2020 à 2022. Deux pistes : des radiations datées de 2023 saisies après la publication (A06, A07 du dossier de comparaison) ou des suspensions que la CNSS compte encore (P81).
- **T41 trop haut.** 102 643 dossiers pour 97 378 allocataires publiés (+5,4 %), 283 534 enfants pour 268 481 (+5,6 %), et un écart qui baisse chaque année (+14,5 % en 2020). Les enfants comptés et payés dans l'année font 271 410, à +1,1 % : la CNSS semble ne compter que les enfants payés. P79 le vérifie pour les allocataires.
- **Des dates de naissance par défaut** : 500 veuves de 120 à 124 ans, quelques pensionnés de plus de 110 ans, 17 enfants nés après l'exercice. La requête les met en « âge inconnu » dès 120 ans ; P80 montre les dates en cause.
- **Plusieurs pensions.** Les doublons dans un même tableau sont rares (15 au plus). Être dans deux tableaux est normal (519 pensionnés de vieillesse sont aussi dans un autre tableau : rente d'accident, pension de veuve…) : on n'additionne jamais les tableaux pour compter des personnes (P77).

### Lot 6 (les écarts de T32 à T41)

- **T41 : seuls les enfants payés comptent.** Un enfant en cours au 31/12, de 21 ans au plus, et payé dans l'année ; l'allocataire est le dossier qui a au moins un tel enfant. En 2024 : 98 048 allocataires pour 97 378 publiés (+0,7 %), 271 577 enfants pour 268 481 (+1,2 %). Sans la condition de paiement, +5 % ; avec un paiement au dernier trimestre seulement, −1 % et −2 %. T41 s'écarte donc de R6 (payée ou non), qui reste vraie pour les pensions : un enfant sans certificat n'est pas radié, il n'est plus payé (P79).
- **Pas de dossiers saisis après coup.** Ne garder que les allocations saisies avant la publication de l'annuaire ne change presque rien (−0,2 %). Avant 2024, l'écart reste grand pour les allocataires (+5 % en 2023, +11 % en 2020) et baisse d'année en année : sans doute des chiffres publiés à l'époque, sur la base de ce moment-là, et des paiements saisis depuis pour des mois anciens (hypothèse non vérifiée). Sans effet pour la BI, qui calcule chaque exercice sur le cliché du début de l'année suivante (P79).
- **1900 est une date de naissance par défaut** : 503 bénéficiaires en 2024, dont 499 veuves, presque tous sur une même date. La règle « 120 ans et plus = âge inconnu » les écarte. Les autres très grands âges partagent leur date (plusieurs personnes par année de naissance, rarement un 1er janvier) : des dates approchées, laissées dans « 100 ans et plus ». 17 enfants nés en 2025 touchent une allocation dès 2024 : âge inconnu (P80).
- **Les suspensions ne font pas l'écart des veuves.** 250 veuves et veufs et 25 orphelins suspendus fin 2024, pour 1 052 et 1 508 de moins que l'annuaire : R6 reste (une pension suspendue ne compte pas). Le stock de suspendues baisse de 2022 à 2023 (T34 413 → 264), l'année du décrochage. La piste des radiations saisies après coup reste à tester par A06 et A07 (P81).

### Lot 7 (T42 à T49, nouveaux bénéficiaires)

- **La première saisie fait l'entrée.** Une personne entre l'année où sa première prestation du tableau est saisie. En 2024 : 2 463 nouveaux retraités pour 2 489 publiés (−1,0 %), veuves et veufs +0,7 %, orphelins +0,5 %, rentes d'orphelins et de veuves exactes, ascendants 15 pour 13. La notification donne 4 à 5 % de moins (P82).
- **Une révision n'est pas une entrée.** Des retraités reçoivent une nouvelle pension alors qu'ils en avaient une : 18 à 47 par an jusqu'en 2024, 220 en 2025. Les compter gonflerait la loi des entrées (P82).
- **Rentes viagères (T48) : 39 premières fois pour 44 publiées**, et 68 rentes saisies : une rente viagère est souvent recalculée (révision du taux d'IPP). La CNSS compte peut-être une rente par accident ; P91 le teste. T45, lu comme le total des rentes AT/MP, retombe à 102 pour 105.
- **Coordination et sorties.** 234 nouveaux retraités de 2024 sont payés par une autre caisse : l'annuaire les compte, on les garde sans montant, comme en T32. Les nouveaux sortis avant le 31/12 sont rares (35 retraités) : ils comptent, c'est une entrée de l'année (P82, P85).
- **L'âge retombe par tranche, pas âge par âge.** Par tranche de 5 ans, les nouveaux retraités retombent sur le tableau 25 (55-59 ans 1 450 pour 1 429, 60-64 ans 941 pour 956, 65-69 ans exact). Âge par âge, les chiffres se décalent : la CNSS calcule sans doute l'âge à une autre date (la date d'effet ?). **Le sexe ne retombe pas** : 641 femmes pour 527 publiées, 1 865 hommes pour 1 959 (P83). Les grilles de l'Annuaire conviennent ; plus de la moitié des nouvelles veuves sont dans la dernière tranche, « 60 ans et plus » (P84).
- **Le rappel.** Une nouvelle pension est payée avec un rappel des mois d'avant l'année : 8 % du montant de l'année pour les retraités, 25 % pour les veuves, 39 % pour les orphelins, et deux fois les mois de l'année pour les rentes d'orphelins. On garde les mois de l'exercice (R4) et on montre le rappel à part : question au métier (P85).
- **Les services passés se calculent** : mois cotisés toujours remplis, 270 mois en médiane (dossiers ordinaires). En coordination, ils sont plus longs (342 en moyenne) et une colonne « cotisations autre caisse » (PE_COT_AUTRE) est remplie (P86).
- **La part CNSS en coordination.** PE_COT_AUTRE est un nombre, rempli pour toutes les pensions de coordination et presque jamais ailleurs ; ses valeurs (de 6 à 464) sont des mois, toujours plus petits que le total (280 mois dans l'autre caisse pour 362 au total). Le total compte donc sans doute les deux caisses : la part CNSS demandée par Kaya 2017 est le total moins PE_COT_AUTRE. Quand l'autre caisse paie (COOR_AC), elle a la plus grande part de la carrière (P88 à P90).
- **Le degré d'IPP de T48 se calcule.** Le taux d'incapacité des rentes viagères est toujours rempli, de 15 à 100, 24 en médiane en 2024. Aucune rente sous 15 % : en dessous, c'est l'allocation d'incapacité (P87, P72).
- **Rentes viagères par accident : non.** Une personne n'a qu'un accident qui donne une rente : compter par dossier donne les mêmes 39. L'écart avec les 44 publiées reste, petit. Les révisions de rentes (12 à 52 par an) dépassent souvent les nouvelles rentes (P91).

### Lot 8 (T50 à T57, évolution sur 5 ans)

- **Le bénéficiaire de l'année est la prestation liquidée.** Allocations de vieillesse liquidées en 2024 : 831 pour 833 publiées (−0,2 %) ; IJ de maternité : 3 890 pour 3 893, et de 2020 à 2024 à moins de 1 % sauf 2022. Les payées s'en écartent de 25 %. Kaya 2017 le voulait pour T54 et T55 ; la règle vaut pour T50 à T55 (P92).
- **Le montant suit la date de saisie du paiement.** Allocations de vieillesse : 497,8 M saisis en 2024 pour 497,7 M publiés (0,02 %), contre +3,9 % au mois payé ; prénatales −1,2 % contre −4,0 % ; allocations de survivants −3,9 % contre −5,5 %. La CNSS publierait ce qu'elle a versé dans l'année, rappels compris : la règle R4 (mois payé) est à revoir pour tous les montants ; P98 le vérifie code par code (P92).
- **Le détail par code.** L'allocation de survivants se liquide par ses parts (conjoint 81, orphelin 472, ascendant 18 en 2024), RAS ne sert plus ; les prénatales sont celles de la maternité (MAP 3 857, FAP 56) ; une IJ AT/MP se liquide en plusieurs fois (371 IJ pour 195 victimes) (P93). Les services passés des allocations de vieillesse se calculent : 96 mois en moyenne en 2024 (P94).
- **T56, par branche.** Au mois payé, AVS 47,24 Md pour 46,63 Md publiés (+1,3 %), PF 10,75 Md pour 11,18 Md (−3,8 %), AT/MP 0,76 Md pour 1,07 Md (−29 %). Les prestations en nature des AT/MP (139,7 M publiés) ne sont pas dans les débours ; les ajustements (AJ) sont des corrections, à laisser hors de T56 ; la ligne « Autres » reste vide (P95).
- **La date des montants (partie B).** Aucune date ne retombe partout. La saisie du paiement (ou son approbation, presque identique) fait mieux que le mois payé sur 11 lignes sur 16, et tombe juste sur les allocations de vieillesse, la majoration pour enfant et l'invalidité ; le mois payé ne fait mieux que sur la pension de vieillesse (+0,7 % contre +1,5 %). Le décaissement est trop souvent vide. Les rentes AT/MP restent 12 % sous le publié quelle que soit la date : des paiements AT/MP manquent ailleurs (P98).
- **L'annuaire est arrêté fin mars.** Les décès de retraités de 2024 que la base connaissait avant avril 2025 retombent sur le tableau 26 mois par mois : 444 pour 447, six mois exacts. D'où la règle R13 : calculer l'exercice N sur le cliché de fin mars N+1 (P99).
- **La vue des décédés de la DSI.** V_PRESTATAIRES_DECEDES_BI prend toutes les prestations notifiées dont l'ayant droit a une date de décès, sans filtre de type ni d'année : plus large que T57 (pension directe, rentier et sinistré, allocataire), sans date de déclaration. Elle expose aussi le nom, l'adresse et le téléphone, que la BI n'a pas à lire (P101).
- **Les accidents mortels.** DOS_DECES_IMMEDIAT n'est rempli que depuis 2024 ; les sinistrés se lisent par le décès de la victime dans l'année de l'accident (8 à 23 par an), presque toujours suivi d'une rente de survivant (P100).
- **T57, les décès.** La base n'a que la date du décès, pas de date de déclaration ; un indicateur DOS_DECES_IMMEDIAT existe sur les dossiers AT, et une vue V_PRESTATAIRES_DECEDES_BI a été préparée par la DSI (P96). En 2024 : 747 pensionnés décédés pour 648 publiés, 619 retraités pour 447. Les décès se déclarent tard (2025 n'a que la moitié des années d'avant) : la CNSS ne voit que ceux connus quand elle publie ; P99 le vérifie (P97).

### Lot 9 (T31, les indus)

- **La norme de 2 % est tenue de loin.** En 2024 : 229,5 M de trop-perçus pour 58,7 Md payés, un taux d'indus de 0,39 % (PVID 0,47 %, AT/MP 0,47 %, PF 0,05 %), quelle que soit la date des paiements. Le mois le plus haut : PVID 0,89 % en novembre (P105).
- **Les statuts se lisent.** C = soldé (un moins-perçu C est entièrement payé, un trop-perçu C entièrement repris) ; A = en cours ; R = rejeté ou annulé (presque rien payé). Depuis 2017, les trop-perçus restent au statut A, repris par précomptes : le statut ne dit plus si un indu est recouvré. T31 garde tout sauf R (P103).
- **Les avances dominent les ajustements** (5,4 Md en 2024) mais ne sont pas des indus. Les moins-perçus de la PF fondent (56 362 en 2023, 1 384 en 2025). Les trop-perçus sur convention bondissent en 2025 (758 contre 15) (P102).
- **Les débours d'ajustement versent de l'argent** : avances (373 M en 2024), rappels de moins-perçus (408 M), rachats de rente AT/MP (21,5 M, proches des 25,4 M publiés) ; ils reprennent avances et trop-perçus remboursés. Les rachats de rente devraient entrer dans T56 (P104).
- **Les trop-perçus n'ont pas de prestation liée** : leur branche se lit par le dossier. Presque aucun ne vient d'un décès (7 en 2024), alors que 175 décès de retraités de 2024 ont été déclarés tard : ces pensions versées après la mort ne deviennent pas des trop-perçus, ou sont reprises autrement (P106).

## Règles proposées après les lots 1 et 2

À faire valider par la direction des prestations avant d'écrire les nouvelles requêtes.

Mise à jour du 30/10/2026 après la comparaison avec l'annuaire statistique de la CNSS 2024 (dossier [comparaison_annuaire_cnss_2024](../comparaison_annuaire_cnss_2024/README.md)) : R1 à R5 confirmées par les chiffres publiés, R6 corrigée, R7 revue. Les règles des dossiers (R10 à R12, D1 à D14), des indus et des contrôles n'y sont pas testées.

| N° | Sujet | Règle proposée | Tableaux |
| --- | --- | --- | --- |
| R1 | Branches | V → PVID ; A → AT/MP ; F et M → PF. Confirmée par l'annuaire CNSS 2024 | Tous |
| R2 | Ligne CIPRES | D'après le code de prestation (correspondance ci-dessous). Confirmée pour les pensions par l'annuaire CNSS 2024 (une ligne par code) | Tous |
| R3 | Paiement | Débours de paiement au statut D ou P ; montant = DEB_MONTANT, jamais le net à payer de l'effet. Confirmée par l'annuaire CNSS 2024 : montants à 0,7–2,5 % près, branche des pensions à 1,3 %. Pour T32 à T41, le montant versé dans l'année aux bénéficiaires comptés (proposition : l'annuaire tombe entre ce montant et tout le payé, lot 5 P75) | T31, T32–T56 |
| R4 | Mois d'un paiement | Mois de DEB_DATE_EFFET (mois payé). Les montants annuels ainsi calculés retombent sur l'annuaire CNSS 2024. À décider après le lot 8 : la saisie du paiement (l'année où la CNSS le verse, rappels compris) retombe mieux sur l'annuaire sur 11 lignes sur 16, exactement pour les allocations de vieillesse ; le mois payé ne fait mieux que sur la pension de vieillesse (+0,7 % contre +1,5 %) (P92, P98). Question posée au point du 02/11/2026 ; en attendant, les requêtes ont un réglage date_montant ('MOIS' aujourd'hui, 'SAISIE' sinon), et rien d'autre n'en dépend | T31, T50–T56 |
| R5 | Personne comptée | L'ayant droit de la prestation (IND_ID_PREST), une fois par tableau ; pour les allocations familiales, l'enfant, et l'allocataire est le dossier (atelier de Kaya 2017). Confirmée pour les orphelins : l'annuaire CNSS 2024 les compte par leur âge, de 0 à 24 ans. Doublons rares dans un tableau (15 au plus, P77). Allocataires : le dossier qui a au moins un enfant compté, l'enfant comptant s'il a été payé dans l'année : +0,7 % d'allocataires et +1,2 % d'enfants face à l'annuaire en 2024 (P79) ; sans la condition de paiement, +5 % (P74) | T32–T49 |
| R6 | Pensionné au 31/12 | Prestation en cours au 31/12 (effet avant, ni radiée ni suspendue sauf réactivation) dont l'ayant droit est en vie, payée ou non ; pour les orphelins, moins de 25 ans (année moins année de naissance) ; une personne comptée une fois par tableau. Corrigée le 30/10/2026 : c'est la définition de l'annuaire CNSS 2024 (+1,5 % en 2024) ; exiger un paiement au 4e trimestre donnait 8 % de pensionnés de vieillesse de moins. Limite d'âge des orphelins à confirmer. Confirmée le 31/10/2026 par le lot 5 : T32 +0,9 %, T36 exact, T33 et T37 à T40 à quelques pourcents ; les pensions de coordination payées par une autre caisse (COOR_AC) sont comptées, sans montant (P78). T41 fait exception : seuls les enfants payés dans l'année comptent (P79). Reste : veuves et orphelins sous le publié depuis 2023 ; les suspensions, rares, ne l'expliquent pas (P81) ; A06 et A07 testent les radiations saisies après coup | T32–T41 |
| R7 | Nouveau pensionné | Prestation saisie dans l'exercice (PE_DATE_INSERT) ; une personne comptée une fois. Revue le 30/10/2026 : l'annuaire CNSS 2024 compte ainsi (4 090 pour 4 037 publiés) ; la notification, proposée avant, en donnait 3 888. À confirmer avec la direction des prestations. Confirmée le 01/11/2026 par le lot 7 : la première prestation de la personne dans le tableau, saisie dans l'exercice, retombe à 1 % près sur l'annuaire (T42 2 463 pour 2 489) ; une révision n'est pas une entrée (220 en 2025) ; rentes viagères 39 pour 44 (P91) | T42–T49 |
| R8 | Âge | Année de l'exercice moins année de naissance ; tranches de 5 ans regroupées selon la grille de chaque tableau ; 120 ans ou plus, âge négatif ou date absente : « âge inconnu » (l'année 1900 est une date par défaut : 503 bénéficiaires en 2024, P76, P80). Sexe 1 = hommes, 2 = femmes (P75) | T32–T49 |
| R9 | Services passés | Mois cotisés de la dernière prestation de la personne ; valeurs au-dessus de 600 écartées. Toujours remplis pour les nouveaux retraités (P86) ; en coordination, la seule part CNSS (Kaya 2017) : le total moins PE_COT_AUTRE, les mois dans l'autre caisse, rempli pour toutes les pensions de coordination et toujours plus petit que le total (P89, P90 ; proposition) | T42, T50 |
| R10 | Dossier déposé | Réception du dossier dans l'exercice | T25–T27 |
| R11 | Dossier liquidé | Dossier notifié dans l'exercice ; durée = notification − réception | T25–T27 |
| R12 | Dossier en instance | Réception sans notification au 31/12 | T25–T27 |
| R13 | Date du calcul | L'exercice N se calcule sur le cliché de fin mars N+1 : c'est la date à laquelle la CNSS arrête son annuaire. Les décès de retraités de 2024 connus de la base fin mars 2025 retombent mois par mois sur le tableau 26 (444 pour 447) ; la base d'aujourd'hui en compte 619 (P99). La même cause explique les veuves et les orphelins sous le publié (radiations saisies après coup). Proposition du 01/11/2026 | T25–T57 |

## Correspondance proposée entre codes de prestation et lignes CIPRES

À faire viser par la direction des prestations (P04).

| Tableaux | Ligne CIPRES | Codes |
| --- | --- | --- |
| T25, T28, T32, T42 | Pension de vieillesse | RPV, RPRA (pension de retraite anticipée, P34 ; rattachement à confirmer) |
| T25, T28, T33 | Pension d'invalidité | RPI |
| T25, T28, T34, T43 | Pension de veuf / veuve | RPSV, RPSF |
| T25, T28, T35, T44 | Pension d'orphelin | RPSE |
| T28, T36 | Pension d'ascendant | RPA |
| T25, T28, T50 | Allocation de vieillesse | RAV |
| T25, T28, T51 | Allocation de survivants | RAS, RASC, RASO, RASA |
| T25, T28 | Autres prestations de la branche | RBEC bonification pour enfant à charge, RME et RME-COL majorations pour enfant (CARFO, collectivités), RCCM contributions aux charges du ménage, RRC remboursement de cotisations (accessoires : montant à rattacher à la pension ?) |
| T26, T29, T37, T45, T48 | Rente viagère | ARI |
| T26, T29, T38, T45, T49 | Rente de veuf / veuve | ARSV, ARSF |
| T26, T29, T39, T45, T46 | Rente d'orphelin | ARSE |
| T26, T29, T40, T45, T47 | Rente d'ascendant | ARSA |
| T26, T29, T52 | Indemnités journalières AT/MP | AIJ |
| T26, T29, T53 | Allocation d'incapacité | AAI |
| T26, T29 | Autres prestations de la branche | Prestations en nature (frais médicaux AT/MP) |
| T27, T30, T41 | Allocations familiales | FAF |
| T27, T30, T54 | Indemnités journalières de maternité | MIJ |
| T27, T30, T55 | Allocations prénatales | FAP, MAP |

## Questions ouvertes

**À la DSI**

- Les débours liés à un trop-perçu ont les deux signes (de −6,1 M à +6,6 M en 2024) : lesquels sont des retenues, lesquels des versements ?
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
- Les prestations en nature des AT/MP (139,7 M publiés en 2024) ne sont pas dans les débours, et les rentes AT/MP y sont 12 % sous le publié : où sont payés les autres montants ? La vue V_PRESTATAIRES_DECEDES_BI, faite pour la BI, contient le nom, le prénom, l'adresse et le téléphone des prestataires décédés : peut-on retirer ces colonnes ? DOS_DECES_IMMEDIAT sera-t-il rempli pour les accidents d'avant 2024 ?
- 120 518 dossiers n'ont pas de catégorie (DOS_CATEGORIE vide) : sont-ils tous des dossiers ordinaires ? Que veut dire la catégorie CNSS_AC, à côté de COOR_AC et COOR_CNSS ?
- L'année 1900 sert-elle de date de naissance par défaut (503 bénéficiaires en 2024, dont 499 veuves) ? Les dates partagées par plusieurs personnes nées la même année sont-elles des dates approchées (« né vers ») ?

**À la direction des prestations**

- T26 : une déclaration d'accident sans arrêt ni soin est-elle un dossier à liquider (en instance tant qu'il n'a rien donné) ? Les soins entrent-ils dans T26, liquidés au premier remboursement ? La durée d'une rente part-elle de la réception de la demande ou du certificat de consolidation (atelier de Kaya 2017) ? Qu'attend une IJ pendant les 7 mois qui suivent la fin de l'arrêt de travail ?
- T27 : un enfant ajouté au dossier d'un allocataire est-il un dossier déposé et liquidé ? Une grossesse compte-t-elle sur les deux lignes (IJ de maternité et allocations prénatales), ou une seule fois ?
- Un dossier déposé est-il une réception ? Un dossier liquidé, un dossier notifié ? Un dossier en instance, une réception sans notification ?
- Un dossier compte-t-il l'année de son dépôt ou l'année de sa liquidation ?
- Les dossiers de coordination payés par une autre caisse et les dossiers transférés entrent-ils dans les tableaux de la CNSS ? L'annuaire de la CNSS compte les pensions COOR_AC (2 901 pensions de vieillesse fin 2025, sans paiement CNSS) : on les compte dans l'effectif, sans montant. Faut-il les garder dans la pension moyenne (près de 9 % plus basse) ?
- T32 à T41 : le montant d'un tableau est-il ce qui a été versé dans l'année aux bénéficiaires comptés au 31/12, ou tout ce qui a été versé, décédés et radiés compris ? L'annuaire de la CNSS 2024 tombe entre les deux.
- Coordination : PE_COT_AUTRE est-il bien le nombre de mois cotisés dans l'autre caisse, compris dans PE_MOIS_COTISATION ? On en tire la part CNSS des services passés (T42, atelier de Kaya 2017).
- T48 : comment l'annuaire compte-t-il ses 44 nouvelles rentes viagères de 2024 ? La base en a 39 nouvelles (68 avec les révisions).
- Montants : l'annuaire compte-t-il un paiement l'année où il est versé (rappels compris) ou l'année du mois qu'il paie ? Pour les allocations de vieillesse de 2024, la date de saisie du paiement retombe à 0,02 % près ; elle fait mieux sur 11 lignes sur 16 (P98).
- À quelle date l'annuaire est-il arrêté ? Les décès de 2024 connus fin mars 2025 retombent exactement sur le tableau 26 : la BI calculerait chaque exercice sur le cliché de fin mars de l'année suivante (R13).
- T57 : l'annuaire publie 89 décès de pensionnés ascendants en 2024 pour 59 pensions d'ascendant en cours : d'où vient ce chiffre ?
- T50 à T53 : un bénéficiaire de l'année est-il celui dont l'allocation ou l'indemnité est liquidée dans l'année (comme Kaya 2017 le veut pour T54 et T55), ou payée ? T56 : que met-on dans la ligne « Autres » ? T57 : les assurés décédés « déclarés » se comptent-ils à la date du décès ou de sa déclaration ? Un sinistré est-il une victime d'accident du travail morte de son accident ?
- T42 : d'où vient le sexe des nouveaux retraités du tableau 25 de l'annuaire (527 femmes publiées pour 2024, 641 dans la base) ? À quelle date l'âge y est-il calculé ?
- T42 à T49 : T45 « nouveaux crédirentiers de la branche AT/MP » est-il le total des nouvelles rentes (viagères et ayants droit) ? T47 « nouveaux crédirentiers ascendants » est-il bien celui des rentes AT/MP (l'atelier de Kaya 2017 le range avec elles) ? Une révision ou une reliquidation fait-elle un nouveau pensionné ? Le montant d'un nouveau comprend-il le rappel des mois d'avant l'exercice ?
- T41 : un enfant d'allocataire compte-t-il s'il n'a rien perçu dans l'année (certificat attendu, par exemple) ? La base ne retombe sur l'annuaire de la CNSS que si l'on compte les seuls enfants payés (+0,7 % d'allocataires en 2024, +5 % sinon) : est-ce bien votre règle ? L'allocataire est-il le dossier ?
- La correspondance des codes ci-dessus est-elle juste (retraite anticipée, accessoires de pension) ?
- Qui est un pensionné au 31 décembre ? L'annuaire de la CNSS 2024 compte la pension en cours ce jour-là, titulaire en vie, payée ou non (règle R6 revue) : la retient-on aussi pour les tableaux CIPRES ? Un nouveau pensionné est-il compté à la saisie de sa pension, comme dans l'annuaire, ou à sa notification (règle R7) ?
- Pour les orphelins, compte-t-on l'orphelin (et non le tuteur payé) ? L'annuaire de la CNSS 2024 le fait (orphelins de 0 à 24 ans).
- T57 « décédés par branche » : s'agit-il des bénéficiaires décédés (fin de droit) ou des assurés décédés qui ouvrent un droit aux survivants ?
- T31 : une pension versée après un décès déclaré tard devient-elle un trop-perçu ? En 2024, seuls 7 trop-perçus suivent un décès, alors que 175 décès de retraités ont été déclarés après mars 2025. Pourquoi les moins-perçus des prestations familiales ont-ils presque disparu (56 362 en 2023, 1 384 en 2025) ?
- T56 : les rachats de rente AT/MP (versés par débours d'ajustement, 21,5 M en 2024) sont-ils des prestations servies, comme dans l'annuaire (25,4 M) ? Et les rappels de moins-perçus ?
- T31 : un trop-perçu se compte-t-il le mois où il est établi ? Les moins-perçus (rappels) restent-ils dans le tableau, alors que Kaya 2017 dit qu'ils ne sont pas des indus ?
- Indus : un remboursement de trop perçu (REM-TP) est-il un indu ? Les avances (AV) et les mois de pension gratuits (MPG) en sont-ils exclus ?
- Une prestation est-elle payée quand l'effet est décaissé, ou dès qu'elle est validée ? Les montants de l'année se comptent-ils au mois payé ou à la date du paiement ?
- 8 447 pensions d'orphelin sont actives mais sans aucun paiement au 4e trimestre 2025 : ce sont sans doute des orphelins de 25 ans et plus, que l'annuaire de la CNSS ne compte pas. La limite est-elle 25 ans ? Faut-il les radier ?
- 69 pensionnés de vieillesse payés ont 100 ans ou plus, et 215 pensionnés actifs sont décédés : un contrôle d'existence est-il prévu ?
- Une allocation de vieillesse peut-elle correspondre à 489 mois cotisés (40 ans) ?
