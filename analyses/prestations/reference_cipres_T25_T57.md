# Tableaux CIPRES T25 à T57 — ce que dit l'Annuaire

Source : `cnss_files/Annuaire_statistique_CIPRES_finalise V110920.pdf`. Les définitions sont aux pages 10 à 15 (liste des tableaux, « Prestations sociales ») et les maquettes aux pages 33 à 48. Le texte a été extrait le 26/10/2026 avec `pdftotext -layout`.

Les branches de l'Annuaire sont PVID (pensions de vieillesse, d'invalidité et de décès), AT/MP (accidents du travail et maladies professionnelles) et PF (prestations familiales, maternité comprise). T56 et T57 parlent de PF, AT/MP, AVS et « autres ».

## Définitions du glossaire utiles au domaine

| Terme | Définition de l'Annuaire |
| --- | --- |
| Âge | Année de constatation moins année de naissance |
| Bénéficiaire | Toute personne percevant une prestation du chef de son activité ou de celle d'autrui (ayant droit ou tiers) |
| Bénéficiaire d'allocation unique | Personne à laquelle une prestation est payée en une seule fois |
| Allocation | Prestation versée soit périodiquement (PF), soit en une seule fois (vieillesse) |
| Durée de liquidation | Temps entre la date de dépôt d'un dossier et celle de la notification des droits au bénéficiaire |
| Dossier payé | Dossier dont les droits échus ont été payés au bénéficiaire |
| Contrôle a posteriori | Vérification sur un dossier de prestation, après le paiement |
| Trop perçu | Paiement de trop, au détriment de l'organisme |
| Moins perçu | Différence négative entre le droit dû et le montant payé |
| Rente | Prestation périodique AT/MP versée à la victime ou à ses ayants droit |
| Pensionné veuf/veuve, orphelin, ascendant | Conjoint, enfant ou parent bénéficiant d'une pension du fait des droits de l'assuré décédé |
| Services passés | Total des périodes d'activité salariée |
| Décédé | Personne dont la mort a été constatée par un acte d'état civil |

## Tableaux

| Tableau | Titre | Ce que demande l'Annuaire | Requête DTM actuelle | Écart supposé (à vérifier, lot 1) |
| --- | --- | --- | --- | --- |
| T25, T26, T27 | Dossiers PVID, AT/MP, PF de l'exercice selon la durée de liquidation | Par type de prestation : dossiers déposés ; liquidés en moins de 15 jours, de 15 à 45 jours, en plus de 45 jours ; liquidés ; en instance ; rejetés. Norme : moins de 45 jours. Fin d'exercice | `dossier.sql` (DTM_DOSSIER) | H1, H2, H3, H4, H5 |
| T28, T29, T30 | Dossiers PVID, AT/MP, PF contrôlés a posteriori, par type de prestation | Dossiers payés (PF : bénéficiaires payés) et dossiers contrôlés par trimestre. Norme : au moins 5 % | `controle_prestation.sql` (DTM_CONTROLE_PRESTATION) ; taux dans `indicateurs_prestations.sql` | H11 |
| T31 | Prestations indues par branche et par mois | Par mois et par branche : prestations payées, trop perçus, moins perçus. Norme : 2 % au plus | `prestation_indue.sql` (DTM_PRESTATION_INDUE) ; payées dans DTM_PRESTATION | H12 |
| T32 à T36 | Pensionnés PVID par tranche d'âge et sexe : vieillesse, invalidité, veufs/veuves, orphelins, ascendants | Effectif et montant à la fin de l'exercice, par tranche d'âge et par sexe | `prestation.sql` (DTM_PRESTATION) | H6, H7, H8, H9 |
| T37 à T40 | Bénéficiaires de rente viagère et crédirentiers veufs, orphelins, ascendants (AT/MP) | Idem | `prestation.sql` | H6 à H9 |
| T41 | Allocataires PF par tranche d'âge et sexe | Idem | `prestation.sql` | H6 à H9 |
| T42 à T49 | Nouveaux pensionnés et crédirentiers de l'exercice | Effectif et montant par tranche d'âge et sexe ; T42 ajoute le total des services passés, T48 le degré moyen d'IPP | `prestation.sql` (NB_NOUVEAUX, NB_MOIS_COTISES, TAUX_IPP_MOYEN) | H8, H9, H10 |
| T50 à T55 | Évolution sur 5 ans : allocations de vieillesse (avec nombre moyen de services passés), allocations de survivants, IJ AT/MP, allocations d'incapacité, indemnités de maternité, allocations prénatales | Effectif et montant par année, par sexe (T54 et T55 : femmes seulement) | `prestation.sql` | H6, H7, H8, H10 |
| T56 | Prestations servies par branche, 5 ans | Montants par branche : PF, AT/MP, AVS, autres | `prestation.sql` | H5, H6, H7 |
| T57 | Décédés par branche, 5 ans | Nombre de décédés par branche : PF, AT/MP, AVS, autres | `prestation.sql` (NB_DECES) | Définition à préciser |

## Tranches d'âge demandées par l'Annuaire

Chaque tableau a sa propre grille. Le DTM n'en a qu'une (`DTM.DIM_TRANCHE_AGE`).

| Tableaux | Tranches |
| --- | --- |
| T32 (vieillesse) | Moins de 55, 55-59, 60-64 … 95-99, 100 et plus |
| T33 (invalidité) | Moins de 25, 25-29 … 55-59, 60 et plus |
| T34, T45 (veufs ; nouveaux crédirentiers AT/MP) | Moins de 15, 15-19 … 95-99, 100 et plus |
| T35, T39 (orphelins) | Moins de 5, 5-9, 10-14, 15-19, 20-24, 25 et plus |
| T44, T46 (nouveaux orphelins) | Moins de 5, 5-9, 10-14, 15-19, 20 et plus |
| T36, T42, T47 (ascendants ; nouveaux pensionnés de vieillesse) | Moins de 50, 50-54 … 95-99, 100 et plus |
| T40 (crédirentiers ascendants) | Moins de 50, 50-54 … 75-79, 80 et plus |
| T37, T38 (rente viagère ; crédirentiers veufs) | Moins de 20, 20-24 … 95-99, 100 et plus |
| T41 (allocataires PF) | Moins de 20, 20-24 … 75-79, 80 et plus |
| T43 (nouveaux pensionnés veufs) | Moins de 20, 20-24 … 55-59, 60 et plus |
| T48, T49 (nouvelles rentes viagères ; nouveaux crédirentiers veufs) | Moins de 20, 20-24 … 60-64, 65 et plus |

## Hypothèses tirées de la lecture des requêtes DTM

Elles restent à confirmer par le lot 1 (requêtes sur la base source USER_DWH) avant d'en faire des constats.

| N° | Hypothèse | Requête | Vérifiée par |
| --- | --- | --- | --- |
| H1 | DTM_DOSSIER ne part que des dossiers qui ont au moins un débours : les dossiers déposés, en instance et rejetés sont sous-comptés | `dossier.sql`, CTE base | P09 |
| H2 | Un dossier est rattaché à l'année de son ouverture, pas à l'année de sa liquidation : un dossier déposé en 2024 et liquidé en 2025 compte dans les liquidés de 2024 | `dossier.sql`, ID_TEMPS | Question métier |
| H3 | Un délai nul ou négatif n'entre dans aucune tranche de durée de liquidation | `dossier.sql`, NB_INF_15J | P10 |
| H4 | Le sens de DOS_STATUT (F liquidé, A en instance) n'est pas confirmé | `dossier.sql` | P02, P06 |
| H5 | La branche M est « Maternité » dans `dossier.sql` et « Maladie » dans `prestation.sql` ; l'Annuaire range la maternité dans PF | `dossier.sql`, `prestation.sql` | P02, P05 |
| H6 | Le net à payer d'un effet est répété sur chacun de ses débours, donc compté plusieurs fois dans MONTANT_NET | `prestation.sql`, jointure FAIT_EFFET | P08 |
| H7 | Les débours annulés sont comptés comme des prestations payées | `prestation.sql` | P07 |
| H8 | NB_BENEFICIAIRES est un nombre distinct par mois : il ne s'additionne pas sur l'année, et ne donne pas l'effectif présent au 31 décembre demandé par T32 à T41 | `prestation.sql` | Conception |
| H9 | Une seule grille de tranches d'âge pour les 11 grilles de l'Annuaire | `prestation.sql`, DIM_TRANCHE_AGE | P14 |
| H10 | Les mois cotisés d'un dossier sont additionnés à chaque débours, donc gonflés | `prestation.sql`, NB_MOIS_COTISES | Lot 2 |
| H11 | Dans DTM_CONTROLE_PRESTATION, les dossiers payés ne sont comptés que parmi les dossiers contrôlés : le taux de contrôle ne peut pas être calculé. DTM_INDICATEURS_PRESTATIONS calcule ce taux sur les ajustements, pas sur les contrôles | `controle_prestation.sql`, `indicateurs_prestations.sql` | P13 |
| H12 | Tout ajustement qui n'est pas un trop perçu est classé « moins perçu » ; la branche M est exclue des indus | `prestation_indue.sql` | P11 |
| H13 | Pour les pensions de survivants et d'orphelins, la personne comptée (âge, sexe) est celle qui reçoit le paiement, pas forcément l'ayant droit | `prestation.sql`, IND_ID_BENEF | P16 |
| H14 | DTM_DOSSIER lit `DTM.DIM_TYPE_PRESTATION`, qui n'est pas chargée par la configuration des dimensions | `dossier.sql` | P18 |

## État des hypothèses après le lot 1 (26/10/2026)

| N° | État | Ce que montrent les données |
| --- | --- | --- |
| H1 | Confirmée | En 2025, DTM_DOSSIER ne garde que 33 % des dossiers AT/MP ouverts, 61 % en PF, 83 % en vieillesse (P09) |
| H2 | Question métier | Année de dépôt ou année de liquidation |
| H3 | Écartée | 6 délais nuls et aucun négatif en 2025 (P10). Le vrai problème est H4 |
| H4 | Confirmée, plus grave | A = dossier actif, F = fermé : 61 632 dossiers de vieillesse sont au statut A (P02). DTM_DOSSIER ne trouve donc que 310 dossiers « liquidés » ouverts en 2025, pour 15 479 notifiés (P10) |
| H5 | Tranchée | M = prestations de maternité, branche PF ; aucune prestation en nature en M (P02, P05). Le libellé « Maladie » de `prestation.sql` est faux, et M doit rejoindre PF dans T27, T30, T31, T56 et T57 |
| H6 | Confirmée | Net à payer 2025 vu par le DTM : 222,9 Md, pour 58,5 Md réels (× 3,8) (P08) |
| H7 | Écartée | Aucun paiement annulé ; 410 ajustements annulés (P07). Mais DEB_STATUT (7 valeurs sans libellé) n'est pas filtré |
| H8 | Conception | Inchangée ; aggravée par N2 ci-dessous |
| H9 | Nuancée | La source a 21 tranches de 5 ans : les 11 grilles CIPRES s'obtiennent par regroupement (P14) |
| H10 | Abandonnée | Le contenu du DTM n'est plus analysé ; les mois cotisés sont étudiés depuis la source (lot 2, P29–P30) |
| H11 | Confirmée | 115 contrôles en 2025 pour 155 206 dossiers payés (0,07 %), plus aucune saisie depuis juillet 2025 ; DEB_VERIFIE marque 19 % des débours de vieillesse, ce n'est pas un contrôle (P06, P13) |
| H12 | Confirmée en partie | Le classement suit les libellés, sauf REM-TP (remboursement d'un trop perçu, compté comme un nouvel indu). La branche M est exclue à tort (P11) |
| H13 | Confirmée | Pour les orphelins (RPSE, RASO, ARSE), la personne payée est le tuteur, jamais l'orphelin (P16) |
| H14 | Écartée | DIM_TYPE_PRESTATION existe : 79 lignes faites à la main (P18) |

Constats nouveaux :

| N° | Constat | Requête |
| --- | --- | --- |
| N1 | La base source est une copie arrêtée au 02/02/2026, et le DWH en est la copie complète (cliché 092026) | P01, P17 |
| N2 | 62 % des paiements PF de 2025 (1 993 982) et 23 % des paiements de vieillesse n'ont pas d'effet : DTM_PRESTATION, qui prend la personne payée, la DR et le service dans l'effet, n'a pour eux ni bénéficiaire, ni sexe, ni âge | P07 |
| N3 | La date de décaissement manque pour 80 % des paiements PF et 43 % des paiements de vieillesse de 2025 | P07 |
| N4 | La catégorie de dossier décrit la coordination avec d'autres caisses (3 326 dossiers V payés par une autre caisse, 158 transférés), pas le type de pension | P03 |
| N5 | Les réceptions sont plus nombreuses que les dossiers ouverts (V 2025 : 4 671 contre 3 638), et les rejets ne sont presque pas saisis (486 en tout) | P09 |
| N6 | Durée de liquidation 2025 (médiane) : V 62 j, AT/MP 156 j, PF 13,5 j, maternité 17 j ; plus de 45 jours pour 61 % des dossiers V et 98 % des dossiers AT/MP | P10 |
| N7 | Les étapes sont des étapes de calcul du montant, pas des événements de vie : elles ne donnent pas les pensionnés présents au 31/12 | P12 |
| N8 | Deux codes d'allocation prénatale : FAP (branche F) et MAP (branche M) | P04 |
| N9 | Les décès saisis tombent de 1 100–1 360 par an (2019–2024) à 487 en 2025 : saisie en retard | P15 |
