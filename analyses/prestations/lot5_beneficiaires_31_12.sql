-- =============================================================================
-- LOT 5 — Contrôles pour T32 à T41 (bénéficiaires au 31/12, par âge et sexe)
-- Rédigé et exécuté le 31/10/2026 ; résultats dans resultats/lot5_*.tsv.
-- Sur la base source, schéma USER_DWH, comme les lots 1 à 4.
-- Exécution : en mode script (F5) dans SQL Developer ; comptages et sommes
-- seulement. P74 et P75 lisent DEBOURS (paiements des pensions, rentes et
-- allocations familiales) : plusieurs minutes chacune.
-- Données nominatives : jamais lues. Sexe, date de naissance et date de décès
-- ne servent qu'à compter.
-- Pourquoi ce lot : T32 à T41 répartissent par tranche d'âge et par sexe les
-- bénéficiaires présents en fin d'exercice, avec les montants servis (Annuaire
-- p. 13 et 14, maquettes p. 37 à 41). Les règles sont prêtes et la définition
-- du pensionné a été confirmée par l'annuaire de la CNSS (R5, R6, R8). Ce lot
-- tranche ce qui reste :
--   · le stock de chaque tableau, de 2020 à 2025, et pour T41 qui compter
--     comme allocataire                                              → P74
--   · le montant : payé dans l'année aux bénéficiaires comptés, ou tout ce
--     qui a été payé dans l'année                                    → P75
--   · l'âge, le sexe et les grilles de l'Annuaire                   → P76
--   · les personnes qui ont plusieurs pensions                      → P77
--   · la coordination : seule la part CNSS compte (Kaya 2017)       → P78
-- Le périmètre de chaque tableau (règle R2) :
--   T32 RPV, RPRA · T33 RPI · T34 RPSV, RPSF · T35 RPSE · T36 RPA ·
--   T37 ARI · T38 ARSV, ARSF · T39 ARSE · T40 ARSA · T41 FAF.
-- Chiffres publiés pour comparer (annuaire de la CNSS 2024, au 31/12) :
--   pensions (tableau 18, de 2020 à 2024) : vieillesse 25 161 / 26 415 /
--   27 873 / 29 042 / 30 730 ; invalidité 114 / 111 / 115 / 116 / 119 ;
--   veuves 19 309 / 19 964 / 20 734 / 20 832 / 21 438 ; veufs 225 / 249 / 274
--   / 298 / 329 ; orphelins 8 982 / 8 789 / 8 667 / 8 687 / 9 478 ; ascendants
--   48 / 53 / 59 / 54 / 59 ; anticipée 69 / 66 / 68 / 75 / 78.
--   rentes AT/MP (tableau 80) : viagères 1 303 / 1 278 / 1 270 / 1 246 /
--   1 255 ; veuves 308 / 296 / 294 / 283 / 283 ; veufs 1 / 2 / 3 / 2 / 2 ;
--   orphelins 492 / 421 / 399 / 379 / 485 ; ascendants 185 / 187 / 193 / 196 /
--   207.
--   prestations familiales (tableau 38) : allocataires 84 401 / 88 587 /
--   91 085 / 93 233 / 97 378 ; enfants 245 540 / 254 013 / 257 676 /
--   257 461 / 268 481 (tous de 21 ans au plus, tableau 36).
--   montants 2024 : vieillesse 38 235 880 033 (tableau 19 ; hommes 24 627,
--   femmes 6 103) ; veuves, veufs, orphelins, ascendants 7 047 164 871
--   (tableau 20) ; invalidité 113 603 730 (tableau 21) ; enfants
--   d'allocataires 8 236 741 500 (tableau 36).
-- Numérotation : suite du lot 4, P74 à P78. La comparaison avec l'annuaire
-- (analyses/comparaison_annuaire_cnss_2024) a ses propres numéros (A01 à A08).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- P74 — Le stock au 31/12 de chaque tableau, de 2020 à 2025
-- Question : combien de bénéficiaires compte chaque tableau, et qui compter
--            comme allocataire en T41 ?
-- Objectif : pour chaque année et chaque tableau : prestations en cours au
--            31/12 (effet avant, ni radiée ni suspendue sauf réactivation,
--            ayant droit en vie, règle R6) ; les mêmes avec la limite d'âge
--            (orphelins de moins de 25 ans en T35 et T39, enfants de 21 ans au
--            plus en T41) ; personnes distinctes comptées ; personnes payées
--            dans l'année. Pour T41 : dossiers, titulaires des dossiers et
--            personnes payées distincts.
-- Lecture  : les personnes comptées doivent retomber sur les chiffres publiés
--            (en-tête du lot), à quelques pourcents près. Pour T41, la colonne
--            qui retombe sur les allocataires publiés (97 378 en 2024) dit qui
--            est l'allocataire : le dossier (Kaya 2017), son titulaire ou la
--            personne payée ; les enfants doivent retomber sur 268 481.
-- Tableaux : T32 à T41.
-- Résultat : Exécutée le 31/10/2026 ; détail dans
--            resultats/lot5_P74_stock_2020_2025.tsv. Personnes comptées au
--            31/12 de 2020 / 2021 / 2022 / 2023 / 2024 / 2025 :
--            · T32 25 273 / 26 587 / 28 077 / 29 265 / 31 093 / 32 840 ;
--            · T33 111 / 109 / 119 / 121 / 125 / 123 ;
--            · T34 19 869 / 20 567 / 21 154 / 20 258 / 20 715 / 20 851 ;
--            · T35 9 686 / 9 535 / 9 276 / 8 108 / 7 970 / 7 311 (pensions
--              d'orphelins en cours, tous âges : 13 769 en 2024) ;
--            · T36 58 / 60 / 65 / 59 / 59 / 60 ;
--            · T37 1 358 / 1 325 / 1 305 / 1 238 / 1 236 / 1 226 ;
--            · T38 319 / 310 / 312 / 293 / 288 / 287 ;
--            · T39 475 / 482 / 497 / 450 / 454 / 439 ;
--            · T40 225 / 218 / 230 / 214 / 214 / 207 ;
--            · T41 dossiers 96 666 / 98 934 / 101 036 / 101 222 / 102 643 /
--              102 969 et enfants 264 178 / 271 585 / 278 386 / 280 288 /
--              283 534 / 282 036. En 2024 : titulaires 102 637, bénéficiaires
--              du paiement (IND_ID_BENEF) 101 683, enfants payés dans l'année
--              288 347.
--            · Personnes payées dans l'année 2024 : T32 28 775, T34 20 211,
--              T35 6 224.
-- Constat  : · Écart au publié en 2024 : T32 +0,9 % (31 093 contre 30 808,
--              vieillesse et anticipée ; moins de 0,5 % de 2020 à 2023) ; T33
--              +5 % (125 / 119) ; T36 exact (59) ; T37 −1,5 % ; T38 +1,1 % ;
--              T39 −6,4 % ; T40 +3,4 %. La règle R6 est la bonne pour les
--              pensions et les rentes.
--            · Payée ou non : en 2024, 27 670 pensionnés T32 comptés ont été
--              payés dans l'année (28 775 moins 1 105 payés non comptés, P75),
--              bien moins que les 30 808 publiés. La CNSS compte aussi les
--              pensions sans paiement dans l'année (R6).
--            · T34 et T35 décrochent depuis 2023 : +1,7 % et +7,8 % en 2020,
--              −4,8 % et −15,9 % en 2024 (veuves et veufs 20 715 contre
--              21 767 ; orphelins 7 970 contre 9 478). C'était déjà le constat
--              de la comparaison avec l'annuaire (A01) : A06 et A07, écrites
--              dans analyses/comparaison_annuaire_cnss_2024, testent les
--              radiations saisies après coup ; P81 teste les suspensions.
--            · Orphelins : 5 774 pensions d'orphelins en cours en 2024 ont 25
--              ans ou plus (13 769 − 7 995), mais ne sont presque jamais
--              payées (P75 : 129 payés non comptés). La limite de 25 ans est
--              appliquée par le paiement, pas par la radiation : elle est
--              indispensable dans la requête.
--            · T41 trop haut : dossiers +5,4 % en 2024 (102 643 contre
--              97 378), enfants +5,6 % (283 534 contre 268 481), et l'écart
--              baisse chaque année (+14,5 % en 2020). Ni le dossier, ni le
--              titulaire, ni le bénéficiaire du paiement ne retombe sur
--              97 378. En revanche, les enfants comptés et payés dans l'année
--              font 271 410 (288 347 − 16 937, P75), à +1,1 % du publié : pour
--              les allocations familiales, la CNSS semble ne compter que les
--              enfants payés. À vérifier pour les allocataires : P79.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           p.DOS_CODE,
           pm.tableau,
           p.IND_ID_PREST,
           p.IND_ID_BENEF,
           d.IND_ID                                               AS ind_dossier,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre             pm ON pm.code   = p.TPE_CODE
    JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID   = p.IND_ID_PREST
),
paie AS (
    SELECT DISTINCT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2020-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.DOS_CODE,
           pe.IND_ID_PREST,
           pe.IND_ID_BENEF,
           pe.ind_dossier,
           a.annee - pe.annee_naissance                           AS age,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                THEN 1 ELSE 0 END                                 AS en_cours,
           CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END       AS paye
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
),
compte AS (
    SELECT b.*,
           CASE WHEN en_cours = 1
                 AND (tableau NOT IN ('T35', 'T39') OR age < 25)
                 AND (tableau <> 'T41' OR age <= 21)
                THEN 1 ELSE 0 END                                 AS compte
    FROM base b
)
SELECT annee,
       tableau,
       SUM(en_cours)                                              AS prestations_en_cours,
       SUM(compte)                                                AS prestations_comptees,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS personnes_comptees,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN DOS_CODE END)                     AS dossiers_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN ind_dossier END)                  AS titulaires_comptes,
       COUNT(DISTINCT CASE WHEN compte = 1 AND tableau = 'T41'
                           THEN IND_ID_BENEF END)                 AS payes_comptes,
       COUNT(DISTINCT CASE WHEN paye = 1 THEN IND_ID_PREST END)   AS personnes_payees_annee
FROM compte
GROUP BY annee, tableau
ORDER BY tableau, annee;


-- -----------------------------------------------------------------------------
-- P75 — Le montant : payé aux bénéficiaires comptés, ou tout ce qui est payé
-- Question : quel montant mettre en face de l'effectif ?
-- Objectif : pour 2024 et 2025, par tableau et par sexe de l'ayant droit :
--            bénéficiaires comptés au 31/12 (comme P74) et montant qui leur a
--            été payé dans l'année ; montant payé dans l'année à toutes les
--            prestations du tableau, y compris celles arrêtées en cours
--            d'année (décès, radiation, fin de droits).
-- Lecture  : comparer 2024 aux montants publiés (en-tête du lot). Si
--            « montant des comptés » retombe sur l'annuaire, la CNSS met en
--            face de l'effectif ce qu'elle a payé à ces personnes (la pension
--            moyenne = montant ÷ effectif a alors un sens) ; si c'est « tout
--            le payé », elle publie la dépense de l'année. Le sexe 1 doit être
--            les hommes : vieillesse 2024 publiée 24 627 hommes, 6 103 femmes.
-- Tableaux : T32 à T41.
-- Résultat : Exécutée le 31/10/2026 ; détail dans
--            resultats/lot5_P75_montants_2024_2025.tsv. En 2024, montant des
--            comptés / tout le payé / publié :
--            · T32 37 665 983 112 / 38 625 184 454 / 38 235 880 033
--              (vieillesse seule) ;
--            · T33 112 058 723 / 116 130 355 / 113 603 730 ;
--            · T34 à T36 7 161 046 495 / 7 222 987 423 / 7 047 164 871 ;
--            · T41 7 688 913 000 / 7 954 458 000 / 8 236 741 500 (enfants).
--            · Sexe 1 / 2, T32 2024 : 24 814 / 6 279 (publié 24 627 hommes,
--              6 103 femmes) ; T34 à T36 : 4 924 / 23 820 (publié 5 792 et
--              25 512).
--            · Payés dans l'année mais pas comptés au 31/12, 2024 : T32 1 105,
--              T34 374, T35 129, T41 16 937.
--            · 2025 : T32 comptés 40 967 168 499, tout le payé
--              41 599 581 768 ; T41 7 487 328 000 et 7 686 565 000.
-- Constat  : · Sexe 1 = hommes, 2 = femmes : confirmé.
--            · Pour T32 et T33, le montant publié tombe entre nos deux
--              montants : T32 −1,5 % pour les comptés et +1,0 % pour tout le
--              payé ; T33 −1,4 % et +2,2 %. Pour T34 à T36, nos deux montants
--              sont un peu au-dessus (+1,6 % et +2,5 %). L'annuaire ne tranche
--              pas : les deux définitions sont à moins de 2,5 %.
--            · Proposition BI : garder le montant des comptés. C'est le seul
--              qui se range dans les tranches d'âge, chaque franc étant
--              rattaché à une personne comptée. Le reste (T32 2024 :
--              959 201 342 FCFA versés à 1 105 personnes sorties dans l'année)
--              se montre à part, en contrôle. Question à la CNSS (fiche T32,
--              Q3).
--            · T41 : nos deux montants sont sous le publié (−6,7 % et −3,4 %)
--              alors que notre effectif est au-dessus : encore un signe que
--              trop d'enfants sont comptés (P79).
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2024 AS annee FROM DUAL UNION ALL SELECT 2025 FROM DUAL
),
perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
pe AS (
    SELECT p.PE_ID,
           pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)                AS annee_naissance,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
),
paie AS (
    SELECT deb.PE_ID,
           EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)                  AS annee,
           SUM(deb.DEB_MONTANT)                                   AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2024-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY deb.PE_ID, EXTRACT(YEAR FROM deb.DEB_DATE_EFFET)
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.sexe,
           pe.IND_ID_PREST,
           pa.montant,
           CASE WHEN pe.date_effet <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                 AND (pe.date_radiation IS NULL
                      OR pe.date_radiation > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (   pe.date_suspension IS NULL
                      OR pe.date_suspension > TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      OR (    pe.date_reactivation >= pe.date_suspension
                          AND pe.date_reactivation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                 AND (pe.date_deces IS NULL
                      OR pe.date_deces > TO_DATE(a.annee || '1231', 'YYYYMMDD'))
                 AND (pe.tableau NOT IN ('T35', 'T39') OR a.annee - pe.annee_naissance < 25)
                 AND (pe.tableau <> 'T41' OR a.annee - pe.annee_naissance <= 21)
                THEN 1 ELSE 0 END                                 AS compte
    FROM annees a
    CROSS JOIN pe
    LEFT JOIN paie pa ON pa.PE_ID = pe.PE_ID
                     AND pa.annee = a.annee
)
SELECT annee,
       tableau,
       sexe,
       COUNT(DISTINCT CASE WHEN compte = 1 THEN IND_ID_PREST END) AS personnes_comptees,
       ROUND(SUM(CASE WHEN compte = 1 THEN montant END))          AS montant_des_comptes,
       ROUND(SUM(montant))                                        AS montant_tout_le_paye,
       COUNT(DISTINCT CASE WHEN compte = 0 AND montant > 0
                           THEN IND_ID_PREST END)                 AS payes_non_comptes
FROM base
GROUP BY annee, tableau, sexe
ORDER BY annee, tableau, sexe;


-- -----------------------------------------------------------------------------
-- P76 — L'âge et le sexe des bénéficiaires comptés en 2024
-- Question : l'âge (année moins année de naissance, règle R8) et les grilles
--            de l'Annuaire donnent-ils la répartition publiée ?
-- Objectif : pour 2024, par tableau, tranche de 5 ans et sexe : personnes
--            comptées au 31/12 (comme P74) ; avec les âges inconnus et ceux
--            hors de la grille du tableau (veuves de moins de 15 ans,
--            orphelins de 25 ans et plus…).
-- Lecture  : comparer aux tableaux 19 (vieillesse), 20 (veuves, veufs,
--            orphelins, ascendants), 21 (invalidité), 36 (enfants
--            d'allocataires) et 76 (rentes viagères) de l'annuaire de la CNSS.
--            Une forme semblable valide la règle de l'âge et les grilles
--            (T32 : moins de 55 ans… 100 ans et plus ; T35 : moins de 5 ans…
--            20-24 ans ; T37 : moins de 20 ans… ; T41 : moins de 20 ans… 80 ans
--            et plus). Liste longue : à garder dans resultats/.
-- Tableaux : T32 à T41.
-- Résultat : Exécutée le 31/10/2026 ; détail dans
--            resultats/lot5_P76_ages_2024.tsv. T32 2024, nous / publié
--            (tableau 19) : moins de 55 ans 16 / 2 ; 55–59 3 778 / 3 399 ;
--            60–64 8 854 / 8 638 ; 65–69 7 737 / 7 757 ; 70–74 5 536 / 5 623 ;
--            75–79 2 997 / 3 078 ; 80–84 1 220 / 1 264 ; 85–89 590 / 619 ;
--            90–94 195 / 195 ; 95–99 97 / 99 ; 100 et plus 74 / 56.
--            · Âges impossibles : T34 500 femmes de 120 à 124 ans ; T38 3 de
--              120 à 124 ans ; T32 1 de 120 à 124 ans et 26 de 105 à 119 ans ;
--              T41 17 enfants d'âge négatif (nés après l'exercice). Aucun sexe
--              inconnu.
-- Constat  : · La répartition par âge de T32 colle à l'annuaire (90–94 exact,
--              65–69 à 20 près). Nous avons un peu plus de jeunes pensionnés
--              (55 à 64 ans : +595) et un peu moins de pensionnés âgés (65 à
--              89 ans : −261). C'est la trace d'une base qui a vécu depuis la
--              publication : des pensions liquidées après coup avec un effet
--              en 2024, des décès saisis après coup. A06 (dossier de la
--              comparaison) le vérifiera.
--            · Les 500 veuves de 120 à 124 ans sont nées vers 1900 : une date
--              de naissance par défaut. Proposition BI : 120 ans ou plus
--              devient « Âge inconnu ». P80 montre les dates en cause.
--            · Les grilles de l'Annuaire conviennent : T35 et T39 tiennent
--              dans 0–24 ans, T41 dans 0–21 ans, T33 de 40 à 84 ans.
--            · T32 compte 16 pensionnés de moins de 55 ans (15 de 50 à 54
--              ans). L'annuaire de la CNSS publie la retraite anticipée à part
--              (78 en 2024) : question Q2 de la fiche T32.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T41', 'FAF'  FROM DUAL
),
compte AS (
    SELECT DISTINCT pm.tableau,
           p.IND_ID_PREST,
           NVL(TO_CHAR(i.IND_SEXE), '?')                          AS sexe,
           2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE)         AS age
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2024-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
      AND (pm.tableau <> 'T41'
           OR 2024 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) <= 21)
)
SELECT tableau,
       CASE WHEN age IS NULL THEN 'âge inconnu'
            WHEN age < 0     THEN 'âge négatif'
            ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                 || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
       END                                                        AS tranche_5_ans,
       SUM(CASE WHEN sexe = '1' THEN 1 ELSE 0 END)                AS sexe_1,
       SUM(CASE WHEN sexe = '2' THEN 1 ELSE 0 END)                AS sexe_2,
       SUM(CASE WHEN sexe NOT IN ('1', '2') THEN 1 ELSE 0 END)    AS sexe_inconnu,
       COUNT(*)                                                   AS personnes
FROM compte
GROUP BY tableau,
         CASE WHEN age IS NULL THEN 'âge inconnu'
              WHEN age < 0     THEN 'âge négatif'
              ELSE LPAD(TO_CHAR(FLOOR(age / 5) * 5), 3, '0') || '-'
                   || LPAD(TO_CHAR(FLOOR(age / 5) * 5 + 4), 3, '0')
         END
ORDER BY tableau, tranche_5_ans;


-- -----------------------------------------------------------------------------
-- P77 — Les personnes qui ont plusieurs pensions
-- Question : une personne comptée une fois par tableau, est-ce que ça change
--            quelque chose ?
-- Objectif : au 31/12/2025, par tableau : prestations comptées, personnes
--            distinctes, personnes avec deux prestations ou plus dans le même
--            tableau ; et personnes comptées dans deux tableaux de pension ou
--            plus (par exemple une veuve elle-même retraitée).
-- Lecture  : si les doublons dans un tableau sont rares, compter par personne
--            ou par pension revient au même ; sinon, la règle « une fois par
--            tableau » (R6) change les chiffres. Les personnes présentes dans
--            deux tableaux y comptent deux fois, une fois dans chacun : c'est
--            voulu (une pension de retraite et une pension de veuve).
-- Tableaux : T32 à T41.
-- Résultat : Exécutée le 31/10/2026 ; détail dans
--            resultats/lot5_P77_plusieurs_pensions_2025.tsv. Au 31/12/2025,
--            personnes qui ont 2 prestations ou plus dans le même tableau :
--            T35 15, T32 10, T37 7, T34 6, ailleurs 0. Personnes présentes
--            aussi dans un autre tableau : T32 519, T37 472, T34 54, T38 9,
--            T33, T35, T39 et T40 1 chacun.
-- Constat  : · Les doublons dans un même tableau sont rares (15 au plus) :
--              compter les personnes (R5) ou les pensions donne presque le
--              même chiffre. On garde la personne.
--            · Être dans plusieurs tableaux est normal : un retraité qui
--              touche aussi une rente d'accident du travail (T32 et T37), une
--              veuve qui a sa propre retraite (T32 et T34). Chaque tableau
--              compte ses bénéficiaires : on n'additionne jamais les tableaux
--              pour compter des personnes.
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
    UNION ALL SELECT 'T37', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T38', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T38', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T39', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T40', 'ARSA' FROM DUAL
),
compte AS (
    SELECT pm.tableau,
           p.IND_ID_PREST
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2025-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2025-12-31')
      AND (pm.tableau NOT IN ('T35', 'T39')
           OR 2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
),
par_personne AS (
    SELECT tableau, IND_ID_PREST, COUNT(*) AS nb
    FROM compte
    GROUP BY tableau, IND_ID_PREST
),
tableaux_par_personne AS (
    SELECT IND_ID_PREST, COUNT(DISTINCT tableau) AS nb_tableaux
    FROM compte
    GROUP BY IND_ID_PREST
)
SELECT pp.tableau,
       SUM(pp.nb)                                                 AS prestations_comptees,
       COUNT(*)                                                   AS personnes,
       SUM(CASE WHEN pp.nb >= 2 THEN 1 ELSE 0 END)                AS personnes_2_et_plus,
       SUM(CASE WHEN tp.nb_tableaux >= 2 THEN 1 ELSE 0 END)       AS dans_2_tableaux_ou_plus
FROM par_personne pp
JOIN tableaux_par_personne tp ON tp.IND_ID_PREST = pp.IND_ID_PREST
GROUP BY pp.tableau
ORDER BY pp.tableau;


-- -----------------------------------------------------------------------------
-- P78 — La coordination : seule la part CNSS compte
-- Question : l'atelier de Kaya 2017 demande, pour les pensions de
--            coordination, de ne compter que la part CNSS (T32, T42). Que
--            paie la CNSS pour ces pensions ?
-- Objectif : au 31/12/2025, pour T32 à T36, par catégorie de dossier : pensions
--            comptées (comme P74), celles payées par la CNSS en 2025, montant
--            payé en 2025.
-- Lecture  : si les pensions COOR_AC (payées par une autre caisse) n'ont pas de
--            paiement CNSS, elles comptent dans l'effectif sans montant, ou il
--            faut les sortir de l'effectif (question au métier). Le montant de
--            la base est déjà la part CNSS : rien à retirer.
-- Tableaux : T32 à T36, T42.
-- Résultat : Exécutée le 31/10/2026 ; détail dans
--            resultats/lot5_P78_coordination_2025.tsv. T32 2025, pensions
--            comptées / payées par la CNSS / montant : ORDINAIRE 21 060 /
--            20 656 / 33 057 988 669 ; sans catégorie 6 844 / 6 448 /
--            5 595 141 728 ; COOR_AC 2 901 / 0 / 0 ; COOR_CNSS 2 012 / 1 944 /
--            2 312 485 002 ; CNSS_AC 34 / 2 / 1 553 100. COOR_AC ailleurs :
--            T34 334 (1 payée), T35 315 (2 payées).
-- Constat  : · Les pensions COOR_AC (coordination, payées par une autre
--              caisse) n'ont aucun paiement de la CNSS : 2 901 en T32, près
--              d'une sur onze.
--            · La CNSS les compte dans ses effectifs : sans elles, T32 2024
--              tomberait vers 28 200 (si leur nombre de 2024 est proche de
--              celui de 2025), soit −8 % du publié ; avec elles, on est à
--              +0,9 %. Proposition BI : les compter dans l'effectif, sans
--              montant. Le montant de la base est déjà la part CNSS (Kaya
--              2017).
--            · Conséquence : la pension moyenne (montant ÷ effectif) baisse de
--              près de 9 % en T32. Question à la CNSS : la calculer sur toutes
--              les pensions ou sur les seules pensions payées par la CNSS ?
-- -----------------------------------------------------------------------------
WITH perimetre AS (
    SELECT 'T32' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T32', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T33', 'RPI'  FROM DUAL
    UNION ALL SELECT 'T34', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T34', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T35', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T36', 'RPA'  FROM DUAL
),
compte AS (
    SELECT p.PE_ID,
           pm.tableau,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre pm ON pm.code = p.TPE_CODE
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID = p.IND_ID_PREST
    WHERE p.PE_DATE_EFFET <= DATE '2025-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2025-12-31')
      AND (   p.PE_DATE_SUSPENSION IS NULL
           OR p.PE_DATE_SUSPENSION > DATE '2025-12-31'
           OR (    p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2025-12-31'))
      AND (i.IND_DATE_DECES IS NULL OR i.IND_DATE_DECES > DATE '2025-12-31')
      AND (pm.tableau <> 'T35'
           OR 2025 - EXTRACT(YEAR FROM i.IND_DATE_NAISSANCE) < 25)
),
paie AS (
    SELECT deb.PE_ID, SUM(deb.DEB_MONTANT) AS montant
    FROM USER_DWH.DEBOURS deb
    WHERE deb.DEB_TYPE = 'PM'
      AND deb.DEB_STATUT IN ('D', 'P')
      AND deb.DEB_DATE_EFFET >= DATE '2025-01-01'
      AND deb.DEB_DATE_EFFET <  DATE '2026-01-01'
      AND deb.PE_ID IN (SELECT PE_ID FROM compte)
    GROUP BY deb.PE_ID
)
SELECT c.tableau,
       c.categorie,
       COUNT(*)                                                   AS pensions_comptees,
       SUM(CASE WHEN pa.PE_ID IS NOT NULL THEN 1 ELSE 0 END)      AS payees_par_la_cnss_2025,
       ROUND(SUM(pa.montant))                                     AS montant_2025
FROM compte c
LEFT JOIN paie pa ON pa.PE_ID = c.PE_ID
GROUP BY c.tableau, c.categorie
ORDER BY c.tableau, pensions_comptees DESC;
