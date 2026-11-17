-- =============================================================================
-- Point 8. La personne comptée
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « on compte l'ayant droit (PRESTATION_ESP,
--   IND_ID_PREST) une fois par tableau. Une veuve retraitée compte dans T34 et
--   dans T32 : on n'additionne jamais deux tableaux. » ; « fin 2024, 31 267
--   pensions de vieillesse et de retraite anticipée en cours pour 31 093
--   personnes. L'annuaire CNSS 2024 fait pareil : en comptant les personnes,
--   on retombe mieux sur ses 30 808 retraités. »
-- Requêtes :
--   1. Le stock au 31/12 de chaque tableau : prestations en cours et
--      personnes comptées.
--   2. Les personnes qui ont plusieurs prestations dans un même tableau, ou
--      qui sont dans deux tableaux.
-- Reprise de : lot5_beneficiaires_31_12.sql, P74 et P77 (exécutées le
--   31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 1
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc beneficiaire (la
--   personne comptée est IND_ID_PREST, une fois par tableau ; en T41, le
--   titulaire du dossier). Fiche des règles : règle 10.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année et par tableau : prestations en cours au 31/12,
-- prestations comptées, personnes comptées (IND_ID_PREST distincts).
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

-- Résultat obtenu (P74, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P74_stock_2020_2025.tsv), T32 au 31/12/2024 :
--   31 267 pensions en cours (vieillesse et retraite anticipée) pour 31 093
--   personnes.
-- Face au tableau 18 de l'annuaire CNSS 2024 (p. 16) : 30 808 retraités
-- (30 730 de vieillesse et 78 en retraite anticipée).
-- Lecture : 174 personnes ont deux pensions en cours (calculé ici). En
--   comptant les personnes, la base est à +0,9 % du publié, contre +1,5 % en
--   comptant les pensions.


-- -----------------------------------------------------------------------------
-- Requête 2. Au 31/12/2025 : personnes qui ont deux prestations ou plus dans
-- le même tableau, et personnes présentes aussi dans un autre tableau.
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

-- Résultat obtenu (P77, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P77_plusieurs_pensions_2025.tsv) :
--   · Deux prestations ou plus dans le même tableau : T35 15, T32 10, T37 7,
--     T34 6, ailleurs 0.
--   · Présentes aussi dans un autre tableau : T32 519, T37 472, T34 54, T38 9 ;
--     T33, T35, T39 et T40 1 chacun.
-- Lecture : une veuve qui a sa propre retraite est dans T32 et dans T34 ;
--   chaque tableau la compte, et on n'additionne jamais deux tableaux pour
--   compter des personnes.
