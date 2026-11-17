-- =============================================================================
-- Point 3. Le pensionné au 31 décembre
-- Fiche : points_a_valider_v2.md, rubrique « Ce qu'on applique, à confirmer ».
-- Ce que dit le point : « une prestation (PRESTATION_ESP) commencée avant le
--   31/12 (PE_DATE_EFFET), ni radiée ni suspendue ce jour-là
--   (PE_DATE_RADIATION, PE_DATE_SUSPENSION), titulaire en vie
--   (IND_DATE_DECES). » ; « fin 2024, 31 093 pensionnés de vieillesse dans la
--   base ; l'annuaire CNSS 2024 fait pareil : 30 808 publiés. En exigeant un
--   paiement dans l'année, 27 670 seulement. »
-- Requêtes :
--   1. Le stock au 31/12 de chaque tableau, de 2020 à 2025 : prestations en
--      cours, personnes comptées, personnes payées dans l'année.
--   2. Le montant payé aux personnes comptées, et les personnes payées mais
--      pas comptées au 31/12.
-- Reprise de : lot5_beneficiaires_31_12.sql, P74 et P75 (exécutées le
--   31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages et sommes seulement, aucune donnée nominative. Les
--   deux requêtes lisent DEBOURS : plusieurs minutes chacune.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours (la
--   prestation en cours au 31/12, payée ou non ; sauf T41, bloc paie). Fiche
--   des règles : règle 49.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année (2020 à 2025) et par tableau (T32 à T41) : prestations
-- en cours au 31/12, prestations et personnes comptées, personnes payées dans
-- l'année.
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
-- resultats/lot5_P74_stock_2020_2025.tsv), T32 :
--   Année   Prestations en cours   Personnes comptées   Personnes payées dans l'année
--   2024    31 267                 31 093               28 775
--   Personnes comptées au 31/12, de 2020 à 2025 : 25 273 / 26 587 / 28 077 /
--   29 265 / 31 093 / 32 840.
-- Face au tableau 18 de l'annuaire CNSS 2024 (p. 16) : 30 730 retraités de
-- vieillesse et 78 en retraite anticipée, soit 30 808.


-- -----------------------------------------------------------------------------
-- Requête 2. Par tableau et par sexe : personnes comptées, montant payé aux
-- comptés, tout le payé, personnes payées dans l'année mais pas comptées au
-- 31/12.
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

-- Résultat obtenu (P75, exécutée le 31/10/2026 ; détail dans
-- resultats/lot5_P75_montants_2024_2025.tsv) : en 2024, T32 compte 1 105
-- personnes payées dans l'année mais pas comptées au 31/12 (1 023 hommes et
-- 82 femmes).
-- Lecture (calculé ici) :
--   · 31 093 pensionnés comptés fin 2024 pour 30 808 publiés : +0,9 %.
--   · En exigeant un paiement dans l'année : 28 775 − 1 105 = 27 670 comptés et
--     payés, bien sous les 30 808 publiés. L'annuaire compte donc la pension
--     en cours, payée ou non.
