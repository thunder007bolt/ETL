-- =============================================================================
-- Point 102. L'âge limite des rentes d'orphelin (T39)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « PRESTATION_ESP, code ARSE, en cours au 31/12, et
--   l'âge (IND_DATE_NAISSANCE). » ; « en 2024, 454 rentiers orphelins de moins
--   de 25 ans, 531 sans limite d'âge. L'annuaire CNSS 2024 compte autrement :
--   485, entre les deux. De 2021 à 2023, ses chiffres sont sous les nôtres,
--   comme avec une limite plus basse. »
-- Requête : le stock au 31/12 de chaque tableau, de 2020 à 2025 :
--   prestations en cours et prestations comptées.
-- Reprise de : lot5_beneficiaires_31_12.sql, P74 (exécutée le 31/10/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. Lit DEBOURS :
--   plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc en_cours (T39 :
--   orphelins de moins de 25 ans, en dur ; le réglage age_max_orphelin est
--   proposé dans regles_t32_t41_beneficiaires.md). Fiche des règles :
--   règle 50.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par année et par tableau : prestations en cours au 31/12 (tous âges),
-- prestations et personnes comptées (en T35 et T39, moins de 25 ans).
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
-- resultats/lot5_P74_stock_2020_2025.tsv), T39 au 31/12, face à l'annuaire
-- CNSS 2024 (tableau 80, p. 62, rentes d'orphelins de 2020 à 2024) :
--   Année   Sans limite d'âge   Moins de 25 ans   Publié
--   2020    605                 475               492
--   2021    599                 482               421
--   2022    602                 497               399
--   2023    527                 450               379
--   2024    531                 454               485
-- Lecture : en 2024, l'annuaire (485) tombe entre nos 454 et 531 ; de 2021 à
--   2023, il est sous nos chiffres avec la limite de 25 ans, comme avec une
--   limite plus basse.
