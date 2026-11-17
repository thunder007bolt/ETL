-- =============================================================================
-- Point 39. Qui est l'allocataire (T41)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T32 à
--   T41.
-- Ce que dit le point : « le titulaire (DOSSIER, IND_ID), son âge, son sexe et
--   son décès (INDIVIDU, IND_DATE_NAISSANCE, IND_SEXE, IND_DATE_DECES). » ;
--   « en 2024, 102 637 titulaires pour 102 643 dossiers (enfants non payés
--   compris) ; le nombre de titulaires décédés n'est pas encore mesuré. »
-- Requêtes :
--   1. Le stock au 31/12 de chaque tableau, dont T41 : dossiers, titulaires,
--      bénéficiaires du paiement, enfants.
--   2. Les titulaires des dossiers de T41 fin 2024 qui sont décédés.
-- Reprise de : lot5_beneficiaires_31_12.sql, P74 (exécutée le 31/10/2026) ;
--   la requête 2 est nouvelle.
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 1
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t32_t41_requete_proposee.sql, bloc beneficiaire (en
--   T41, la personne est le titulaire du dossier, DOSSIER.IND_ID). Fiche des
--   règles : règle 56.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année et par tableau : prestations en cours, personnes,
-- dossiers, titulaires et bénéficiaires du paiement comptés, personnes payées.
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
-- resultats/lot5_P74_stock_2020_2025.tsv), T41 au 31/12/2024 :
--   Allocations en cours   Enfants comptés   Dossiers   Titulaires   Bénéficiaires du paiement
--   297 069                283 534           102 643    102 637      101 683
-- Lecture : 102 637 titulaires pour 102 643 dossiers : un titulaire n'a
--   presque jamais deux dossiers.


-- -----------------------------------------------------------------------------
-- Requête 2. Dossiers de T41 au 31/12/2024 (au moins une allocation familiale
-- FAF en cours, enfant de 21 ans au plus, en vie), et ceux dont le titulaire
-- (DOSSIER.IND_ID) est décédé à cette date (INDIVIDU, IND_DATE_DECES).
-- -----------------------------------------------------------------------------
WITH en_cours AS (
    SELECT DISTINCT p.DOS_CODE
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.INDIVIDU e ON e.IND_ID = p.IND_ID_PREST
    WHERE p.TPE_CODE = 'FAF'
      AND p.PE_DATE_EFFET <= DATE '2024-12-31'
      AND (p.PE_DATE_RADIATION IS NULL OR p.PE_DATE_RADIATION > DATE '2024-12-31')
      AND (p.PE_DATE_SUSPENSION IS NULL OR p.PE_DATE_SUSPENSION > DATE '2024-12-31'
           OR (p.PE_DATE_REACTIVATION IS NOT NULL
               AND p.PE_DATE_REACTIVATION >= p.PE_DATE_SUSPENSION
               AND p.PE_DATE_REACTIVATION <= DATE '2024-12-31'))
      AND (e.IND_DATE_DECES IS NULL OR e.IND_DATE_DECES > DATE '2024-12-31')
      AND 2024 - EXTRACT(YEAR FROM e.IND_DATE_NAISSANCE) <= 21
)
SELECT COUNT(*)                                                   AS dossiers,
       COUNT(DISTINCT d.IND_ID)                                   AS titulaires,
       SUM(CASE WHEN t.IND_DATE_DECES <= DATE '2024-12-31'
                THEN 1 ELSE 0 END)                                AS dossiers_titulaire_decede
FROM en_cours c
JOIN USER_DWH.DOSSIER       d ON d.DOS_CODE = c.DOS_CODE
LEFT JOIN USER_DWH.INDIVIDU t ON t.IND_ID   = d.IND_ID;

-- Résultat : pas encore exécutée. Les deux premières colonnes doivent
--   retomber près de P74 (102 643 dossiers, 102 637 titulaires) : la règle
--   des dates est celle du bloc en_cours de t32_t41_requete_proposee.sql.
