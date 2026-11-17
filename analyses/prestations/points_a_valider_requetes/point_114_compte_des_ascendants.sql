-- =============================================================================
-- Point 114. Le compte des ascendants (T47)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « la première rente ARSA de la personne
--   (PRESTATION_ESP) saisie dans l'année (PE_DATE_INSERT), avec sa date
--   d'effet (PE_DATE_EFFET) et sa radiation (PE_DATE_RADIATION). » ;
--   « l'annuaire CNSS 2024 compte autrement : 13 nouveaux ascendants de rente
--   en 2024 dans son tableau 77, 15 dans la base, à la saisie comme à la
--   notification. »
-- Requêtes :
--   1. Les entrées de chaque tableau de T42 à T49 : saisies, première fois,
--      notifiées.
--   2. Les 15 nouveaux ascendants de rente de 2024 : année de la date
--      d'effet, radiés ou non, à la fin de 2024.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P82 (exécutée le
--   01/11/2026) ; la requête 2 est nouvelle (« on les regardera un par un »).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs premiere et
--   entree. Fiche des règles : règle 62.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Par année et par tableau : personnes saisies, première fois,
-- notifiées.
-- -----------------------------------------------------------------------------
WITH annees AS (
    SELECT 2019 + LEVEL AS annee FROM DUAL CONNECT BY LEVEL <= 6
),
perimetre AS (
    SELECT 'T42' AS tableau, 'RPV' AS code FROM DUAL
    UNION ALL SELECT 'T42', 'RPRA' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSV' FROM DUAL
    UNION ALL SELECT 'T43', 'RPSF' FROM DUAL
    UNION ALL SELECT 'T44', 'RPSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T45', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSF' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T45', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T46', 'ARSE' FROM DUAL
    UNION ALL SELECT 'T47', 'ARSA' FROM DUAL
    UNION ALL SELECT 'T48', 'ARI'  FROM DUAL
    UNION ALL SELECT 'T49', 'ARSV' FROM DUAL
    UNION ALL SELECT 'T49', 'ARSF' FROM DUAL
),
pe AS (
    SELECT pm.tableau,
           p.IND_ID_PREST,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_NOTIFICATION)                          AS date_notification,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           TRUNC(p.PE_DATE_SUSPENSION)                            AS date_suspension,
           TRUNC(p.PE_DATE_REACTIVATION)                          AS date_reactivation,
           TRUNC(i.IND_DATE_DECES)                                AS date_deces,
           NVL(d.DOS_CATEGORIE, '(sans catégorie)')               AS categorie
    FROM USER_DWH.PRESTATION_ESP p
    JOIN perimetre             pm ON pm.code   = p.TPE_CODE
    JOIN USER_DWH.DOSSIER      d  ON d.DOS_CODE = p.DOS_CODE
    LEFT JOIN USER_DWH.INDIVIDU i ON i.IND_ID   = p.IND_ID_PREST
),
-- la première prestation de la personne dans le tableau
premiere AS (
    SELECT tableau, IND_ID_PREST, MIN(date_saisie) AS premiere_saisie
    FROM pe
    GROUP BY tableau, IND_ID_PREST
),
base AS (
    SELECT a.annee,
           pe.tableau,
           pe.IND_ID_PREST,
           pe.categorie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS saisie,
           CASE WHEN EXTRACT(YEAR FROM pe.date_notification) = a.annee
                THEN 1 ELSE 0 END                                 AS notifiee,
           CASE WHEN EXTRACT(YEAR FROM pr.premiere_saisie) = a.annee
                THEN 1 ELSE 0 END                                 AS premiere_fois,
           CASE WHEN pe.date_radiation <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR pe.date_deces     <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                  OR (    pe.date_suspension <= TO_DATE(a.annee || '1231', 'YYYYMMDD')
                      AND (   pe.date_reactivation IS NULL
                           OR pe.date_reactivation < pe.date_suspension
                           OR pe.date_reactivation > TO_DATE(a.annee || '1231', 'YYYYMMDD')))
                THEN 1 ELSE 0 END                                 AS sortie
    FROM annees a
    CROSS JOIN pe
    JOIN premiere pr ON pr.tableau      = pe.tableau
                    AND pr.IND_ID_PREST = pe.IND_ID_PREST
)
SELECT annee,
       tableau,
       SUM(saisie)                                                AS prestations_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 THEN IND_ID_PREST END) AS personnes_saisies,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND premiere_fois = 1
                           THEN IND_ID_PREST END)                 AS premiere_fois,
       COUNT(DISTINCT CASE WHEN notifiee = 1
                           THEN IND_ID_PREST END)                 AS personnes_notifiees,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND sortie = 1
                           THEN IND_ID_PREST END)                 AS sorties_avant_31_12,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'COOR_AC'
                           THEN IND_ID_PREST END)                 AS coor_ac,
       COUNT(DISTINCT CASE WHEN saisie = 1 AND categorie = 'CNSS_AC'
                           THEN IND_ID_PREST END)                 AS cnss_ac
FROM base
GROUP BY annee, tableau
ORDER BY tableau, annee;

-- Résultat obtenu (P82, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P82_entrees_2020_2025.tsv), T47 en 2024 : 15 saisies, 15 en
-- première fois, 15 notifiées. Face au tableau 77 de l'annuaire CNSS 2024
-- (p. 61) : 13.


-- -----------------------------------------------------------------------------
-- Requête 2. Les ascendants dont la première rente d'ascendant (ARSA) est
-- saisie en 2024 : par année de la date d'effet, radiés ou non au
-- 31/12/2024, notifiés en 2024 ou non. Comptages seulement.
-- -----------------------------------------------------------------------------
WITH arsa AS (
    SELECT p.IND_ID_PREST,
           TRUNC(p.PE_DATE_INSERT)                                AS date_saisie,
           TRUNC(p.PE_DATE_EFFET)                                 AS date_effet,
           TRUNC(p.PE_DATE_NOTIFICATION)                          AS date_notification,
           TRUNC(p.PE_DATE_RADIATION)                             AS date_radiation,
           ROW_NUMBER() OVER (PARTITION BY p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT, p.PE_ID) AS rang
    FROM USER_DWH.PRESTATION_ESP p
    JOIN USER_DWH.DOSSIER d ON d.DOS_CODE = p.DOS_CODE
    WHERE p.TPE_CODE = 'ARSA'
)
SELECT EXTRACT(YEAR FROM date_effet)                              AS annee_effet,
       COUNT(*)                                                   AS nouveaux_2024,
       SUM(CASE WHEN date_radiation <= DATE '2024-12-31'
                THEN 1 ELSE 0 END)                                AS radies_fin_2024,
       SUM(CASE WHEN EXTRACT(YEAR FROM date_notification) = 2024
                THEN 1 ELSE 0 END)                                AS notifies_2024
FROM arsa
WHERE rang = 1
  AND date_saisie >= DATE '2024-01-01'
  AND date_saisie <  DATE '2025-01-01'
GROUP BY EXTRACT(YEAR FROM date_effet)
ORDER BY 1;

-- Résultat : pas encore exécutée. Le total de nouveaux_2024 doit redonner
--   les 15 de P82.
-- Lecture attendue : une date d'effet ancienne, ou une radiation dans
--   l'année, peut expliquer que l'annuaire en compte 2 de moins.
