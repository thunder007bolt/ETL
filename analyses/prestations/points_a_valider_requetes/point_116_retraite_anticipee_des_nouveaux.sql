-- =============================================================================
-- Point 116. La retraite anticipée des nouveaux (T42)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « une prestation RPRA (TPE_CODE), la première de la
--   personne, saisie dans l'année (PE_DATE_INSERT). » ; « environ 4 nouvelles
--   retraites anticipées par an dans la base. L'annuaire CNSS 2024 en publie 3
--   à part en 2024 ; avec elles, il compte 2 489 nouveaux retraités, la base
--   2 463. »
-- Requêtes :
--   1. Les nouvelles pensions de 2024 par type, selon la date retenue.
--   2. Les entrées de T42 à T49, de 2020 à 2025.
-- Reprise de : comparaison_annuaire_cnss_2024/requetes_comparaison.sql, A03
--   (exécutée le 30/10/2026) ; lot7_nouveaux_beneficiaires.sql, P82
--   (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative. La requête 1
--   lit DEBOURS : plusieurs minutes.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc perimetre (T42 =
--   RPV et RPRA ; le réglage retraite_anticipee est proposé dans
--   regles_t42_t49_nouveaux.md, pas encore dans la requête). Fiche des
--   règles : règle 61.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Requête 1. Les pensions de 2024 par type et selon la date retenue :
-- notification, date d'effet, saisie, premier paiement.
-- -----------------------------------------------------------------------------
WITH pe AS (
    SELECT p.PE_ID,
           p.TPE_CODE,
           p.PE_DATE_NOTIFICATION,
           p.PE_DATE_EFFET,
           p.PE_DATE_INSERT
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE IN ('RPV', 'RPSV', 'RPSF', 'RPSE', 'RPI', 'RPRA', 'RPA')
),
premier AS (
    SELECT d.PE_ID,
           MIN(d.DEB_DATE_EFFET)                                  AS premier_paiement
    FROM USER_DWH.DEBOURS d
    WHERE d.DEB_TYPE = 'PM'
      AND d.DEB_STATUT IN ('D', 'P')
      AND d.PE_ID IN (SELECT PE_ID FROM pe)
    GROUP BY d.PE_ID
)
SELECT CASE pe.TPE_CODE
           WHEN 'RPV'  THEN '1 retraite vieillesse'
           WHEN 'RPSV' THEN '2 veuves'
           WHEN 'RPSF' THEN '3 veufs'
           WHEN 'RPSE' THEN '4 orphelins'
           WHEN 'RPI'  THEN '5 invalidité'
           WHEN 'RPRA' THEN '6 anticipée'
           ELSE             '7 ascendants'
       END                                                        AS type_pension,
       SUM(CASE WHEN pe.PE_DATE_NOTIFICATION >= DATE '2024-01-01'
                 AND pe.PE_DATE_NOTIFICATION <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS notifiees_2024,
       SUM(CASE WHEN pe.PE_DATE_EFFET >= DATE '2024-01-01'
                 AND pe.PE_DATE_EFFET <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS effet_2024,
       SUM(CASE WHEN pe.PE_DATE_INSERT >= DATE '2024-01-01'
                 AND pe.PE_DATE_INSERT <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS saisies_2024,
       SUM(CASE WHEN pr.premier_paiement >= DATE '2024-01-01'
                 AND pr.premier_paiement <  DATE '2025-01-01'
                THEN 1 ELSE 0 END)                                AS premier_paiement_2024
FROM pe
LEFT JOIN premier pr ON pr.PE_ID = pe.PE_ID
GROUP BY CASE pe.TPE_CODE
             WHEN 'RPV'  THEN '1 retraite vieillesse'
             WHEN 'RPSV' THEN '2 veuves'
             WHEN 'RPSF' THEN '3 veufs'
             WHEN 'RPSE' THEN '4 orphelins'
             WHEN 'RPI'  THEN '5 invalidité'
             WHEN 'RPRA' THEN '6 anticipée'
             ELSE             '7 ascendants'
         END
ORDER BY type_pension;

-- Résultat obtenu (A03, exécutée le 30/10/2026) : pensions anticipées (RPRA)
-- de 2024 : 3 notifiées, 10 par la date d'effet, 4 saisies, 8 au premier
-- paiement. Face au tableau 23 de l'annuaire CNSS 2024 (p. 19) : 3 pensions
-- anticipées, publiées à part.


-- -----------------------------------------------------------------------------
-- Requête 2. Par année et par tableau : personnes saisies, première fois,
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
-- resultats/lot7_P82_entrees_2020_2025.tsv), T42 (RPV et RPRA) en 2024 :
-- 2 463 en première fois, pour 2 489 publiés (2 486 de vieillesse et 3
-- anticipées, tableau 23).
-- Lecture : environ 4 retraites anticipées saisies par an ; la base les
--   compte dans T42, l'annuaire aussi pour arriver à 2 489.
