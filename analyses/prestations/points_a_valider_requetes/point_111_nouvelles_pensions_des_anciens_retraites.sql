-- =============================================================================
-- Point 111. Les nouvelles pensions des anciens retraités
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « une pension de vieillesse (PRESTATION_ESP, RPV ou
--   RPRA) saisie dans l'année (PE_DATE_INSERT) pour une personne
--   (IND_ID_PREST) qui en avait déjà une. » ; « 220 cas en 2025, 47 en 2024 :
--   près de cinq fois plus en un an. »
-- Requête : les entrées de chaque tableau de T42 à T49, de 2020 à 2025 :
--   personnes saisies et première fois.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P82 (exécutée le 01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, bloc premiere (seule la
--   première prestation de la personne dans le tableau compte). Fiche des
--   règles : règle 62.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Par année et par tableau : personnes saisies, première fois.
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
-- resultats/lot7_P82_entrees_2020_2025.tsv) : personnes saisies qui avaient
-- déjà une prestation du tableau, de 2020 à 2025 :
--   T42   18 / 18 / 28 / 23 / 47 / 220
--   T48   18 / 32 / 52 / 12 / 29 / 49
-- Lecture : 47 anciens retraités ont eu une nouvelle pension de vieillesse en
--   2024, 220 en 2025 : près de cinq fois plus. Ils ne comptent pas comme
--   nouveaux retraités.
