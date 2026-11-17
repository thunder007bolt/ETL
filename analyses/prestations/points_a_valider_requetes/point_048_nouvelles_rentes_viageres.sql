-- =============================================================================
-- Point 48. Les nouvelles rentes viagères (T48)
-- Fiche : points_a_valider_v2.md, rubrique « Questions par tableau », T42 à
--   T49.
-- Ce que dit le point : « la première rente ARI de la personne
--   (PRESTATION_ESP) saisie dans l'année (PE_DATE_INSERT) ; sa notification
--   est dans PE_DATE_NOTIFICATION. » ; « l'annuaire CNSS 2024 compte
--   autrement : son tableau 75 publie 44 nouvelles rentes viagères pour 2024.
--   La base en a 39 la première fois, 57 notifiées dans l'année. »
-- Requêtes :
--   1. Les entrées de chaque tableau de T42 à T49, de 2020 à 2025.
--   2. Les rentes viagères saisies de 2020 à 2025 : par personne, par
--      dossier, révisions.
-- Reprise de : lot7_nouveaux_beneficiaires.sql, P82 et P91 (exécutées le
--   01/11/2026).
-- Base : USER_DWH, la copie du 02/02/2026, comme les lots. SQL Developer, mode
--   script (F5). Comptages seulement, aucune donnée nominative.
-- Règle appliquée dans : t42_t49_requete_proposee.sql, blocs premiere et
--   entree (la première rente viagère de la personne). Fiche des règles :
--   règle 62.
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
-- resultats/lot7_P82_entrees_2020_2025.tsv), T48 en 2024 : 68 personnes
-- saisies, 39 en première fois, 57 notifiées.
-- Face au tableau 75 de l'annuaire CNSS 2024 (p. 60) : 44 nouvelles rentes
-- viagères.


-- -----------------------------------------------------------------------------
-- Requête 2. Rentes viagères (ARI) saisies chaque année : personnes, première
-- rente de la personne, première du dossier, dossiers, révisions dans le même
-- dossier.
-- -----------------------------------------------------------------------------
WITH ari AS (
    SELECT p.DOS_CODE,
           p.IND_ID_PREST,
           p.PE_DATE_INSERT,
           ROW_NUMBER() OVER (PARTITION BY p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT, p.PE_ID)          AS rang_personne,
           ROW_NUMBER() OVER (PARTITION BY p.DOS_CODE, p.IND_ID_PREST
                              ORDER BY p.PE_DATE_INSERT, p.PE_ID)          AS rang_dossier
    FROM USER_DWH.PRESTATION_ESP p
    WHERE p.TPE_CODE = 'ARI'
)
SELECT EXTRACT(YEAR FROM PE_DATE_INSERT)                          AS annee,
       COUNT(DISTINCT IND_ID_PREST)                               AS personnes,
       COUNT(DISTINCT CASE WHEN rang_personne = 1
                           THEN IND_ID_PREST END)                 AS premiere_de_la_personne,
       COUNT(DISTINCT CASE WHEN rang_dossier = 1
                           THEN IND_ID_PREST END)                 AS premiere_du_dossier,
       COUNT(DISTINCT CASE WHEN rang_dossier = 1
                           THEN DOS_CODE END)                     AS dossiers_premiere,
       COUNT(DISTINCT CASE WHEN rang_dossier > 1
                           THEN IND_ID_PREST END)                 AS revision_meme_dossier
FROM ari
WHERE PE_DATE_INSERT >= DATE '2020-01-01'
  AND PE_DATE_INSERT <  DATE '2026-01-01'
GROUP BY EXTRACT(YEAR FROM PE_DATE_INSERT)
ORDER BY annee;

-- Résultat obtenu (P91, exécutée le 01/11/2026 ; détail dans
-- resultats/lot7_P91_rentes_viageres_par_dossier.tsv) :
--   Année   Personnes   Première de la personne   Première du dossier   Dossiers   Révisions
--   2020    43          25                        25                    25         18
--   2021    63          31                        32                    32         31
--   2022    76          24                        24                    24         52
--   2023    41          29                        29                    29         12
--   2024    68          39                        39                    39         29
--   2025    84          35                        35                    35         49
-- Lecture : 39 nouvelles rentes viagères en 2024 par personne comme par
--   dossier, pour 44 publiées ; à la notification, 57. Aucune des deux dates
--   ne retombe sur l'annuaire.
