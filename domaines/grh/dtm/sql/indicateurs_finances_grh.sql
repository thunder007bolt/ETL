-- DTM_INDICATEURS_FINANCES_GRH — Taux d'encadrement par exercice (autres ratios en attente)
-- Tableaux : CIPRES T65 (second taux : cadres supérieurs) ; taux prévus pour T63 et T64, vides
-- Sources  : DTM.DTM_GRH_EFFECTIF (même cliché), DTM.DIM_ANNEE
-- Grain    : AN_ID (une ligne par année)
-- :1 = CLICHE (MMYYYY) — filtre DTM_GRH_EFFECTIF sur le cliché chargé et étiquette les lignes insérées
--
-- Règles :
--   - lit DTM_GRH_EFFECTIF du même cliché, qui doit être chargé avant cette table
--     (ordre de dtm_config.py) ;
--   - taux d'encadrement : cadres supérieurs (HC, 05, 06) / ensemble des agents présents ;
--   - taux financiers vides en attendant une source comptable.
--
-- Étapes :
--   1. ANNEES         années publiées dans DTM_GRH_EFFECTIF et connues de DIM_ANNEE
--   2. EFFECTIF       cadres supérieurs et effectif total par année
--   3. SELECT final   taux d'encadrement et taux financiers vides
WITH
-- ── 1. Années à publier ──────────────────────────────────────────────────────
-- Années présentes dans DTM_GRH_EFFECTIF pour ce cliché et déclarées dans
-- DIM_ANNEE (clé AN_ID de la table cible).
ANNEES AS (
    SELECT DISTINCT TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) AS annee
    FROM   DTM.DTM_GRH_EFFECTIF
    WHERE  CLICHE = :1
      AND  TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4)) IN (
               SELECT AN_ID FROM DTM.DIM_ANNEE
           )
),
-- ── 2. Cadres supérieurs et effectif total par année ─────────────────────────
-- Somme des lignes de DTM_GRH_EFFECTIF : chaque agent n'y figure qu'une fois
-- par exercice, la somme ne compte donc personne deux fois.
EFFECTIF AS (
    SELECT
        TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))                             AS annee,
        -- Cadres supérieurs : qualifications HC, 05, 06
        SUM(CASE WHEN QUAL_CODE IN ('HC','05','06') THEN NB_AGENTS ELSE 0 END) AS nb_cadres,
        -- Tous les agents présents, actifs ou non
        SUM(NB_AGENTS)                                                         AS nb_total
    FROM   DTM.DTM_GRH_EFFECTIF
    WHERE  CLICHE = :1
    GROUP  BY TO_NUMBER(SUBSTR(TO_CHAR(ID_TEMPS), 1, 4))
)
-- ── 3. Taux d'encadrement et taux financiers ─────────────────────────────────
-- Taux = cadres supérieurs / effectif total, arrondi à 4 décimales. Les taux
-- financiers (T63, T64) restent vides, et OBSERVATION le rappelle.
SELECT
    a.annee                                                       AS AN_ID,
    ROUND(NVL(e.nb_cadres, 0) / NULLIF(e.nb_total, 0), 4)        AS TX_ENCADREMENT,
    NULL                                                          AS TX_FRAIS_PERSONNEL,
    NULL                                                          AS TX_CHARGES_ADMIN,
    NULL                                                          AS TX_FORMATION,
    NULL                                                          AS TX_EXECUTION_BUDGETAIRE,
    NVL(e.nb_cadres, 0)                                           AS NB_CADRES,
    NVL(e.nb_total,  0)                                           AS NB_EFFECTIF_TOTAL,
    'TX_FRAIS_PERSONNEL, TX_CHARGES_ADMIN, TX_FORMATION, TX_EXECUTION_BUDGETAIRE : '
    || 'en attente sources (masse sal. personnel CNSS + DWH.FAIT_BUDGET)'             AS OBSERVATION,
    :1                                                            AS CLICHE,
    SYSDATE                                                       AS DATE_CHARGEMENT
FROM       ANNEES  a
LEFT JOIN  EFFECTIF e ON e.annee = a.annee
